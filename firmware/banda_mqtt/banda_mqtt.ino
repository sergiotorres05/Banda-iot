// =====================================================================================
//  BANDA DE MONITOREO · Nodo IoT (ESP32) · Laboratorio integrador de Telemática UPTC
//  Versión 30 · MQTT sobre TLS (QoS 1) hacia EMQX Cloud
//
//  Sensores:
//   - Analógico (ADC): potenciómetro de posición del carro en GPIO 34 (0,37 V izq · 1,34 V centro · 2,26 V der)
//   - Digital: FC-51 (infrarrojo) en GPIO 32, y opcional VL53L0X (I2C, distancia en mm) con USAR_VL53L0X = 1
//
//  Tópicos (BASE = uptc/telematica/banda01):
//   BASE/posicion      medición JSON cada 2 s  {"node_id","variable","valor","unidad","crudo","ts"}
//   BASE/presencia_ir  medición JSON cada 2 s  (1 = objeto frente al sensor, 0 = libre)
//   BASE/distancia     medición JSON cada 2 s  (solo con VL53L0X)
//   BASE/estado        estado completo de la banda (retenido) al cambiar y cada 10 s
//   BASE/eventos       faltantes, reposiciones, inicios, paros (QoS 1)
//   BASE/conexion      "online" al conectar; "offline" lo publica el broker si el ESP32 se cae (LWT)
//   BASE/cmd           órdenes desde el panel (el ESP32 se suscribe con QoS 1)
//
//  Arquitectura interna: el recorrido y los sensores corren en el núcleo 1; la conexión MQTT en el
//  núcleo 0, así la red nunca retrasa la detección de posición. Si se pierde el Wi-Fi o el broker,
//  las lecturas se guardan en una cola (hasta 90 = 3 min) y se envían con su hora original al volver.
//
//  LIBRERÍAS (Arduino IDE > Administrar bibliotecas):
//   - "MQTT" de Joel Gaehwiler (lwmqtt)   -> permite publicar con QoS 1
//   - "Adafruit_VL53L0X" (solo si USAR_VL53L0X = 1)
// =====================================================================================

#include <WiFi.h>
#include <WiFiClientSecure.h>
#include <MQTT.h>
#include <time.h>
#include <sys/time.h>

#define USAR_VL53L0X 0          // pon 1 si conectas el VL53L0X por I2C (SDA = GPIO 21, SCL = GPIO 22)
#if USAR_VL53L0X
#include <Wire.h>
#include <Adafruit_VL53L0X.h>
Adafruit_VL53L0X vl53;
bool vl53Ok = false;
#endif

// ----- Wi-Fi (CÁMBIALOS) -----
const char* WIFI_NOMBRE = "M";
const char* WIFI_CLAVE  = "12345678";

// ----- Broker MQTT EMQX (CÁMBIALOS) -----
const char* MQTT_HOST  = "y8711e21.ala.eu-central-1.emqxsl.com";
const int   MQTT_PORT  = 8883;                 // MQTT sobre TLS
const char* MQTT_USER  = "Esp32_banda";
const char* MQTT_CLAVE = "TU_CLAVE_ESP32";     // la contraseña que creaste en EMQX para Esp32_banda
const char* NODE_ID    = "banda01";
const char* BASE       = "uptc/telematica/banda01";

// Certificado raíz del broker (opcional). Si lo dejas en nullptr la conexión va cifrada pero sin
// verificar el certificado. Para verificarlo, descarga el CA en EMQX (Deployment Overview) y pégalo aquí.
const char* CA_BROKER = nullptr;

const unsigned long INTERVALO_MUESTRA = 2000;   // ms entre lecturas publicadas (la guía pide 2 a 5 s)
const unsigned long INTERVALO_ESTADO  = 10000;  // ms entre publicaciones del estado si no cambia nada
const unsigned long VIGENCIA_ORDEN    = 30000;  // ms: una orden más vieja que esto se descarta

// ----- Motor y sensor de posición -----
const int IN1 = 26;
const int IN2 = 27;
const int SENSOR = 34;

// ----- Sensor infrarrojo FC-51 (montado en el carro, mirando hacia el producto) -----
// Salida OUT: 0 V (LOW) cuando detecta un objeto, alto cuando no hay nada.
// Aliméntalo con 3.3 V para que su salida no pase de 3.3 V en el pin del ESP32.
const int SENSOR_IR = 32;


// ----- Alertas -----
const int LED_POS[3] = {13, 14, 25};   // LED rojo de cada posición: izquierda, centro, derecha
const int BUZZER = 33;                 // zumbador activo

// ----- Productos -----
// Posición 0 = izquierda, 1 = centro, 2 = derecha
const char* NOMBRE[3] = {"Material 1", "Material 2", "Material 3"};

const unsigned long TIEMPO_LECTURA = 400;   // ms que el sensor revisa en cada parada
const int MUESTRAS = 20;                    // lecturas tomadas en ese tiempo

// ----- Recorrido -----
const int V_DER = 2260;   // mV, final derecha
const int V_MED = 1340;   // mV, centro exacto (pon aquí el voltaje que leas con el carro en el puro centro)
const int V_IZQ = 370;    // mV, final izquierda

const int CORTE_DER = V_MED - 130;   // corte anticipado del centro yendo a la DERECHA
const int CORTE_IZQ = V_MED + 60;    // corte anticipado del centro yendo a la IZQUIERDA

const int TOL_CENTRO = 20;           // mV aceptados alrededor del centro (más pequeño = más preciso)
const unsigned long PULSO = 40;      // ms del primer toque de corrección
const unsigned long PULSO_MIN = 10;  // ms del toque más corto
const unsigned long FRENO = 50;
const unsigned long EXTRA_DER = 300;   // ms que sigue andando después del final DERECHO (súbelo si queda corto)
const unsigned long EXTRA_IZQ = 0;     // ms que sigue andando después del final IZQUIERDO (0 = frena apenas llega)

unsigned long espera = 1000;   // pausa en cada punto (se cambia desde la app)

enum Estado { PARADO, DERECHA, IZQUIERDA, ESPERANDO };
Estado estado = PARADO;
Estado siguiente = DERECHA;
Estado ultimaDir = DERECHA;
unsigned long inicioEspera = 0;
bool medioHecho = false;
int vActual = 0;

// ----- Estado de los productos -----
// Códigos: PEND = sin revisar, OK = hay producto, FALTA = no hay producto
String prodCodigo[3] = {"PEND", "PEND", "PEND"};
String prodTexto[3]  = {"Sin revisar", "Sin revisar", "Sin revisar"};
unsigned long alertaId = 0;    // aumenta con cada alerta nueva
String mensajeAlerta = "-";
String prodUid[3] = {"", "", ""};   // no se usa con el FC-51 (el panel lo espera vacío)
unsigned long vueltas = 0;          // recorridos completados (llegadas a un extremo)


// ================= Comunicación con la nube (compartido entre núcleos) =================
WiFiClientSecure net;
MQTTClient mqtt(1536);              // búfer de 1,5 KB para el estado completo
SemaphoreHandle_t candado;
TaskHandle_t tareaNubeHandle = NULL;

String estadoNube = "{}";           // copia del estado en JSON (la arma el núcleo 1)
const int MAX_EVENTOS = 12;
String eventosPend[MAX_EVENTOS];
int nEventos = 0;

struct Lectura { uint64_t ts; int mv; int crudo; int ir; int dist; };
const int MAX_LECTURAS = 90;        // cola de lecturas: 90 x 2 s = 3 min sin conexión
Lectura lecturas[MAX_LECTURAS];
int lecIni = 0, lecN = 0;
unsigned long ultimaMuestra = 0, ultimaCopia = 0;
unsigned long perdidas = 0;         // lecturas descartadas porque la cola se llenó

volatile bool cmdOn = false, cmdOff = false, cmdReset = false;
volatile int cmdEspera = 0;
volatile bool mqttOk = false;
String ultimaClave = "";
long ultimoIdOrden = -1;            // QoS 1 puede entregar una orden dos veces: se ignora el duplicado

void copiarEstado();

// Hora real en milisegundos (NTP). Devuelve 0 si aún no se ha sincronizado.
uint64_t ahoraMs() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  if (tv.tv_sec < 1700000000) return 0;
  return (uint64_t)tv.tv_sec * 1000ULL + tv.tv_usec / 1000;
}

String u64(uint64_t v) {             // String() no acepta uint64_t en todas las versiones
  char b[24];
  snprintf(b, sizeof(b), "%llu", (unsigned long long)v);
  return String(b);
}

// Guarda un evento; la tarea de la nube lo publica con QoS 1
void agregarEvento(const char* tipo, int pos, String msg) {
  String ev = String("{\"node_id\":\"") + NODE_ID + "\",\"tipo\":\"" + tipo + "\",\"pos\":" +
              (pos >= 0 ? String(pos) : String("null")) + ",\"msg\":\"" + msg + "\",\"ts\":" + u64(ahoraMs()) + "}";
  xSemaphoreTake(candado, portMAX_DELAY);
  if (nEventos == MAX_EVENTOS) {
    for (int i = 1; i < MAX_EVENTOS; i++) eventosPend[i - 1] = eventosPend[i];
    nEventos--;
  }
  eventosPend[nEventos++] = ev;
  xSemaphoreGive(candado);
  if (tareaNubeHandle) xTaskNotifyGive(tareaNubeHandle);
}

// ================= Lectura del sensor =================
int leerMv() {
  long suma = 0;
  for (int i = 0; i < 20; i++) suma += analogReadMilliVolts(SENSOR);
  return suma / 20;
}

// ================= Motor =================
void motorParar() {
  digitalWrite(IN1, LOW);
  digitalWrite(IN2, LOW);
}

void motorFrenar() {
  digitalWrite(IN1, HIGH);   // las dos en HIGH = freno
  digitalWrite(IN2, HIGH);
  delay(FRENO);
  motorParar();
}

void motorDerecha() {
  motorParar();
  delay(200);
  digitalWrite(IN1, HIGH);
  digitalWrite(IN2, LOW);
}

void motorIzquierda() {
  motorParar();
  delay(200);
  digitalWrite(IN1, LOW);
  digitalWrite(IN2, HIGH);
}

void pulso(Estado dir, unsigned long ms) {
  if (dir == DERECHA) {
    digitalWrite(IN1, HIGH);
    digitalWrite(IN2, LOW);
  } else {
    digitalWrite(IN1, LOW);
    digitalWrite(IN2, HIGH);
  }
  delay(ms);
  motorFrenar();
  delay(200);
}

// Lectura más estable para el centro: promedia 5 lecturas de leerMv()
int leerMvPreciso() {
  long suma = 0;
  for (int i = 0; i < 5; i++) {
    suma += leerMv();
    delay(5);
  }
  return suma / 5;
}

// Corrige con toques cortos hasta quedar en V_MED ± TOL_CENTRO.
// Cada vez que se pasa al otro lado, el toque se reduce a la mitad.
void ajustarCentro() {
  delay(250);
  unsigned long ms = PULSO;
  int ultimo = 0;
  for (int i = 0; i < 20; i++) {
    int v = leerMvPreciso();
    int sentido = 0;
    if (v < V_MED - TOL_CENTRO) sentido = 1;
    else if (v > V_MED + TOL_CENTRO) sentido = -1;
    else break;
    if (ultimo != 0 && sentido != ultimo) {
      ms = ms / 2;
      if (ms < PULSO_MIN) ms = PULSO_MIN;
    }
    pulso(sentido == 1 ? DERECHA : IZQUIERDA, ms);
    ultimo = sentido;
  }
  Serial.print("  Centro final: ");
  Serial.print(leerMvPreciso());
  Serial.print(" mV (objetivo ");
  Serial.print(V_MED);
  Serial.println(" mV)");
}

// ================= Sensor de producto (FC-51) =================
// Toma varias lecturas durante TIEMPO_LECTURA y decide por mayoría,
// así un reflejo o una lectura suelta no dan una falsa alarma.
bool hayProducto() {
  int detecciones = 0;
  for (int i = 0; i < MUESTRAS; i++) {
    if (digitalRead(SENSOR_IR) == LOW) detecciones++;   // LOW = objeto detectado
    delay(TIEMPO_LECTURA / MUESTRAS);
  }
  Serial.print("  FC-51: ");
  Serial.print(detecciones);
  Serial.print("/");
  Serial.print(MUESTRAS);
  Serial.println(" lecturas con objeto");
  return detecciones > MUESTRAS / 2;
}

void pitar(int veces) {
  for (int i = 0; i < veces; i++) {
    digitalWrite(BUZZER, HIGH);
    delay(150);
    digitalWrite(BUZZER, LOW);
    delay(100);
  }
}

// Revisa el producto de una posición y actualiza LED, zumbador y estado para la app
void revisarProducto(int pos) {
  String codigo, texto;

  Serial.print("Posición ");
  Serial.print(pos + 1);
  Serial.println(":");

  if (hayProducto()) {
    codigo = "OK";
    texto = "✓ " + String(NOMBRE[pos]);
    Serial.println("  Hay producto");
  } else {
    codigo = "FALTA";
    texto = "✗ Falta";
    Serial.println("  No hay producto");
  }

  bool cambio = (codigo != prodCodigo[pos]);
  bool estabaVacio = (prodCodigo[pos] == "FALTA");
  prodCodigo[pos] = codigo;
  prodTexto[pos] = texto;

  // LED rojo encendido mientras el producto falte
  digitalWrite(LED_POS[pos], codigo == "OK" ? LOW : HIGH);

  // Alerta solo cuando la posición pasa a estar vacía (no en cada vuelta)
  if (codigo == "FALTA" && cambio) {
    alertaId++;
    mensajeAlerta = "Falta " + String(NOMBRE[pos]);
    Serial.print("ALERTA: ");
    Serial.println(mensajeAlerta);
    agregarEvento("FALTA", pos, mensajeAlerta);
    pitar(3);
  }

  // Aviso a la nube cuando alguien repone el producto
  if (codigo == "OK" && estabaVacio) {
    agregarEvento("REPUESTO", pos, String(NOMBRE[pos]) + " repuesto");
  }
}

void borrarAlertas() {
  for (int i = 0; i < 3; i++) {
    prodUid[i] = "";
    prodCodigo[i] = "PEND";
    prodTexto[i] = "Sin revisar";
    digitalWrite(LED_POS[i], LOW);
  }
  mensajeAlerta = "-";
  Serial.println("Alertas borradas");
  agregarEvento("RESET", -1, "Alarmas borradas");
}

// ================= Recorrido =================
void mover(Estado dir) {
  if (dir == DERECHA) {
    motorDerecha();
    Serial.println("Moviendo a la DERECHA");
  } else {
    motorIzquierda();
    Serial.println("Moviendo a la IZQUIERDA");
  }
  estado = dir;
  ultimaDir = dir;
}

// Se detiene en un punto, revisa el producto de esa posición y empieza la pausa
void esperar(Estado sig, const char* lugar, bool esCentro, int pos) {
  motorFrenar();
  if (esCentro) {
    delay(200);
    ajustarCentro();
  }
  if (!esCentro) vueltas++;
  siguiente = sig;
  estado = ESPERANDO;
  Serial.print("Llegó a ");
  Serial.println(lugar);
  revisarProducto(pos);
  copiarEstado();
  inicioEspera = millis();
}

void arrancar() {
  if (estado != PARADO) return;
  int v = leerMv();
  Estado dir = ultimaDir;
  if (dir == DERECHA && v >= V_DER) dir = IZQUIERDA;
  if (dir == IZQUIERDA && v <= V_IZQ) dir = DERECHA;
  medioHecho = (dir == DERECHA) ? (v >= CORTE_DER) : (v <= CORTE_IZQ);
  Serial.println("Sistema ARRANCADO");
  agregarEvento("INICIO", -1, "Ronda iniciada");
  mover(dir);
}

void detener() {
  if (estado == PARADO) return;
  if (estado == ESPERANDO) ultimaDir = siguiente;
  motorFrenar();
  estado = PARADO;
  Serial.println("Sistema PARADO");
  agregarEvento("PARO", -1, "Banda detenida");
}

// ================= Estado en JSON =================
// Estado completo en JSON para el panel
String apiEstado() {
  String e;
  switch (estado) {
    case PARADO:    e = "PARADO"; break;
    case DERECHA:   e = "DERECHA"; break;
    case IZQUIERDA: e = "IZQUIERDA"; break;
    case ESPERANDO: e = "ESPERANDO"; break;
  }
  String j = "{";
  j += "\"estado\":\"" + e + "\",";
  j += "\"mv\":" + String(vActual) + ",";
  j += "\"vIzq\":" + String(V_IZQ) + ",";
  j += "\"vMed\":" + String(V_MED) + ",";
  j += "\"vDer\":" + String(V_DER) + ",";
  j += "\"pausa\":" + String(espera / 1000) + ",";
  j += "\"rssi\":" + String(WiFi.RSSI()) + ",";
  j += "\"vueltas\":" + String(vueltas) + ",";
  j += "\"alertaId\":" + String(alertaId) + ",";
  j += "\"alerta\":\"" + mensajeAlerta + "\",";
  j += "\"productos\":[";
  for (int i = 0; i < 3; i++) {
    if (i > 0) j += ",";
    j += "{\"nombre\":\"" + String(NOMBRE[i]) + "\",";
    j += "\"codigo\":\"" + prodCodigo[i] + "\",";
    j += "\"uid\":\"" + prodUid[i] + "\"}";
  }
  j += "]}";
  return j;
}

// Versión resumida del estado que se publica (sin textos largos, para ahorrar tráfico)
void copiarEstado() {
  String e = apiEstado();
  xSemaphoreTake(candado, portMAX_DELAY);
  estadoNube = e;
  xSemaphoreGive(candado);
  String clave = String((int)estado) + prodCodigo[0] + prodCodigo[1] + prodCodigo[2] + String(espera) + String(alertaId);
  if (clave != ultimaClave) {        // cambió algo importante: publicar el estado ya
    ultimaClave = clave;
    if (tareaNubeHandle) xTaskNotifyGive(tareaNubeHandle);
  }
}

// Toma una muestra de los sensores y la pone en la cola (núcleo 1)
void tomarMuestra() {
  Lectura l;
  l.ts = ahoraMs();
  l.mv = vActual;                                  // promedio de 20 lecturas (ADC calibrado, mV)
  l.crudo = analogRead(SENSOR);                    // valor crudo del ADC de 12 bits (0 a 4095)
  l.ir = digitalRead(SENSOR_IR) == LOW ? 1 : 0;    // FC-51: LOW = objeto detectado
  l.dist = -1;
#if USAR_VL53L0X
  if (vl53Ok) {
    VL53L0X_RangingMeasurementData_t m;
    vl53.rangingTest(&m, false);
    if (m.RangeStatus != 4) l.dist = m.RangeMilliMeter;   // 4 = fuera de rango
  }
#endif
  xSemaphoreTake(candado, portMAX_DELAY);
  if (lecN == MAX_LECTURAS) {                      // cola llena (mucho tiempo sin red): se pierde la más vieja
    lecIni = (lecIni + 1) % MAX_LECTURAS;
    lecN--;
    perdidas++;
  }
  lecturas[(lecIni + lecN) % MAX_LECTURAS] = l;
  lecN++;
  xSemaphoreGive(candado);
}

String medicion(const char* variable, String valor, const char* unidad, uint64_t ts, int crudo) {
  String j = String("{\"node_id\":\"") + NODE_ID + "\",\"variable\":\"" + variable + "\",\"valor\":" + valor +
             ",\"unidad\":\"" + unidad + "\"";
  if (crudo >= 0) j += ",\"crudo\":" + String(crudo);
  j += ",\"ts\":" + u64(ts) + "}";
  return j;
}

// ---- Órdenes recibidas por MQTT (se llama dentro de mqtt.loop(): aquí NO se publica nada) ----
long numeroJson(const String& s, const char* campo, long porDefecto) {
  int i = s.indexOf(String("\"") + campo + "\"");
  if (i < 0) return porDefecto;
  i = s.indexOf(':', i);
  if (i < 0) return porDefecto;
  return s.substring(i + 1).toInt();
}
uint64_t numero64Json(const String& s, const char* campo) {
  int i = s.indexOf(String("\"") + campo + "\"");
  if (i < 0) return 0;
  i = s.indexOf(':', i);
  return i < 0 ? 0 : strtoull(s.c_str() + i + 1, NULL, 10);
}
String textoJson(const String& s, const char* campo) {
  int i = s.indexOf(String("\"") + campo + "\"");
  if (i < 0) return "";
  i = s.indexOf(':', i);
  int a = s.indexOf('"', i + 1);
  int b = s.indexOf('"', a + 1);
  return (a < 0 || b < 0) ? "" : s.substring(a + 1, b);
}

void alRecibir(String& topico, String& carga) {
  if (!topico.endsWith("/cmd")) return;
  long id = numeroJson(carga, "id", -1);
  String cmd = textoJson(carga, "comando");
  int valor = numeroJson(carga, "valor", 0);
  uint64_t ts = numero64Json(carga, "ts");
  uint64_t ahora = ahoraMs();

  if (id >= 0 && id == ultimoIdOrden) {
    Serial.println("Orden duplicada (reentrega QoS 1): ignorada");
    return;
  }
  ultimoIdOrden = id;
  if (ahora && ts && (ahora > ts + VIGENCIA_ORDEN)) {
    Serial.printf("Orden '%s' vencida (%llu s de antigüedad): descartada por seguridad\n",
                  cmd.c_str(), (unsigned long long)((ahora - ts) / 1000));
    return;
  }
  Serial.printf("Orden desde la nube: %s %d\n", cmd.c_str(), valor);
  if (cmd == "on") cmdOn = true;
  else if (cmd == "off") cmdOff = true;
  else if (cmd == "reset") cmdReset = true;
  else if (cmd == "espera" && valor >= 1 && valor <= 600) cmdEspera = valor;
}

String topico(const char* sub) { return String(BASE) + "/" + sub; }

bool conectarMqtt() {
  String id = String("esp32-") + NODE_ID;          // ID fijo: el broker recuerda la sesión (clean session = false)
  Serial.print("MQTT: conectando a ");
  Serial.println(MQTT_HOST);
  if (!mqtt.connect(id.c_str(), MQTT_USER, MQTT_CLAVE)) {
    Serial.printf("MQTT: error %d, código %d\n", mqtt.lastError(), mqtt.returnCode());
    return false;
  }
  mqtt.subscribe(topico("cmd"), 1);                // suscripción a las órdenes con QoS 1
  mqtt.publish(topico("conexion"), "online", true, 1);
  ultimaClave = "";                                // fuerza publicar el estado completo
  Serial.println("MQTT: conectado (TLS, QoS 1)");
  return true;
}

void tareaNube(void* parametro) {
  if (CA_BROKER) net.setCACert(CA_BROKER);
  else net.setInsecure();
  mqtt.begin(MQTT_HOST, MQTT_PORT, net);
  static String topicoWill = topico("conexion");   // debe seguir existiendo mientras viva la conexión
  mqtt.setWill(topicoWill.c_str(), "offline", true, 1);   // última voluntad (LWT)
  mqtt.setKeepAlive(30);
  mqtt.setCleanSession(false);
  mqtt.setTimeout(5000);
  mqtt.onMessage(alRecibir);

  unsigned long ultimoIntento = 0, ultimoEstado = 0;
  bool forzarEstado = true;

  for (;;) {
    bool aviso = ulTaskNotifyTake(pdTRUE, pdMS_TO_TICKS(50)) > 0;
    if (aviso) forzarEstado = true;

    if (WiFi.status() != WL_CONNECTED) { mqttOk = false; continue; }

    if (!mqtt.connected()) {
      mqttOk = false;
      if (millis() - ultimoIntento > 3000) {
        ultimoIntento = millis();
        if (conectarMqtt()) { mqttOk = true; forzarEstado = true; }
      }
      continue;
    }
    mqttOk = true;
    mqtt.loop();

    // 1) Eventos (QoS 1: el broker confirma con PUBACK; si falla se reintenta en la siguiente vuelta)
    while (true) {
      xSemaphoreTake(candado, portMAX_DELAY);
      String ev = nEventos ? eventosPend[0] : "";
      xSemaphoreGive(candado);
      if (ev == "") break;
      if (!mqtt.publish(topico("eventos"), ev, false, 1)) break;
      xSemaphoreTake(candado, portMAX_DELAY);
      for (int i = 1; i < nEventos; i++) eventosPend[i - 1] = eventosPend[i];
      nEventos--;
      xSemaphoreGive(candado);
    }

    // 2) Lecturas de los sensores (hasta 5 por vuelta para vaciar la cola tras una desconexión)
    for (int k = 0; k < 5; k++) {
      xSemaphoreTake(candado, portMAX_DELAY);
      bool hay = lecN > 0;
      Lectura l;
      if (hay) l = lecturas[lecIni];
      xSemaphoreGive(candado);
      if (!hay) break;
      bool ok = mqtt.publish(topico("posicion"), medicion("posicion", String(l.mv / 1000.0, 3), "V", l.ts, l.crudo), false, 1) &&
                mqtt.publish(topico("presencia_ir"), medicion("presencia_ir", String(l.ir), "bool", l.ts, -1), false, 1);
      if (ok && l.dist >= 0) ok = mqtt.publish(topico("distancia"), medicion("distancia", String(l.dist), "mm", l.ts, -1), false, 1);
      if (!ok) break;
      xSemaphoreTake(candado, portMAX_DELAY);
      lecIni = (lecIni + 1) % MAX_LECTURAS;
      lecN--;
      xSemaphoreGive(candado);
    }

    // 3) Estado completo (retenido): al cambiar algo o cada 10 s
    if (forzarEstado || millis() - ultimoEstado > INTERVALO_ESTADO) {
      xSemaphoreTake(candado, portMAX_DELAY);
      String e = estadoNube;
      xSemaphoreGive(candado);
      String cuerpo = String("{\"node_id\":\"") + NODE_ID + "\",\"ts\":" + u64(ahoraMs()) + "," + e.substring(1);
      if (mqtt.publish(topico("estado"), cuerpo, true, 1)) {
        ultimoEstado = millis();
        forzarEstado = false;
      }
    }
  }
}

// Ejecuta en el núcleo 1 las órdenes que llegaron por MQTT
void procesarComandosNube() {
  bool hubo = cmdOff || cmdOn || cmdReset || cmdEspera > 0;
  if (cmdOff)   { cmdOff = false; cmdOn = false; detener(); }
  if (cmdOn)    { cmdOn = false; arrancar(); }
  if (cmdReset) { cmdReset = false; borrarAlertas(); }
  if (cmdEspera > 0) {
    espera = (unsigned long)cmdEspera * 1000;
    Serial.printf("Pausa cambiada desde la nube: %d s\n", cmdEspera);
    cmdEspera = 0;
  }
  if (hubo) copiarEstado();
}

// ================= Wi-Fi =================
void conectarWiFi() {
  WiFi.mode(WIFI_STA);
  WiFi.setSleep(false);
  WiFi.setAutoReconnect(true);
  WiFi.begin(WIFI_NOMBRE, WIFI_CLAVE);
  Serial.print("Conectando a ");
  Serial.print(WIFI_NOMBRE);
  unsigned long t0 = millis();
  while (WiFi.status() != WL_CONNECTED && millis() - t0 < 20000) {
    delay(500);
    Serial.print(".");
  }
  Serial.println();
  if (WiFi.status() == WL_CONNECTED) {
    Serial.print("Wi-Fi conectado. IP: ");
    Serial.println(WiFi.localIP());
  } else {
    Serial.println("Aún sin Wi-Fi (debe ser de 2,4 GHz). Se conectará solo cuando esté disponible.");
  }
}

void setup() {
  Serial.begin(115200);
  delay(500);
  Serial.println("=== VERSION 30: banda de monitoreo · MQTT/TLS QoS 1 ===");
  candado = xSemaphoreCreateMutex();

  pinMode(IN1, OUTPUT);
  pinMode(IN2, OUTPUT);
  motorParar();
  analogReadResolution(12);                 // ADC de 12 bits: 0 a 4095
  analogSetPinAttenuation(SENSOR, ADC_11db); // rango de entrada hasta ~3,1 V

  for (int i = 0; i < 3; i++) {
    pinMode(LED_POS[i], OUTPUT);
    digitalWrite(LED_POS[i], LOW);
  }
  pinMode(BUZZER, OUTPUT);
  digitalWrite(BUZZER, LOW);
  pinMode(SENSOR_IR, INPUT_PULLUP);

#if USAR_VL53L0X
  Wire.begin(21, 22);
  vl53Ok = vl53.begin();
  Serial.println(vl53Ok ? "VL53L0X detectado por I2C (0x29)" : "VL53L0X NO detectado: revisa SDA/SCL");
#endif

  conectarWiFi();
  configTime(0, 0, "pool.ntp.org", "time.nist.gov");   // hora real para las marcas de tiempo (UTC)

  vActual = leerMv();
  copiarEstado();
  agregarEvento("ENCENDIDO", -1, "Banda encendida");
  xTaskCreatePinnedToCore(tareaNube, "nube", 16384, NULL, 1, &tareaNubeHandle, 0);

  pitar(1);
  Serial.println("Listo. 1 = arrancar, 2 = parar");
}

void loop() {
  if (Serial.available() > 0) {
    char c = Serial.read();
    if (c == '1') arrancar();
    if (c == '2') detener();
  }
  procesarComandosNube();

  int v = leerMv();
  vActual = v;

  if (millis() - ultimaMuestra >= INTERVALO_MUESTRA) {
    ultimaMuestra = millis();
    tomarMuestra();
  }
  if (millis() - ultimaCopia >= 500) {
    ultimaCopia = millis();
    copiarEstado();
  }

  switch (estado) {
    case DERECHA:
      if (v >= V_DER) {
        delay(EXTRA_DER);
        medioHecho = false;
        esperar(IZQUIERDA, "FINAL DERECHA", false, 2);
      } else if (!medioHecho && v >= CORTE_DER) {
        medioHecho = true;
        esperar(DERECHA, "el CENTRO", true, 1);
      }
      break;

    case IZQUIERDA:
      if (v <= V_IZQ) {
        delay(EXTRA_IZQ);
        medioHecho = false;
        esperar(DERECHA, "FINAL IZQUIERDA", false, 0);
      } else if (!medioHecho && v <= CORTE_IZQ) {
        medioHecho = true;
        esperar(IZQUIERDA, "el CENTRO", true, 1);
      }
      break;

    case ESPERANDO:
      if (millis() - inicioEspera >= espera) {
        mover(siguiente);
      }
      break;

    case PARADO:
      break;
  }
}
