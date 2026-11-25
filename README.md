# Banda de monitoreo · Sistema IoT de adquisición real en la nube

Laboratorio integrador de Telemática (UPTC, 2026-2). Un carro lineal recorre una estantería, mide su posición con un potenciómetro (ADC) y detecta si hay producto en cada puesto con un sensor infrarrojo. Los datos viajan por MQTT a la nube, se guardan como series temporales y se muestran en un panel web protegido con JWT, desde el que también se controla la banda.

## Arquitectura

```
ESP32 + sensores ──MQTT/TLS 8883 (QoS 1)──> Broker EMQX Cloud ──> Worker paho-mqtt ──> InfluxDB Cloud
   ▲  (núcleo 1: recorrido y sensores                     │                (bucket "banda")
   │   núcleo 0: comunicación)                             │
   └────────── BASE/cmd (QoS 1) <── API REST Flask + JWT <─┴── Panel web (fetch cada 3 s)
                                     (Render)
```

| Etapa | Componente | Protocolo | Capas OSI |
|---|---|---|---|
| 1. Nodo edge | ESP32, potenciómetro (ADC 12 bits), FC-51 (digital), VL53L0X opcional (I2C) | ADC, GPIO, I2C · Wi-Fi 802.11 | 1 y 2 |
| 2. Mensajería | Broker EMQX Cloud Serverless | MQTT 5/3.1.1 sobre TLS sobre TCP 8883 | 4 a 7 |
| 3. Ingesta | Worker suscriptor (paho-mqtt) dentro de `app.py` → InfluxDB Cloud | HTTPS · Line Protocol | 6 y 7 |
| 4. Backend y web | Flask + JWT, panel HTML/JS | HTTPS · REST · JSON · JWT | 5 a 7 |

## Tópicos MQTT (`BASE = uptc/telematica/banda01`)

| Tópico | Sentido | QoS | Retenido | Contenido |
|---|---|---|---|---|
| `BASE/posicion` | ESP32 → nube | 1 | no | `{"node_id","variable":"posicion","valor":1.341,"unidad":"V","crudo":1663,"ts":…}` cada 2 s |
| `BASE/presencia_ir` | ESP32 → nube | 1 | no | `{"variable":"presencia_ir","valor":0 o 1,"unidad":"bool",…}` cada 2 s |
| `BASE/distancia` | ESP32 → nube | 1 | no | solo con VL53L0X, en mm |
| `BASE/estado` | ESP32 → nube | 1 | sí | estado completo de la banda, al cambiar y cada 10 s |
| `BASE/eventos` | ESP32 → nube | 1 | no | `{"tipo":"FALTA","pos":1,"msg":"Falta Material 2",…}` |
| `BASE/conexion` | ESP32 / broker | 1 | sí | `online` al conectar; `offline` lo publica el broker si el ESP32 se cae (Last Will) |
| `BASE/cmd` | nube → ESP32 | 1 | no | `{"id","comando":"on"/"off"/"reset"/"espera","valor","ts"}` |

## Esquema en InfluxDB

- **measurement `medicion`** · tags: `node_id`, `ubicacion`, `sensor`, `unidad` · field: `valor` (float) · tiempo: marca del ESP32 (NTP).
- **measurement `evento`** · tags: `node_id`, `tipo` · fields: `mensaje` (string), `posicion` (int).

Los tags van indexados (se filtra por ellos); los fields son los valores medidos, que cambian en cada punto.

## API REST

| Ruta | Protección | Uso |
|---|---|---|
| `POST /api/login` | pública | recibe usuario y contraseña, devuelve un JWT (HS256, 8 h) |
| `GET /api/sensors/latest` | JWT | últimos valores, estado de la banda y conexión |
| `GET /api/sensors/history?variable=posicion&seg=300` | JWT | histórico desde InfluxDB (Flux) |
| `GET /api/eventos` | JWT | últimos eventos |
| `POST /api/comando` | JWT | publica una orden en `BASE/cmd` |
| `GET /api/status` | pública | estado del worker MQTT e InfluxDB (sin datos sensibles) |

El token se guarda en `sessionStorage` y se envía en el encabezado `Authorization: Bearer <token>`.

## Estructura del repositorio

```
app.py                     backend: worker MQTT + API REST con JWT + sirve el panel
frontend/index.html        panel web (gráficas SVG, polling cada 3 s)
frontend/login.html        inicio de sesión
firmware/banda_mqtt/       código del ESP32 (Arduino)
herramientas/              simulador MQTT para pruebas sin hardware
requirements.txt · render.yaml · .env.example
```

## Despliegue

1. **Broker:** EMQX Cloud Serverless, puerto 8883 (TLS). Usuarios: `Esp32_banda` (nodo) y `Servidor_banda` (backend).
2. **Base de datos:** InfluxDB Cloud (plan gratuito), organización `banda-monitoreo`, bucket `banda` (retención 30 días), token de API.
3. **Backend:** Render (Web Service, plan Free) conectado a este repositorio. Variables de entorno según `.env.example`. Comando de arranque: `gunicorn app:app --workers 1 --threads 8 --timeout 60`.
4. **Firmware:** Arduino IDE con las librerías "MQTT" (Joel Gaehwiler) y, si se usa, "Adafruit_VL53L0X". Completar Wi-Fi y la contraseña de `Esp32_banda`.
5. **Mantener despierto el servicio gratuito:** un monitor externo (por ejemplo cron-job.org) que visite `/api/status` cada 10 minutos.

## Pruebas de extremo a extremo

- **Sensores reales:** mover el carro a mano o con INICIAR y observar el voltaje en la gráfica; tapar y destapar el FC-51 y ver el escalón en la gráfica de presencia.
- **Desconexión de Wi-Fi:** apagar el punto de acceso 1 minuto. El broker publica `offline` (Last Will) y el panel lo muestra. Al volver, el ESP32 envía las lecturas guardadas en su cola con su hora original y la gráfica no queda con hueco.
- **Órdenes durante la desconexión:** con sesión persistente y QoS 1, el broker guarda las órdenes; al reconectar el ESP32 las recibe y descarta las de más de 30 s.
- **JSON malformado:** el worker lo descarta, lo cuenta en `/api/status` (`malformados`) y sigue funcionando.
