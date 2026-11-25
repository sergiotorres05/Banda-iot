# Backend del sistema IoT "Banda de monitoreo" (Laboratorio integrador de Telemática)
#
#   ESP32 --MQTT/TLS (QoS 1)--> Broker EMQX --> worker MQTT (este archivo, paho-mqtt) --> InfluxDB Cloud
#   Navegador --HTTPS + JWT--> API REST (este archivo, Flask) --> InfluxDB / estado en memoria
#   Órdenes: Navegador --> /api/comando --> publica en <BASE>/cmd (QoS 1) --> ESP32
#
# Variables de entorno (se configuran en Render, nunca en el código):
#   MQTT_HOST, MQTT_PORT (8883), MQTT_USER, MQTT_PASS, MQTT_BASE (uptc/telematica/banda01)
#   INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG, INFLUX_BUCKET
#   PANEL_USER, PANEL_CLAVE   credenciales para entrar al panel
#   JWT_SECRET                (opcional) clave para firmar los JWT; si no existe se deriva de las demás

# Carga el códec idna antes de que arranquen los hilos (evita 'unknown encoding: idna' en Python 3.13+)
import encodings.idna  # noqa: F401
"servidor".encode("idna")
import hashlib
import hmac
import json
import os
import re
import ssl
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from functools import wraps
from zoneinfo import ZoneInfo

import jwt
import paho.mqtt.client as mqtt
from flask import Flask, g, jsonify, request, send_from_directory
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import WriteOptions

# ---------------- Configuración ----------------
MQTT_HOST = os.environ.get("MQTT_HOST", "")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "8883"))
MQTT_USER = os.environ.get("MQTT_USER", "")
MQTT_PASS = os.environ.get("MQTT_PASS", "")
MQTT_BASE = os.environ.get("MQTT_BASE", "uptc/telematica/banda01").rstrip("/")
MQTT_TLS = os.environ.get("MQTT_TLS", "1") != "0"

INFLUX_URL = os.environ.get("INFLUX_URL", "")
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN", "")
INFLUX_ORG = os.environ.get("INFLUX_ORG", "")
INFLUX_BUCKET = os.environ.get("INFLUX_BUCKET", "banda")

PANEL_USER = os.environ.get("PANEL_USER", "admin")
PANEL_CLAVE = os.environ.get("PANEL_CLAVE", "")
JWT_SECRET = os.environ.get("JWT_SECRET") or hashlib.sha256(
    ("jwt|" + PANEL_CLAVE + "|" + INFLUX_TOKEN + "|" + MQTT_PASS).encode()).hexdigest()
JWT_HORAS = 8

UBICACION = os.environ.get("UBICACION", "estanteria_A")
ZONA = ZoneInfo("America/Bogota")
LIMITE_EN_LINEA = 15          # s sin mensajes del nodo para considerarlo desconectado
VARIABLE_OK = re.compile(r"^[a-z0-9_]{1,32}$")
COMANDOS = {"on", "off", "reset", "espera"}

app = Flask(__name__, static_folder=None)
CARPETA_WEB = os.path.join(os.path.dirname(os.path.abspath(__file__)), "frontend")

# ---------------- Estado en memoria (lo más reciente, para responder rápido) ----------------
CANDADO = threading.Lock()
ULTIMOS = {}                                 # variable -> {valor, unidad, ts}
RECIENTES = {}                               # variable -> deque[(ts_ms, valor)] últimos 30 min
ESTADO = {"datos": None, "t": 0.0}           # estado completo de la banda (tópico estado, retenido)
CONEXION = {"nodo": "desconocido", "t": 0.0}  # online / offline (LWT del ESP32)
EVENTOS = deque(maxlen=200)
STATS = {"mensajes": 0, "malformados": 0, "ultimo_msg": 0.0, "mqtt_conectado": False, "influx_errores": 0}

# ---------------- InfluxDB ----------------
influx = None
write_api = None
query_api = None


def iniciar_influx():
    global influx, write_api, query_api
    if not (INFLUX_URL and INFLUX_TOKEN and INFLUX_ORG):
        print("[AVISO] InfluxDB no configurado (INFLUX_URL, INFLUX_TOKEN, INFLUX_ORG)")
        return
    influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG, timeout=10_000)
    # Escritura por lotes: junta puntos y los envía cada 2 s (menos peticiones a la nube)
    write_api = influx.write_api(write_options=WriteOptions(batch_size=100, flush_interval=2_000))
    query_api = influx.query_api()
    print("[INFLUX] Cliente listo")


def guardar_medicion(nodo, variable, valor, unidad, ts_ms):
    """Esquema: measurement 'medicion' | tags: node_id, ubicacion, sensor, unidad | field: valor."""
    if not write_api:
        return
    try:
        p = (Point("medicion")
             .tag("node_id", nodo).tag("ubicacion", UBICACION).tag("sensor", variable).tag("unidad", unidad)
             .field("valor", float(valor))
             .time(int(ts_ms), WritePrecision.MS))
        write_api.write(bucket=INFLUX_BUCKET, record=p)
    except Exception as e:
        STATS["influx_errores"] += 1
        print(f"[INFLUX] Error al escribir: {e}")


def guardar_evento(nodo, tipo, pos, msg, ts_ms):
    if not write_api:
        return
    try:
        p = (Point("evento").tag("node_id", nodo).tag("tipo", tipo)
             .field("mensaje", msg).field("posicion", int(pos) if pos is not None else -1)
             .time(int(ts_ms), WritePrecision.MS))
        write_api.write(bucket=INFLUX_BUCKET, record=p)
    except Exception as e:
        STATS["influx_errores"] += 1
        print(f"[INFLUX] Error al escribir evento: {e}")


# ---------------- Worker MQTT (suscriptor) ----------------
def ts_valido(ts):
    """Usa la marca de tiempo del ESP32 si es razonable; si no, la hora del servidor."""
    ahora = time.time() * 1000
    if isinstance(ts, (int, float)) and abs(ahora - ts) < 7 * 24 * 3600 * 1000:
        return int(ts)
    return int(ahora)


def al_conectar(cliente, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        STATS["mqtt_conectado"] = True
        # Suscripción a todos los tópicos del nodo con QoS 1 (aquí se hace la suscripción)
        cliente.subscribe(MQTT_BASE + "/#", qos=1)
        print(f"[MQTT] Conectado a {MQTT_HOST}, suscrito a {MQTT_BASE}/#")
    else:
        print(f"[MQTT] Conexión rechazada: {reason_code}")


def al_desconectar(cliente, userdata, flags, reason_code, properties=None):
    STATS["mqtt_conectado"] = False
    print(f"[MQTT] Desconectado ({reason_code}); paho reintenta solo")


def al_recibir(cliente, userdata, msg):
    STATS["mensajes"] += 1
    STATS["ultimo_msg"] = time.time()
    sub = msg.topic[len(MQTT_BASE) + 1:] if msg.topic.startswith(MQTT_BASE + "/") else msg.topic
    if sub == "cmd":
        return  # son nuestras propias órdenes

    texto = msg.payload.decode("utf-8", errors="replace")
    if sub == "conexion":  # mensaje retenido / última voluntad (LWT)
        with CANDADO:
            CONEXION.update(nodo=texto.strip()[:20], t=time.time())
        return

    # Todo lo demás debe ser JSON: si llega malformado se cuenta y se descarta sin tumbar el worker
    try:
        d = json.loads(texto)
        if not isinstance(d, dict):
            raise ValueError("no es un objeto JSON")
    except ValueError as e:
        STATS["malformados"] += 1
        print(f"[MQTT] JSON malformado en {msg.topic}: {e} -> descartado")
        return

    nodo = str(d.get("node_id", "banda01"))[:32]
    ts = ts_valido(d.get("ts"))

    if sub == "estado":
        with CANDADO:
            ESTADO.update(datos=d, t=time.time())
        return

    if sub == "eventos":
        tipo = str(d.get("tipo", ""))[:20]
        texto_ev = str(d.get("msg", ""))[:200]
        pos = d.get("pos") if isinstance(d.get("pos"), int) else None
        if tipo and texto_ev:
            with CANDADO:
                EVENTOS.appendleft({"tipo": tipo, "posicion": pos, "mensaje": texto_ev, "ts": ts})
            guardar_evento(nodo, tipo, pos, texto_ev, ts)
        return

    # Mediciones: <BASE>/<variable>  {"node_id","variable","valor","unidad","ts"}
    variable = str(d.get("variable", sub))
    valor = d.get("valor")
    unidad = str(d.get("unidad", ""))[:12]
    if not VARIABLE_OK.match(variable) or not isinstance(valor, (int, float)):
        STATS["malformados"] += 1
        print(f"[MQTT] Medición inválida en {msg.topic}: {texto[:80]}")
        return
    with CANDADO:
        ULTIMOS[variable] = {"valor": valor, "unidad": unidad, "ts": ts, "node_id": nodo}
        RECIENTES.setdefault(variable, deque(maxlen=1200)).append((ts, valor))
    guardar_medicion(nodo, variable, valor, unidad, ts)


mqtt_cliente = None


def iniciar_mqtt():
    global mqtt_cliente
    if not (MQTT_HOST and MQTT_USER):
        print("[AVISO] MQTT no configurado (MQTT_HOST, MQTT_USER, MQTT_PASS)")
        return
    # Diagnóstico sin revelar la clave: muestra el usuario exacto y cuántos caracteres tiene la clave
    print(f"[MQTT] Host={MQTT_HOST!r} puerto={MQTT_PORT} usuario={MQTT_USER!r} "
          f"clave={len(MQTT_PASS)} caracteres, espacios al borde={MQTT_PASS != MQTT_PASS.strip()}")
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                    client_id=f"servidor-banda-{os.getpid()}-{os.urandom(3).hex()}", clean_session=True)
    c.username_pw_set(MQTT_USER, MQTT_PASS)
    if MQTT_TLS:
        c.tls_set(cert_reqs=ssl.CERT_REQUIRED)   # valida el certificado del broker con las CA del sistema
    c.on_connect = al_conectar
    c.on_disconnect = al_desconectar
    c.on_message = al_recibir
    c.reconnect_delay_set(min_delay=1, max_delay=30)
    c.connect_async(MQTT_HOST, MQTT_PORT, keepalive=30)
    c.loop_start()   # hilo propio: escucha el broker sin bloquear a Flask
    mqtt_cliente = c


# Los hilos (MQTT e InfluxDB) se arrancan dentro del proceso que atiende las peticiones.
# Gunicorn puede importar este archivo en el proceso maestro y luego copiarlo (fork) al worker:
# los hilos no pasan con la copia, así que se inician aquí, una vez por proceso.
_SERVICIOS = {"pid": None}
_CANDADO_INICIO = threading.Lock()


def asegurar_servicios():
    if _SERVICIOS["pid"] == os.getpid():
        return
    with _CANDADO_INICIO:
        if _SERVICIOS["pid"] == os.getpid():
            return
        STATS["mqtt_conectado"] = False
        iniciar_influx()
        iniciar_mqtt()
        _SERVICIOS["pid"] = os.getpid()


@app.before_request
def _antes_de_cada_peticion():
    asegurar_servicios()


# ---------------- Autenticación JWT ----------------
def crear_token(usuario):
    ahora = datetime.now(timezone.utc)
    carga = {"sub": usuario, "rol": "operador", "iat": ahora, "exp": ahora + timedelta(hours=JWT_HORAS)}
    return jwt.encode(carga, JWT_SECRET, algorithm="HS256")


def requiere_jwt(f):
    @wraps(f)
    def envoltura(*a, **k):
        cab = request.headers.get("Authorization", "")
        if not cab.startswith("Bearer "):
            return jsonify(error="falta el token"), 401
        try:
            g.usuario = jwt.decode(cab[7:], JWT_SECRET, algorithms=["HS256"])["sub"]
        except jwt.ExpiredSignatureError:
            return jsonify(error="token vencido"), 401
        except jwt.InvalidTokenError:
            return jsonify(error="token inválido"), 401
        return f(*a, **k)
    return envoltura


@app.route("/api/login", methods=["POST"])
def login():
    d = request.get_json(silent=True) or {}
    usuario = str(d.get("usuario", ""))
    clave = str(d.get("clave", ""))
    if not PANEL_CLAVE:
        return jsonify(error="Falta configurar PANEL_CLAVE en el servidor"), 500
    if hmac.compare_digest(usuario, PANEL_USER) and hmac.compare_digest(clave, PANEL_CLAVE):
        return jsonify(token=crear_token(usuario), expira_horas=JWT_HORAS)
    time.sleep(1)  # frena los intentos repetidos
    return jsonify(error="Usuario o contraseña incorrectos"), 401


# ---------------- API protegida ----------------
def hora_local(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000, ZONA).strftime("%d/%m %H:%M:%S")


@app.route("/api/sensors/latest")
@requiere_jwt
def sensors_latest():
    ahora = time.time()
    with CANDADO:
        ultimos = {k: dict(v) for k, v in ULTIMOS.items()}
        estado = dict(ESTADO["datos"]) if ESTADO["datos"] else None
        t_estado = ESTADO["t"]
        conexion = dict(CONEXION)
    for v in ultimos.values():
        v["hora"] = hora_local(v["ts"])
    t_ult = max([t_estado] + [v["ts"] / 1000 for v in ultimos.values()]) if (ultimos or t_estado) else 0
    edad = ahora - t_ult if t_ult else None
    en_linea = conexion["nodo"] == "online" and edad is not None and edad <= LIMITE_EN_LINEA
    return jsonify(
        sensores=ultimos,
        estado=estado,
        nodo=conexion["nodo"],
        enLinea=en_linea,
        edad=round(edad) if edad is not None else None,
        actualizado=hora_local(t_ult * 1000) if t_ult else None,
        broker=STATS["mqtt_conectado"],
    )


@app.route("/api/sensors/history")
@requiere_jwt
def sensors_history():
    variable = request.args.get("variable", "posicion")
    seg = max(60, min(request.args.get("seg", 300, type=int), 30 * 24 * 3600))
    if not VARIABLE_OK.match(variable):
        return jsonify(error="variable no válida"), 400

    # Consulta histórica en InfluxDB (Flux). Para rangos largos promedia en ventanas para no traer miles de puntos.
    ventana = max(1, seg // 300)
    if query_api:
        flux = f'''
from(bucket: "{INFLUX_BUCKET}")
  |> range(start: -{seg}s)
  |> filter(fn: (r) => r._measurement == "medicion" and r.sensor == "{variable}" and r._field == "valor")
  |> aggregateWindow(every: {ventana}s, fn: mean, createEmpty: false)
  |> keep(columns: ["_time", "_value"])'''
        try:
            tablas = query_api.query(flux, org=INFLUX_ORG)
            puntos = [[int(r.get_time().timestamp() * 1000), round(r.get_value(), 4)]
                      for t in tablas for r in t.records]
            puntos.sort()
            return jsonify(variable=variable, seg=seg, fuente="influxdb", ahora=int(time.time() * 1000), puntos=puntos)
        except Exception as e:
            STATS["influx_errores"] += 1
            print(f"[INFLUX] Error al consultar: {e}")
    # Respaldo: lo que hay en memoria (últimos 30 min)
    desde = (time.time() - seg) * 1000
    with CANDADO:
        puntos = [[t, v] for t, v in RECIENTES.get(variable, []) if t >= desde]
    return jsonify(variable=variable, seg=seg, fuente="memoria", ahora=int(time.time() * 1000), puntos=puntos)


@app.route("/api/eventos")
@requiere_jwt
def api_eventos():
    n = max(1, min(request.args.get("n", 40, type=int), 200))
    with CANDADO:
        lista = list(EVENTOS)[:n]
    return jsonify([dict(e, hora=hora_local(e["ts"])) for e in lista])


@app.route("/api/comando", methods=["POST"])
@requiere_jwt
def api_comando():
    d = request.get_json(silent=True) or {}
    comando = d.get("comando")
    if comando not in COMANDOS:
        return jsonify(error="orden no válida"), 400
    valor = 0
    if comando == "espera":
        try:
            valor = int(d.get("valor"))
        except (TypeError, ValueError):
            return jsonify(error="la pausa debe ser un número"), 400
        if not 1 <= valor <= 600:
            return jsonify(error="la pausa debe estar entre 1 y 600 s"), 400
    if not (mqtt_cliente and STATS["mqtt_conectado"]):
        return jsonify(error="el servidor no está conectado al broker MQTT"), 503
    orden = {"id": int(time.time() * 1000) % 10**9, "comando": comando, "valor": valor,
             "ts": int(time.time() * 1000), "usuario": g.usuario}
    info = mqtt_cliente.publish(MQTT_BASE + "/cmd", json.dumps(orden), qos=1)
    return jsonify(ok=info.rc == mqtt.MQTT_ERR_SUCCESS, id=orden["id"]), 201


@app.route("/api/status")
def status():
    """Público y sin datos sensibles: sirve para revisar el sistema y para mantener despierto el servicio."""
    return jsonify(
        mqtt=STATS["mqtt_conectado"],
        influx=bool(INFLUX_URL and INFLUX_TOKEN and INFLUX_ORG),
        mensajes=STATS["mensajes"],
        malformados=STATS["malformados"],
        influx_errores=STATS["influx_errores"],
        segundos_desde_ultimo_mensaje=round(time.time() - STATS["ultimo_msg"]) if STATS["ultimo_msg"] else None,
    )


# ---------------- Frontend ----------------
@app.route("/")
def inicio():
    return send_from_directory(CARPETA_WEB, "index.html")


@app.route("/login")
def pagina_login():
    return send_from_directory(CARPETA_WEB, "login.html")


if __name__ == "__main__":
    asegurar_servicios()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
