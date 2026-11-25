# Simulador del nodo ESP32 por MQTT. SOLO para probar el broker, el worker y el panel antes de
# conectar el hardware (la guía no acepta datos simulados como fuente principal en la entrega).
#
# Instalar:  py -m pip install paho-mqtt
# Ejecutar:  py simulador_esp32_mqtt.py      (Ctrl + C para detener)

import json
import random
import ssl
import time

import paho.mqtt.client as mqtt

HOST = "y8711e21.ala.eu-central-1.emqxsl.com"
PUERTO = 8883
USUARIO = "Esp32_banda"
CLAVE = "TU_CLAVE_ESP32"
BASE = "uptc/telematica/banda01"

V_IZQ, V_MED, V_DER = 370, 1340, 2260
estado = {"estado": "PARADO", "mv": V_IZQ, "vIzq": V_IZQ, "vMed": V_MED, "vDer": V_DER, "pausa": 3, "rssi": -60,
          "vueltas": 0, "alertaId": 0, "alerta": "-",
          "productos": [{"nombre": f"Material {i + 1}", "codigo": "PEND", "uid": ""} for i in range(3)]}
direccion, objetivo, fin_espera = 1, 1, 0


def ahora():
    return int(time.time() * 1000)


def publicar(c, sub, datos, retener=False):
    c.publish(f"{BASE}/{sub}", json.dumps(datos), qos=1, retain=retener)


def al_recibir(c, u, msg):
    o = json.loads(msg.payload)
    print("Orden recibida:", o["comando"], o["valor"])
    if o["comando"] == "on" and estado["estado"] == "PARADO":
        estado["estado"] = "DERECHA" if direccion == 1 else "IZQUIERDA"
    elif o["comando"] == "off":
        estado["estado"] = "PARADO"
    elif o["comando"] == "espera":
        estado["pausa"] = o["valor"]
    elif o["comando"] == "reset":
        for p in estado["productos"]:
            p["codigo"] = "PEND"


c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="esp32-simulado")
c.username_pw_set(USUARIO, CLAVE)
c.tls_set(cert_reqs=ssl.CERT_REQUIRED)
c.will_set(f"{BASE}/conexion", "offline", qos=1, retain=True)
c.on_message = al_recibir
c.connect(HOST, PUERTO, keepalive=30)
c.loop_start()
c.subscribe(f"{BASE}/cmd", qos=1)
c.publish(f"{BASE}/conexion", "online", qos=1, retain=True)
print("Simulando la banda. Ctrl + C para detener.")

while True:
    if estado["estado"] in ("DERECHA", "IZQUIERDA"):
        destino = [V_IZQ, V_MED, V_DER][objetivo]
        estado["mv"] += 150 * direccion
        if (direccion == 1 and estado["mv"] >= destino) or (direccion == -1 and estado["mv"] <= destino):
            estado["mv"] = destino
            p = estado["productos"][objetivo]
            p["codigo"] = "OK" if random.random() > 0.3 else "FALTA"
            if p["codigo"] == "FALTA":
                estado["alertaId"] += 1
                publicar(c, "eventos", {"node_id": "banda01", "tipo": "FALTA", "pos": objetivo, "msg": "Falta " + p["nombre"], "ts": ahora()})
            estado["estado"] = "ESPERANDO"
            fin_espera = time.time() + estado["pausa"]
    elif estado["estado"] == "ESPERANDO" and time.time() >= fin_espera:
        if objetivo == 2:
            direccion = -1
        elif objetivo == 0:
            direccion = 1
        objetivo += direccion
        estado["estado"] = "DERECHA" if direccion == 1 else "IZQUIERDA"

    ir = 1 if estado["estado"] == "ESPERANDO" and random.random() > 0.3 else 0
    publicar(c, "posicion", {"node_id": "banda01", "variable": "posicion", "valor": round(estado["mv"] / 1000, 3), "unidad": "V", "ts": ahora()})
    publicar(c, "presencia_ir", {"node_id": "banda01", "variable": "presencia_ir", "valor": ir, "unidad": "bool", "ts": ahora()})
    publicar(c, "estado", dict(estado, node_id="banda01", ts=ahora()), retener=True)
    print(estado["estado"], estado["mv"], "mV")
    time.sleep(2)
