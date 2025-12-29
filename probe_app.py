import json
import os
import ssl
from datetime import datetime, timezone

from flask import Flask, jsonify, request
from flask_mqtt import Mqtt


def env_bool(name: str, default: str = "false") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes", "on"}


def utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


MQTT_HOST = os.environ.get("MQTT_HOST", "test.mosquitto.org")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USERNAME = os.environ.get("MQTT_USERNAME")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
MQTT_TLS_CA_CERTS = os.environ.get("MQTT_TLS_CA_CERTS")
MQTT_TLS_CIPHERS = os.environ.get("MQTT_TLS_CIPHERS")
MQTT_TLS_ENABLED = env_bool("MQTT_TLS_ENABLED", "false")
TOPIC_CMD_BASE = os.environ.get("TOPIC_CMD_BASE", "uca/iot/piscine/cmd")

app = Flask(__name__)

app.config["MQTT_BROKER_URL"] = MQTT_HOST
app.config["MQTT_BROKER_PORT"] = MQTT_PORT
app.config["MQTT_TLS_ENABLED"] = MQTT_TLS_ENABLED
if os.environ.get("MQTT_CLIENT_ID"):
    app.config["MQTT_CLIENT_ID"] = os.environ["MQTT_CLIENT_ID"]
if MQTT_USERNAME:
    app.config["MQTT_USERNAME"] = MQTT_USERNAME
if MQTT_PASSWORD:
    app.config["MQTT_PASSWORD"] = MQTT_PASSWORD
if os.environ.get("MQTT_KEEPALIVE"):
    app.config["MQTT_KEEPALIVE"] = int(os.environ["MQTT_KEEPALIVE"])
if MQTT_TLS_CA_CERTS:
    app.config["MQTT_TLS_CA_CERTS"] = MQTT_TLS_CA_CERTS
if MQTT_TLS_ENABLED:
    app.config["MQTT_TLS_VERSION"] = ssl.PROTOCOL_TLSv1_2
    if MQTT_TLS_CIPHERS:
        app.config["MQTT_TLS_CIPHERS"] = MQTT_TLS_CIPHERS

mqtt = Mqtt(app)

print(
    "[MQTT] config host={host} port={port} tls={tls} cmd_base={cmd} user_set={user}".format(
        host=MQTT_HOST,
        port=MQTT_PORT,
        tls=MQTT_TLS_ENABLED,
        cmd=TOPIC_CMD_BASE,
        user=bool(MQTT_USERNAME),
    )
)


@mqtt.on_connect()
def on_connect(client, userdata, flags, rc):
    print(f"[MQTT] connected rc={rc}")


@mqtt.on_disconnect()
def on_disconnect(client, userdata, rc):
    print(f"[MQTT] disconnected rc={rc}")


@mqtt.on_log()
def on_log(client, userdata, level, buf):
    print(f"[MQTT-LOG] {level} {buf}")


@mqtt.on_publish()
def on_publish(client, userdata, mid):
    print(f"[MQTT] published mid={mid}")


@app.get("/")
def health():
    return "OK"


@app.route("/publish", methods=["GET", "POST"])
def publish():
    data = request.get_json(silent=True) or {}
    pool = data.get("pool") or request.args.get("pool")
    decision = data.get("decision") or request.args.get("decision") or "refused"
    if not pool:
        return jsonify({"ok": False, "error": "missing pool"}), 400

    topic = f"{TOPIC_CMD_BASE.rstrip('/')}/{pool}"
    payload = json.dumps({"pool": pool, "decision": decision, "ts": utcnow_iso()})
    qos = int(os.environ.get("MQTT_QOS", "1"))

    try:
        client = getattr(mqtt, "client", None)
        connected = client.is_connected() if client is not None else False
        result = mqtt.publish(topic, payload, qos=qos)
        if isinstance(result, tuple):
            rc, mid = result
        else:
            rc, mid = None, None
        return jsonify(
            {
                "ok": True,
                "topic": topic,
                "qos": qos,
                "rc": rc,
                "mid": mid,
                "connected": connected,
                "payload": payload,
            }
        )
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
