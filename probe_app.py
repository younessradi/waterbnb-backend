import json
import os
import socket
import ssl
import uuid
from datetime import datetime, timezone

from flask import Flask, jsonify, request
import paho.mqtt.client as mqtt


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
MQTT_FORCE_IPV4 = env_bool("MQTT_FORCE_IPV4", "true")
MQTT_KEEPALIVE = int(os.environ.get("MQTT_KEEPALIVE", "30"))
MQTT_PUBLISH_TIMEOUT = float(os.environ.get("MQTT_PUBLISH_TIMEOUT", "5"))
MQTT_TLS_INSECURE = env_bool("MQTT_TLS_INSECURE", "false")

app = Flask(__name__)

print(
    "[MQTT] config host={host} port={port} tls={tls} cmd_base={cmd} user_set={user}".format(
        host=MQTT_HOST,
        port=MQTT_PORT,
        tls=MQTT_TLS_ENABLED,
        cmd=TOPIC_CMD_BASE,
        user=bool(MQTT_USERNAME),
    )
)

def resolve_ipv4(host: str, port: int) -> str:
    if not MQTT_FORCE_IPV4:
        return host
    try:
        infos = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
        if infos:
            return infos[0][4][0]
    except OSError:
        return host
    return host


def publish_once(topic: str, payload: str, qos: int):
    host = resolve_ipv4(MQTT_HOST, MQTT_PORT)
    client_id = os.environ.get("MQTT_CLIENT_ID") or f"probe-{uuid.uuid4().hex[:12]}"
    client = mqtt.Client(client_id=client_id, protocol=mqtt.MQTTv311, transport="tcp")

    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    if MQTT_TLS_ENABLED:
        client.tls_set(
            ca_certs=MQTT_TLS_CA_CERTS or None,
            tls_version=ssl.PROTOCOL_TLSv1_2,
            ciphers=MQTT_TLS_CIPHERS,
        )
        if MQTT_TLS_INSECURE:
            client.tls_insecure_set(True)

    client.connect(host, MQTT_PORT, keepalive=MQTT_KEEPALIVE)
    client.loop_start()
    try:
        info = client.publish(topic, payload, qos=qos, retain=False)
        info.wait_for_publish(timeout=MQTT_PUBLISH_TIMEOUT)
        return {
            "host": host,
            "client_id": client_id,
            "connected": client.is_connected(),
            "mid": info.mid,
            "published": info.is_published(),
        }
    finally:
        client.disconnect()
        client.loop_stop()


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
        result = publish_once(topic, payload, qos=qos)
        return jsonify(
            {
                "ok": True,
                "topic": topic,
                "qos": qos,
                "connected": result["connected"],
                "published": result["published"],
                "mid": result["mid"],
                "host": result["host"],
                "client_id": result["client_id"],
                "payload": payload,
            }
        )
    except Exception as exc:
        return jsonify({"ok": False, "error": str(exc)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
