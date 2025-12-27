import os
import json
from datetime import datetime, timezone

from flask import Flask, request, jsonify
from flask_mqtt import Mqtt
from pymongo import MongoClient

# -----------------------
# CONFIG (simple)
# -----------------------
MQTT_HOST = os.environ.get("MQTT_HOST", "test.mosquitto.org")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
TOPIC_POOLS = os.environ.get("TOPIC_POOLS", "uca/iot/piscine")
TOPIC_CMD_BASE = os.environ.get("TOPIC_CMD_BASE", "uca/iot/piscine/cmd")
MQTT_USERNAME = os.environ.get("MQTT_USERNAME")
MQTT_PASSWORD = os.environ.get("MQTT_PASSWORD")
MQTT_TLS_CA_CERTS = os.environ.get("MQTT_TLS_CA_CERTS")


def env_bool(name: str, default: str = "false") -> bool:
    return os.environ.get(name, default).strip().lower() in {"1", "true", "yes", "on"}


MQTT_TLS_ENABLED = env_bool("MQTT_TLS_ENABLED", "false")

MONGODB_URI = os.environ.get("MONGODB_URI")  # REQUIRED
if not MONGODB_URI:
    raise RuntimeError("Missing MONGODB_URI env var")

# -----------------------
# APP + DB
# -----------------------
app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-secret")

mongo = MongoClient(MONGODB_URI)
db = mongo["WaterBnB"]
users = db["users"]
access_logs = db["access_logs"]

# -----------------------
# Pool state (in-memory)
# -----------------------
# ident -> occupied bool
pool_occupied = {}

 
def utcnow_iso():
    return datetime.now(timezone.utc).isoformat()

# -----------------------
# MQTT
# -----------------------
app.config["MQTT_BROKER_URL"] = MQTT_HOST
app.config["MQTT_BROKER_PORT"] = MQTT_PORT
app.config["MQTT_TLS_ENABLED"] = MQTT_TLS_ENABLED
if MQTT_USERNAME:
    app.config["MQTT_USERNAME"] = MQTT_USERNAME
if MQTT_PASSWORD:
    app.config["MQTT_PASSWORD"] = MQTT_PASSWORD
if MQTT_TLS_CA_CERTS:
    app.config["MQTT_TLS_CA_CERTS"] = MQTT_TLS_CA_CERTS

mqtt = Mqtt(app)

@mqtt.on_connect()
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("[MQTT] connected")
        mqtt.subscribe(TOPIC_POOLS)
        print(f"[MQTT] subscribed to {TOPIC_POOLS}")
    else:
        print("[MQTT] connect failed rc=", rc)

@mqtt.on_message()
def on_message(client, userdata, msg):
    if msg.topic != TOPIC_POOLS:
        return
    try:
        payload = msg.payload.decode("utf-8", errors="replace")
        dic = json.loads(payload)

        ident = dic["info"]["ident"]

        # Node-RED flow uses piscine.oocuped (typo) -> accept that first
        # if "piscine" in dic and "oocuped" in dic["piscine"]:
        #     occupied = bool(dic["piscine"]["oocuped"])
        # elif "piscine" in dic and "occupied" in dic["piscine"]:
        #     occupied = bool(dic["piscine"]["occupied"])
        # elif "status" in dic and "occupied" in dic["status"]:
        #     occupied = bool(dic["status"]["occupied"])
        # else:
        #     # If unknown, assume occupied (deny) to avoid false opens
        #     occupied = True

        if "piscine" in dic and "occuped" in dic["piscine"]:
            occupied = bool(dic["piscine"]["occuped"])
        elif "piscine" in dic and "oocuped" in dic["piscine"]:
            occupied = bool(dic["piscine"]["oocuped"])
        elif "piscine" in dic and "occupied" in dic["piscine"]:
            occupied = bool(dic["piscine"]["occupied"])
        elif "status" in dic and "occupied" in dic["status"]:
            occupied = bool(dic["status"]["occupied"])
        else:
            occupied = True


        pool_occupied[ident] = occupied
        print(f"[POOL] {ident} occupied={occupied}")

    except Exception as e:
        print("[MQTT] bad payload:", e)

def publish_decision(pool_id: str, decision: str):
    topic = f"{TOPIC_CMD_BASE}/{pool_id}"
    payload = json.dumps({"pool": pool_id, "decision": decision, "ts": utcnow_iso()})
    mqtt.publish(topic, payload)
    print(f"[CMD] {topic} -> {payload}")

@app.get("/")
def health():
    return "OK"

@app.get("/open")
def open_pool():
    idu = (request.args.get("idu") or "").strip()
    idswp = (request.args.get("idswp") or "").strip()

    if not idu or not idswp:
        return jsonify({"ok": False, "error": "missing idu or idswp"}), 400

    user_ok = users.find_one({"name": idu}) is not None

    pool_known = idswp in pool_occupied
    pool_free = pool_known and (pool_occupied[idswp] is False)

    granted = user_ok and pool_free
    decision = "granted" if granted else "refused"

    # Send decision to device
    publish_decision(idswp, decision)

    doc = {
        "ts": datetime.now(timezone.utc),
        "idu": idu,
        "idswp": idswp,
        "decision": decision,
        "reasons": {
            "user_ok": user_ok,
            "pool_known": pool_known,
            "pool_free": pool_free
        }
    }
    access_logs.insert_one(doc)

    return jsonify({
        "ok": True,
        "idu": idu,
        "idswp": idswp,
        "decision": decision,
        "reasons": {
            "user_ok": user_ok,
            "pool_known": pool_known,
            "pool_free": pool_free
        }
    }), 200

if __name__ == "__main__":
    # IMPORTANT: 0.0.0.0 allows other devices on your LAN to reach it
    app.run(host="0.0.0.0", port=5000, debug=False)
