import os
import json
import ssl
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
MQTT_TLS_CIPHERS = os.environ.get("MQTT_TLS_CIPHERS")


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
pools = db["pools"]

# -----------------------
# Pool state (in-memory)
# -----------------------
# ident -> occupied bool
pool_occupied = {}

def load_pool_cache() -> None:
    loaded = 0
    try:
        for doc in pools.find({}, {"ident": 1, "occupied": 1}):
            ident = doc.get("ident")
            if ident is None or "occupied" not in doc:
                continue
            pool_occupied[ident] = bool(doc.get("occupied"))
            loaded += 1
        if loaded:
            print(f"[POOL] cache loaded {loaded} pools from Mongo")
    except Exception as exc:
        print("[POOL] cache load failed:", exc)


load_pool_cache()
 
def utcnow_iso():
    return datetime.now(timezone.utc).isoformat()

# -----------------------
# MQTT
# -----------------------
app.config["MQTT_BROKER_URL"] = MQTT_HOST
app.config["MQTT_BROKER_PORT"] = MQTT_PORT
app.config["MQTT_TLS_ENABLED"] = MQTT_TLS_ENABLED
# Render may spawn multiple workers; give each MQTT client a unique id to avoid broker disconnect churn.
app.config["MQTT_CLIENT_ID"] = os.environ.get("MQTT_CLIENT_ID") or f"waterbnb-{os.getpid()}"
if MQTT_USERNAME:
    app.config["MQTT_USERNAME"] = MQTT_USERNAME
if MQTT_PASSWORD:
    app.config["MQTT_PASSWORD"] = MQTT_PASSWORD
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
    if rc == 0:
        print("[MQTT] connected")
        mqtt.subscribe(TOPIC_POOLS)
        print(f"[MQTT] subscribed to {TOPIC_POOLS}")
    else:
        print("[MQTT] connect failed rc=", rc)


@mqtt.on_disconnect()
def on_disconnect(client, userdata, rc):
    print("[MQTT] disconnected rc=", rc)

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
        try:
            pools.update_one(
                {"ident": ident},
                {"$set": {"ident": ident, "occupied": occupied, "updated_at": datetime.now(timezone.utc)}},
                upsert=True,
            )
        except Exception as exc:
            print("[POOL] Mongo update failed:", exc)
        print(f"[POOL] {ident} occupied={occupied}")

    except Exception as e:
        print("[MQTT] bad payload:", e)

def publish_decision(pool_id: str, decision: str):
    topic = f"{TOPIC_CMD_BASE.rstrip('/')}/{pool_id}"
    payload = json.dumps({"pool": pool_id, "decision": decision, "ts": utcnow_iso()})
    try:
        client = getattr(mqtt, "client", None)
        if client is not None and not client.is_connected():
            print("[MQTT] publish while disconnected")
        result = mqtt.publish(topic, payload, qos=1)
        if isinstance(result, tuple):
            rc, mid = result
            print(f"[CMD] {topic} -> {payload} (rc={rc} mid={mid})")
        else:
            print(f"[CMD] {topic} -> {payload}")
    except Exception as exc:
        print("[CMD] publish failed:", exc)

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

    occupied = pool_occupied.get(idswp)
    pool_known = occupied is not None
    if not pool_known:
        doc = pools.find_one({"ident": idswp}, {"occupied": 1})
        if doc is not None and "occupied" in doc:
            occupied = bool(doc.get("occupied"))
            pool_occupied[idswp] = occupied
            pool_known = True

    pool_free = pool_known and (occupied is False)

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
