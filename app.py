import json
import sqlite3
from os import getenv
from flask import Flask, request, g, make_response
from featureflag import FeatureFlags

app = Flask(__name__)


def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = sqlite3.connect("database.db")
    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


def add_message(message, type="prod", payload=None):
    db = get_db()
    if payload is None:
        payload = {}
    payload.update(
        {
            "message": message,
        }
    )
    if type == "test":
        payload["test"] = True

    db.cursor().execute(f"Insert into message values('{json.dumps(payload)}')")
    db.commit()


@app.route("/")
def index():
    return "Hello world"


@app.route("/api/messages")
def list_messages():
    db = get_db()
    messages = [json.loads(res[0]) for res in db.cursor().execute("select * from message").fetchall()]
    message_count = db.cursor().execute("select count(*) from message").fetchone()[0]
    return {"Response": "ok", "messages": messages, "total_messages": message_count}


@app.route("/api/messages", methods=["POST"])
def create_messages():
    # TODO: Validate existing payload
    # {
    #   "message": "" // required
    #   "type": "" // required
    #   "payload": {} // optional
    # }
    data = request.json
    if "message" not in data or "type" not in data:
        return Exception("You must provide message/type parameter")


    if "payload" in data and not FeatureFlags.get("PAYLOAD_ENABLED", False):
        raise Exception("You havn't paid for Payload feature.")

    add_message(data["message"], data["type"], data.get("payload"))
    return make_response({"success": "created message"}, 201)


if __name__ == "__main__":
    # Enable featureflags
    FeatureFlags["PAYLOAD_ENABLED"] = getenv("PAYLOAD_ENABLED")

    # DB INIT
    with app.app_context():
        db = get_db()
        db.cursor().execute("""
            create table if not exists message (
                payload char
            ) ;
        """)
        db.commit()

    # Create test messages
    with app.app_context():
        add_message("message 1", "test")
        add_message("message 2", "Production")
        add_message("message 3", payload={"numbers": [1, 2, 3]})
        add_message("message 4", "Production")

    app.run(host="0.0.0.0", port=5000, debug=True)
