import json
import os
import time
from flask import Flask, render_template, Response, request
from kafka import KafkaConsumer

app = Flask(__name__)

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:29092")
VALID_TOPIC = os.environ.get("VALID_TOPIC", "transactions_valid")
DLQ_TOPIC = os.environ.get("DLQ_TOPIC", "transactions_dlq")


def get_kafka_consumer():
    """Connect to Kafka and subscribe to valid and DLQ topics."""
    for attempt in range(1, 20):
        try:
            consumer = KafkaConsumer(
                VALID_TOPIC,
                DLQ_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="latest",
                consumer_timeout_ms=1000
            )
            print(f"[DASHBOARD] Subscribed to {VALID_TOPIC} and {DLQ_TOPIC} on {KAFKA_BROKER}")
            return consumer
        except Exception as e:
            print(f"[DASHBOARD] Waiting for Kafka broker... attempt {attempt}/20 ({e})")
            time.sleep(2)
    raise RuntimeError("Failed to connect to Kafka after multiple retries.")


@app.route("/")
def index():
    return render_template("index.html")


@app.after_request
def add_header(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, post-check=0, pre-check=0, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "-1"
    return response


@app.route("/log_error", methods=["POST"])
def log_error():
    print("FRONTEND ERROR:", request.json, flush=True)
    return "OK", 200


def stream_events():
    consumer = get_kafka_consumer()
    print("[DASHBOARD] SSE stream opened.")
    yield ": ping\n\n"
    
    while True:
        try:
            for message in consumer:
                payload = {
                    "topic": message.topic,
                    "data": message.value
                }
                yield f"data: {json.dumps(payload)}\n\n"
            time.sleep(0.1)
        except Exception as e:
            print(f"[DASHBOARD] Stream exception: {e}")
            time.sleep(1)


@app.route("/stream")
def stream():
    response = Response(stream_events(), mimetype="text/event-stream")
    response.headers["Cache-Control"] = "no-cache"
    response.headers["X-Accel-Buffering"] = "no"
    return response


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
