import json
import os
import random
import time
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.environ.get("TOPIC_NAME", "transactions")

VALID_SOURCES = ["mobile", "web", "pos"]
INVALID_SOURCES = ["smart_tv", "unknown_kiosk", "smart_watch", "crypto_terminal"]


def get_kafka_producer(broker: str, max_retries: int = 15, retry_interval: int = 2) -> KafkaProducer:
    """Connect to Kafka with retries."""
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=broker,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",
                retries=3,
            )
            print(f"[PRODUCER] Connected to Kafka broker at {broker}")
            return producer
        except NoBrokersAvailable:
            print(f"[PRODUCER] Broker {broker} not available (attempt {attempt}/{max_retries}). Retrying in {retry_interval}s...")
            time.sleep(retry_interval)
        except Exception as exc:
            print(f"[PRODUCER] Connection error: {exc}. Retrying in {retry_interval}s...")
            time.sleep(retry_interval)

    raise RuntimeError(f"[PRODUCER] Could not connect to Kafka broker after {max_retries} attempts.")


def generate_valid_transaction() -> dict:
    """Generate a standard valid transaction event."""
    user_num = random.randint(10000, 99999)
    # Valid amount between 1 and 10,000,000 (typical values 10,000 - 2,500,000)
    amount = random.choice([
        random.randint(10000, 500000),
        random.randint(500000, 2000000),
        random.randint(2000000, 5000000),
        random.randint(1, 10000000)
    ])
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    source = random.choice(VALID_SOURCES)

    return {
        "user_id": f"U{user_num}",
        "amount": amount,
        "timestamp": now_utc,
        "source": source
    }


def generate_invalid_transaction(category: str = None) -> tuple[dict, str]:
    """
    Generate an INVALID transaction event to test data validation rules:
    1. amount negative or excessive (> 10,000,000 or <= 0)
    2. timestamp invalid (malformed format or unparseable)
    3. source unknown (not in ['mobile', 'web', 'pos'])
    4. missing mandatory fields (user_id, amount, timestamp)
    """
    categories = ["amount_negative", "amount_too_high", "timestamp_invalid", "source_unknown", "missing_user_id", "missing_amount"]
    chosen_cat = category or random.choice(categories)

    base = generate_valid_transaction()

    if chosen_cat == "amount_negative":
        base["amount"] = random.choice([-50000, -100000, 0, -1])
        desc = f"INVALID: Negative/Zero amount ({base['amount']})"
    elif chosen_cat == "amount_too_high":
        base["amount"] = random.choice([15000000, 50000000, 99999999])
        desc = f"INVALID: Amount exceeds 10M range ({base['amount']:,})"
    elif chosen_cat == "timestamp_invalid":
        base["timestamp"] = random.choice([
            "2025-99-99T99:99:99Z",
            "INVALID_TIMESTAMP_STRING",
            "14-12-2025 09:00:20",
            ""
        ])
        desc = f"INVALID: Malformed timestamp ('{base['timestamp']}')"
    elif chosen_cat == "source_unknown":
        base["source"] = random.choice(INVALID_SOURCES)
        desc = f"INVALID: Unknown source ('{base['source']}')"
    elif chosen_cat == "missing_user_id":
        base["user_id"] = None
        desc = "INVALID: Missing mandatory field 'user_id' (null)"
    elif chosen_cat == "missing_amount":
        base["amount"] = None
        desc = "INVALID: Missing mandatory field 'amount' (null)"
    else:
        base["amount"] = -1000
        desc = "INVALID: Generic invalid amount"

    return base, desc


def generate_late_transaction() -> tuple[dict, str]:
    """
    Generate a LATE event (timestamp 4 - 10 minutes in the past)
    to simulate events arriving past the 3-minute watermark threshold.
    """
    user_num = random.randint(10000, 99999)
    amount = random.randint(50000, 500000)
    source = random.choice(VALID_SOURCES)

    # Offset timestamp by 4 to 10 minutes in the past (> 3 minutes watermark)
    delay_minutes = random.randint(4, 10)
    past_time = datetime.now(timezone.utc) - timedelta(minutes=delay_minutes)
    past_timestamp_str = past_time.strftime("%Y-%m-%dT%H:%M:%SZ")

    event = {
        "user_id": f"U{user_num}",
        "amount": amount,
        "timestamp": past_timestamp_str,
        "source": source
    }
    desc = f"LATE EVENT: Arrived with timestamp {delay_minutes} mins in past ('{past_timestamp_str}')"
    return event, desc


def main():
    print("=" * 70)
    print("🚀 TRANSACTION EVENT PRODUCER STARTING")
    print(f"Target Broker : {KAFKA_BROKER}")
    print(f"Target Topic  : {TOPIC_NAME}")
    print("=" * 70)

    producer = get_kafka_producer(KAFKA_BROKER)

    # Cache recent events for generating realistic duplicate events
    recent_events_cache = []
    event_counter = 0

    # Ensure at least 3 invalid events and 3 late events are sent early,
    # followed by continuous realistic streaming.
    initial_invalids = ["amount_negative", "timestamp_invalid", "source_unknown", "amount_too_high"]
    initial_late_count = 3

    print("[PRODUCER] Beginning stream generation (1-2 seconds per event)...")

    try:
        while True:
            event_counter += 1
            event = None
            log_tag = "[VALID]"
            log_desc = ""

            # Schedule:
            # 1. Force the first required invalid events and late events
            if initial_invalids:
                cat = initial_invalids.pop(0)
                event, log_desc = generate_invalid_transaction(cat)
                log_tag = "[INVALID]"
            elif initial_late_count > 0:
                initial_late_count -= 1
                event, log_desc = generate_late_transaction()
                log_tag = "[LATE EVENT]"
            else:
                # Weighted random selection for continuous stream:
                # 70% Valid, 12% Invalid, 10% Late Event, 8% Duplicate Event
                rand_val = random.random()

                if rand_val < 0.70:
                    event = generate_valid_transaction()
                    log_tag = "[VALID]"
                    log_desc = f"Amount: Rp {event['amount']:,} | Source: {event['source']}"
                    # Add to cache for duplicate simulation (keep max 20)
                    recent_events_cache.append(dict(event))
                    if len(recent_events_cache) > 20:
                        recent_events_cache.pop(0)

                elif rand_val < 0.82:
                    event, log_desc = generate_invalid_transaction()
                    log_tag = "[INVALID]"

                elif rand_val < 0.92:
                    event, log_desc = generate_late_transaction()
                    log_tag = "[LATE EVENT]"

                else:
                    if recent_events_cache:
                        # Re-send an exact duplicate from recent events
                        dup_event = random.choice(recent_events_cache)
                        event = dict(dup_event)
                        log_tag = "[DUPLICATE]"
                        log_desc = f"Duplicate event for User {event.get('user_id')} at {event.get('timestamp')}"
                    else:
                        event = generate_valid_transaction()
                        log_tag = "[VALID]"
                        log_desc = f"Amount: Rp {event['amount']:,} | Source: {event['source']}"

            # Send event to Kafka topic
            producer.send(TOPIC_NAME, value=event)
            producer.flush()

            print(f"#{event_counter:04d} {log_tag:<13} | {event} | {log_desc}")

            # Sleep 1 - 2 seconds as required by guidance
            sleep_time = random.uniform(1.0, 2.0)
            time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("\n[PRODUCER] Stopped by user.")
    finally:
        producer.close()
        print("[PRODUCER] Kafka producer connection closed.")


if __name__ == "__main__":
    main()
