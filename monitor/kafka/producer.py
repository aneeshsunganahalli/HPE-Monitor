import json
import logging
from kafka import KafkaProducer

log = logging.getLogger(__name__)
_producer = None

def get_producer():
    global _producer
    if _producer is None:
        _producer = KafkaProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks=1,
            retries=3,
        )
    return _producer

def publish_record(record: dict):
    try:
        get_producer().send("test-topic", value=record)
    except Exception as exc:
        log.warning("Kafka publish failed (poller continues): %s", exc)