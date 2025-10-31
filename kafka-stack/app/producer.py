import json, os, time
from datetime import datetime
from kafka import KafkaProducer
TOPIC = os.getenv("KAFKA_TOPIC", "demo_topic")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
def serializer(v): return json.dumps(v, ensure_ascii=False).encode("utf-8")
def main():
    p = KafkaProducer(bootstrap_servers=[BOOTSTRAP], value_serializer=serializer, acks="all", retries=10, linger_ms=5)
    print(f"[producer] {BOOTSTRAP=} {TOPIC=}")
    for i in range(1, 11):
        msg = {"id": i, "event": "test_message", "ts": datetime.utcnow().isoformat()+"Z", "payload": {"hello":"world","idx":i}}
        md = p.send(TOPIC, value=msg).get(timeout=10)
        print(f"[producer] sent {i} -> {md.topic}:{md.partition}@{md.offset}")
        time.sleep(0.5)
    p.flush(); p.close(); print("[producer] done.")
if __name__ == "__main__": main()
