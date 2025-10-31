import json, os
from kafka import KafkaConsumer
TOPIC = os.getenv("KAFKA_TOPIC", "demo_topic")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "demo_group")
def main():
    c = KafkaConsumer(TOPIC, bootstrap_servers=[BOOTSTRAP], group_id=GROUP_ID,
                      auto_offset_reset="earliest", enable_auto_commit=True,
                      value_deserializer=lambda b: json.loads(b.decode("utf-8")))
    print(f"[consumer] {BOOTSTRAP=} {TOPIC=} {GROUP_ID=}")
    for m in c:
        print(f"[consumer] {m.topic}:{m.partition}@{m.offset} -> {m.value}")
if __name__ == "__main__": main()
