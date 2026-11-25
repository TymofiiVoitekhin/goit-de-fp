"""Перевірка результатів у вихідному Kafka-топіку athlete_enriched_agg."""

import json
import os
from kafka import KafkaConsumer


def main() -> None:
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    kafka_output_topic = os.getenv("KAFKA_OUTPUT_TOPIC", "athlete_enriched_agg")
    preview_limit = 20

    consumer = KafkaConsumer(
        kafka_output_topic,
        bootstrap_servers=[kafka_bootstrap],
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=5000,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )

    print(f"Зчитування повідомлень із Kafka-топіку '{kafka_output_topic}' (перші {preview_limit} записів):")
    print("-" * 100)

    total_messages = 0
    try:
        for message in consumer:
            total_messages += 1
            if total_messages <= preview_limit:
                print(f"[{total_messages:02d}] offset={message.offset} | {json.dumps(message.value, ensure_ascii=False)}")
    finally:
        consumer.close()

    print("-" * 100)
    print(f"Загалом прочитано повідомлень із топіку '{kafka_output_topic}': {total_messages}")


if __name__ == "__main__":
    main()
