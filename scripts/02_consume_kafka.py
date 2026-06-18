import argparse
import json
from kafka import KafkaConsumer
import utils

def consume_kafka(config):
    db = utils.get_mongo_db(config)
    collection = db["observations"]

    consumer = KafkaConsumer(
        config["kafka_topic"],
        bootstrap_servers=config["kafka_broker"],
        auto_offset_reset="earliest",
        consumer_timeout_ms=10000,
        value_deserializer=lambda m:
        json.loads(m.decode("utf-8"))
    )

    opazanja = []
    for message in consumer:
        obs = message.value
        obs["source"] = "kafka"
        opazanja.append(obs)

    if opazanja:
        collection.insert_many(opazanja)
        print(f"Spremljeno {len(opazanja)} opazanja iz Kafke u MongoDB.")
    else:
        print("Nema novih poruka na Kafki.")

    consumer.close()


def main():
    parser = argparse.ArgumentParser(description="Korak 2: Konzumiranje Kafka poruka")
    parser.add_argument("--config", required=True, help="Putanja do config.yaml")
    args = parser.parse_args()

    config = utils.load_config(args.config)
    consume_kafka(config)

if __name__ == "__main__":
    main()