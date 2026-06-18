import argparse
import json
from kafka import KafkaConsumer
import utils
import hashlib
from pymongo import ASCENDING, errors

def consume_kafka(config):
    db = utils.get_mongo_db(config)
    collection = db["observations"]
    collection.create_index([("_hash", ASCENDING)], unique=True)

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

        tekst = json.dumps(obs, sort_keys=True)
        obs["_hash"] = hashlib.md5(tekst.encode("utf-8")).hexdigest()

        opazanja.append(obs)

    if opazanja:
        try:
            result = collection.insert_many(opazanja, ordered=False)
            spremljeno = len(result.inserted_ids)
        except errors.BulkWriteError as bwe:
            spremljeno = bwe.details.get("nInserted", 0)
        
        preskoceno = len(opazanja) - spremljeno

        print(f"Spremljeno {spremljeno} novih, preskoceno {preskoceno} duplikata.")
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