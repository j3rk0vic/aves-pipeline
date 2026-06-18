from kafka import KafkaProducer
import json

opazanja = [
    {
        "key": 2473325,
        "latitude": 45.815,
        "longitude": 15.982,
        "body_size_cm": 14.5,
        "habitat": "forest"
    },
    {
        "key": 2473324,
        "latitude": 43.508,
        "longitude": 16.440,
        "body_temperature": 41.2, 
        "migration_status": "resident"
    }
]

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

for opazanje in opazanja:
    producer.send("bird-observations", value=opazanje)

broj_opazanja = len(opazanja)

print(f"Poslano {broj_opazanja} poruka.")

producer.flush()