import argparse
import json
import io
import os
import glob
import uuid
import time
import hashlib
from datetime import datetime, timezone

import requests
from pymongo import ASCENDING, errors

import utils

def process_audio(config):
    db = utils.get_mongo_db(config)
    minio_client = utils.get_minio_client(config)

    db["observations"].create_index([("_hash", ASCENDING)], unique=True)

    audio_bucket = config["audio_bucket"]
    logs_bucket = config["logs_bucket"]

    for bucket in (audio_bucket, logs_bucket):
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)
            print(f"Napravljen bucket: {bucket}")
    
    audio_dir = config["audio_directory"]
    audio_files = glob.glob(os.path.join(audio_dir, "*"))

    if not audio_files:
        print(f"Nema datoteke u {audio_dir}.")
        return
    print(f"Pronadeno {len(audio_files)} datoteka za obradu.")

    for file_path in audio_files:
        filename = os.path.basename(file_path)
        file_id = str(uuid.uuid4())
        object_name = f"{file_id}_{filename}"
        print(f"\nObradujem: {filename}")

        minio_client.fput_object(audio_bucket, object_name, file_path)
        print(f"  Uploadano na MinIO: {object_name}")

        lat = config["audio_location"]["latitude"]
        lon = config["audio_location"]["longitude"]

        db["audio_files"].insert_one({
            "file_id": file_id,
            "original_filename": filename,
            "minio_bucket": audio_bucket,
            "minio_object_name": object_name,
            "location": {"type": "Point", "coordinates": [lon, lat]},
            "uploaded_at": datetime.now(timezone.utc)
        })


        classify_url = config["classify_url"]
        request_id = str(uuid.uuid4())

        start = time.time()
        with open(file_path, "rb") as f:
            response = requests.post(classify_url, files={"file": (filename, f)})
        duration_ms = round((time.time() - start) * 1000)

        results = response.json().get("results", [])
        print(f"  Klasifikacija: {len(results)} rezultata (HTTP {response.status_code})")

        log_entry = {
            "request_id": request_id,
            "file_id": file_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "endpoint": classify_url,
            "status_code": response.status_code,
            "duration_ms": duration_ms,
            "response_body": response.json(),
        }

        log_bytes = json.dumps(log_entry).encode("utf-8")
        minio_client.put_object(
            logs_bucket,
            f"{request_id}.json",
            io.BytesIO(log_bytes),
            len(log_bytes),
            content_type="application/json",
        )
        print(f"  Log spremljen: {request_id}.json")

        taxonomy = db["taxonomy"]
        for r in results:
            sci_name = r.get("scientific_name")
            species = taxonomy.find_one({"canonicalName": sci_name})

            observation = {
                "scientific_name": sci_name,
                "common_name": r.get("common_name"),
                "confidence": r.get("confidence"),
                "start_time": r.get("start_time"),
                "end_time": r.get("end_time"),
                "source": "audio_classification",
                "audio_file_id": file_id,
                "minio_object_name": object_name,
                "location": {"type": "Point", "coordinates": [lon, lat]},
                "classified_at": datetime.now(timezone.utc),
            }

            if species:
                observation["key"] = species.get("key")
                observation["family"] = species.get("family")
                observation["order"] = species.get("order")

            identitet = f"{filename}|{sci_name}|{r.get('start_time')}|{r.get('end_time')}"
            observation["_hash"] = hashlib.md5(identitet.encode("utf-8")).hexdigest()

            try:
                db["observations"].insert_one(observation)
            except errors.DuplicateKeyError:
                pass

        print(f"  Spremljeno {len(results)} klasifikacija u observations.")

def main():
    parser = argparse.ArgumentParser(description="Korak 3: Obrada audio datoteka")
    parser.add_argument("--config", required=True, help="Putanja do config.yaml")
    args = parser.parse_args()

    config = utils.load_config(args.config)
    process_audio(config)


if __name__ == "__main__":
    main()