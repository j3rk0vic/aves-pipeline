import argparse
import json
import io
import os
import glob
import uuid
import time
from datetime import datetime, timezone

import requests

import utils

def process_audio(config):
    db = utils.get_mongo_db(config)
    minio_client = utils.get_minio_client(config)

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
        object_name = f