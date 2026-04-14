import yaml
from pymongo import MongoClient
from minio import Minio


def load_config(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def get_mongo_db(config: dict):
    client = MongoClient(config["mongo_uri"])
    return client[config["db_name"]]


def get_minio_client(config: dict) -> Minio:
    return Minio(
        config["minio_endpoint"],
        access_key=config["minio_access_key"],
        secret_key=config["minio_secret_key"],
        secure=config.get("minio_secure", False),
    )
