"""
Korak 1 — Dohvat taksonomskih podataka s https://aves.regoch.net/aves.json
i pohrana u MongoDB kolekciju 'taxonomy'.

Pokretanje:
    python scripts/01_fetch_taxonomy.py --config config.yaml
"""

import argparse
import sys
import requests
from pymongo import ASCENDING, errors

from utils import get_mongo_db, load_config


def fetch_taxonomy(config: dict) -> None:
    db = get_mongo_db(config)
    collection = db["taxonomy"]

    # Postavi unique index na 'key' (taxon_id) kako bi spriječili duplikate
    collection.create_index([("key", ASCENDING)], unique=True)

    # Provjeri postoje li već podaci
    if collection.count_documents({}) > 0:
        print("Taksonomski podaci već postoje u MongoDB-u, preskačem dohvat.")
        return

    taxonomy_url = config.get("taxonomy_url", "https://aves.regoch.net/aves.json")
    print(f"Dohvaćam taksonomske podatke s: {taxonomy_url}")

    try:
        response = requests.get(taxonomy_url, timeout=120)
        response.raise_for_status()
    except requests.RequestException as e:
        print(f"Greška pri dohvatu podataka: {e}", file=sys.stderr)
        sys.exit(1)

    species_data = response.json()

    if not isinstance(species_data, list) or len(species_data) == 0:
        print("API nije vratio listu vrsta.", file=sys.stderr)
        sys.exit(1)

    print(f"Dohvaćeno {len(species_data)} zapisa. Upisujem u MongoDB...")

    # Batch insert, preskoči duplikate zahvaljujući unique indexu
    batch_size = 500
    inserted_total = 0

    for i in range(0, len(species_data), batch_size):
        batch = species_data[i : i + batch_size]
        try:
            result = collection.insert_many(batch, ordered=False)
            inserted_total += len(result.inserted_ids)
        except errors.BulkWriteError as bwe:
            # Neki dokumenti su već postojali (duplikati) — to je ok
            inserted_total += bwe.details.get("nInserted", 0)

    print(f"Uspješno uneseno {inserted_total} vrsta u kolekciju 'taxonomy'.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Korak 1: Dohvat taksonomskih podataka")
    parser.add_argument("--config", required=True, help="Putanja do config.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    fetch_taxonomy(config)


if __name__ == "__main__":
    main()
