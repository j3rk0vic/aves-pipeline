"""
Korak 4 — Generiranje CSV izvještaja.

Dohvaća sve pozitivne audio klasifikacije iz MongoDB-a, grupira ih po vrsti,
očisti i transformira podatke (pandas), opcionalno primijeni fuzzy filter po
nazivu vrste, i spremi rezultat u CSV.

Pokretanje:
    python scripts/04_generate_report.py --config config.yaml
    python scripts/04_generate_report.py --config config.yaml --species-filter "Parus"
"""

import argparse
import os

import pandas as pd
from thefuzz import fuzz

import utils


def generate_report(config, species_filter=""):
    db = utils.get_mongo_db(config)

    # 1. Dohvati sve POZITIVNE audio klasifikacije ($gt = greater than, veće od 0)
    opazanja = list(db["observations"].find({
        "source": "audio_classification",
        "confidence": {"$gt": 0},
    }))

    if not opazanja:
        print("Nema klasifikacija za izvještaj.")
        return

    print(f"Učitano {len(opazanja)} klasifikacija iz MongoDB-a.")

    # 2. Stavi sve u pandas tablicu (DataFrame = tablica s redovima i stupcima)
    df = pd.DataFrame(opazanja)

    # 3. Osiguraj da svi stupci koje očekujemo postoje.
    #    (Neke vrste nisu povezane s taksonomijom pa nemaju family/order/key.)
    for col in ["scientific_name", "common_name", "family", "order", "key", "confidence"]:
        if col not in df.columns:
            df[col] = None

    # 4. Čišćenje podataka:
    #    - makni redove bez znanstvenog imena
    #    - makni suvišne razmake oko imena (normalizacija)
    df = df.dropna(subset=["scientific_name"])
    df["scientific_name"] = df["scientific_name"].astype(str).str.strip()
    df["common_name"] = df["common_name"].fillna("").astype(str).str.strip()

    # 5. Grupiraj po vrsti -> broj klasifikacija + statistika pouzdanosti.
    #    .agg(...) za svaku grupu izračuna više vrijednosti odjednom:
    #      - first  = uzmi prvu vrijednost (ime/obitelj su iste za istu vrstu)
    #      - count  = prebroji koliko redova ima ta vrsta
    #      - mean   = prosjek pouzdanosti
    #      - max    = najveća pouzdanost
    izvjestaj = df.groupby("scientific_name").agg(
        common_name=("common_name", "first"),
        family=("family", "first"),
        order=("order", "first"),
        taxon_key=("key", "first"),
        classification_count=("scientific_name", "count"),
        avg_confidence=("confidence", "mean"),
        max_confidence=("confidence", "max"),
    ).reset_index()

    # 6. Transformacije: zaokruži pouzdanost na 4 decimale, sortiraj po broju
    izvjestaj["avg_confidence"] = izvjestaj["avg_confidence"].round(4)
    izvjestaj["max_confidence"] = izvjestaj["max_confidence"].round(4)
    izvjestaj = izvjestaj.sort_values("classification_count", ascending=False)

    # 7. Opcionalni FUZZY filter po nazivu vrste.
    #    fuzz.partial_ratio vrati 0-100 koliko se dva niza poklapaju.
    #    Tako "Parus" pronađe "Parus major" iako nije identično.
    if species_filter:
        print(f"Primjenjujem fuzzy filter: '{species_filter}'")
        prag = 75  # koliko strogo mora biti poklapanje (0-100)
        upit = species_filter.lower()

        def poklapa(red):
            # usporedi upit s znanstvenim I uobičajenim imenom, uzmi bolji rezultat
            sci = str(red["scientific_name"]).lower()
            common = str(red["common_name"]).lower()
            rezultat = max(
                fuzz.token_set_ratio(upit, sci),
                fuzz.token_set_ratio(upit, common),
            )
            return rezultat >= prag

        maska = izvjestaj.apply(poklapa, axis=1)
        izvjestaj = izvjestaj[maska]
        print(f"Nakon filtera ostalo {len(izvjestaj)} vrsta.")

    # 8. Spremi u CSV (napravi output folder ako ne postoji)
    output_dir = config.get("output_directory", "./output")
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "bird_report.csv")
    izvjestaj.to_csv(output_path, index=False, encoding="utf-8")

    print(f"\nIzvještaj spremljen: {output_path} ({len(izvjestaj)} vrsta).\n")
    print(izvjestaj.to_string(index=False))


def main():
    parser = argparse.ArgumentParser(description="Korak 4: Generiranje CSV izvještaja")
    parser.add_argument("--config", required=True, help="Putanja do config.yaml")
    parser.add_argument("--species-filter", default="", help="Opcijski fuzzy filter po nazivu vrste")
    args = parser.parse_args()

    config = utils.load_config(args.config)
    generate_report(config, args.species_filter)


if __name__ == "__main__":
    main()
