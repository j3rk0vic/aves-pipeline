# Pipeline za obradu podataka o opažanju i taksonomiji ptica — Detaljne upute

---

## Pregled projekta

Cilj projekta je implementirati **pipeline od 4 koraka** koji:

1. Prikuplja taksonomske podatke o vrstama ptica s web servisa i sprema ih u MongoDB
2. Konzumira Kafka poruke s opažanjima ptica i sprema ih u MongoDB
3. Obrađuje audio datoteke (upload na MinIO + klasifikacija putem API-ja) i sprema rezultate u MongoDB
4. Generira završni CSV izvještaj sa statistikom, uz opciju fuzzy filtriranja po nazivu vrste

**Tehnologije koje se moraju koristiti:**
- **MinIO** (ili druga S3-kompatibilna pohrana) — za pohranu audio datoteka i logova
- **MongoDB** — za pohranu taksonomskih podataka, opažanja i rezultata klasifikacije
- **Apache Kafka** — kao izvor poruka s opažanjima
- **Python** — preporučeni jezik
- **Snakemake** — preporučeni orkestrator (jedna ulazna točka, parametri za vrijeme izvođenja)

**Vanjski servis:**
- `https://aves.regoch.net` — mock web stranica za taksonomske podatke i klasifikacijski API

---

## Arhitektura i struktura projekta

Preporučena struktura direktorija:

```
project/
├── Snakefile                  # Orkestrator — definira pravila i redoslijed izvršavanja
├── config.yaml                # Konfiguracija (MongoDB URI, MinIO credentials, Kafka broker, direktoriji...)
├── requirements.txt           # Python zavisnosti
├── .github/
│   └── workflows/
│       └── pipeline.yml       # GitHub Actions workflow (ručno okidanje)
├── scripts/
│   ├── 01_fetch_taxonomy.py   # Korak 1: Dohvat taksonomskih podataka
│   ├── 02_consume_kafka.py    # Korak 2: Konzumiranje Kafka poruka
│   ├── 03_process_audio.py    # Korak 3: Upload na MinIO + klasifikacija
│   ├── 04_generate_report.py  # Korak 4: Generiranje CSV izvještaja
│   └── utils.py               # Pomoćne funkcije (MongoDB konekcija, MinIO klijent, itd.)
├── audio_files/               # Lokalni direktorij s audio datotekama za obradu
├── output/                    # Direktorij za generirani CSV
└── README.md
```

---

## Korak 1 — Dohvat taksonomskih podataka (`01_fetch_taxonomy.py`)

### Što treba napraviti

1. **Dohvatiti sve podatke o vrstama ptica** s `https://aves.regoch.net`
2. **Provjeriti postoje li podaci već u MongoDB kolekciji** — ako da, preskočiti ovaj korak
3. **Pohraniti podatke u MongoDB kolekciju** (npr. `bird_species` ili `taxonomy`)

### Detalji implementacije

#### 1.1 Istraživanje API-ja

- Otvori `https://aves.regoch.net` u pregledniku i istraži koje rute/endpointe nudi
- Vjerojatno postoji endpoint poput `GET /api/species` ili slično koji vraća JSON popis vrsta
- Svaka vrsta će imati polja poput: `taxon_id`, `scientific_name`, `common_name`, `family`, `order`, itd.
- Koristi `requests` biblioteku ili `httpx` za dohvat

#### 1.2 Provjera duplikata

- Prije inserta, provjeri postoji li već podatak u kolekciji
- Možeš koristiti `collection.count_documents({})` — ako je > 0, preskoči
- Ili koristi `insert_many` s `ordered=False` i unique index na `taxon_id` (ili ekvivalentnom polju) tako da se duplikati automatski preskaču
- Preporučeno: kreiraj **unique index** na taksonomskom identifikatoru:
  ```python
  db.taxonomy.create_index("taxon_id", unique=True)
  ```

#### 1.3 Pohrana u MongoDB

- Kolekcija: `taxonomy` (ili `bird_species`)
- Svaki dokument bi trebao sadržavati barem:
  ```json
  {
    "taxon_id": 12345,
    "scientific_name": "Parus major",
    "common_name": "Great Tit",
    "family": "Paridae",
    "order": "Passeriformes",
    // ... sva ostala polja koja API vraća
  }
  ```

#### 1.4 Logika skripte (pseudokod)

```python
import requests
from pymongo import MongoClient

def fetch_taxonomy(config):
    client = MongoClient(config["mongo_uri"])
    db = client[config["db_name"]]
    collection = db["taxonomy"]

    # Provjera postoje li podaci
    if collection.count_documents({}) > 0:
        print("Taksonomski podaci već postoje, preskačem.")
        return

    # Dohvat podataka
    response = requests.get("https://aves.regoch.net/api/species")  # prilagodi URL
    species_data = response.json()

    # Insert u MongoDB
    if species_data:
        collection.insert_many(species_data)
        print(f"Uneseno {len(species_data)} vrsta u MongoDB.")
```

### Biblioteke

- `pymongo` — MongoDB driver
- `requests` ili `httpx` — HTTP klijent

---

## Korak 2 — Konzumiranje Kafka poruka (`02_consume_kafka.py`)

### Što treba napraviti

1. **Spojiti se na Kafka broker** i pročitati sve poruke prisutne u trenutku izvršavanja
2. **Parsirati svaku poruku** — mora sadržavati taksonomski kod ptice i geo lokaciju (lat, lon)
3. **Pohraniti sva opažanja u MongoDB**

### Detalji implementacije

#### 2.1 Kafka consumer konfiguracija

- Koristi `kafka-python` ili `confluent-kafka` biblioteku
- Postavi `auto_offset_reset='earliest'` da čitaš sve poruke od početka
- Postavi `consumer_timeout_ms` kako bi consumer prestao čitati kad nema više poruka (npr. `consumer_timeout_ms=10000`)
- Postavi `enable_auto_commit=True` ili ručno komitaj nakon obrade
- Topic/grupu definiraj u konfiguraciji

#### 2.2 Format poruka

Svaka poruka sadrži:
- **Taksonomski kod** (`taxon_id` ili `species_key`) — identifikator opažene ptice
- **Geografski položaj**: `latitude` i `longitude`
- **Varijabilna biološka svojstva** — poruke iz različitih izvora mogu imati različita polja! Npr.:
  - `body_size`, `body_temperature`, `migration_status`, `flight_pattern`, `habitat`
  - Neke poruke će imati sva polja, neke samo neka
  - **Ne hardkodirati strukturu** — čuvaj sva polja koja dođe u poruci

#### 2.3 Pohrana u MongoDB

- Kolekcija: `observations` (ili `sightings`)
- Svaki dokument:
  ```json
  {
    "taxon_id": 12345,
    "location": {
      "type": "Point",
      "coordinates": [longitude, latitude]
    },
    "source": "kafka",
    "observed_at": "2025-04-10T14:30:00Z",
    "body_size": 14.5,
    "habitat": "forest",
    // ... ostala polja koja su prisutna u poruci
  }
  ```
- Spremi `location` u GeoJSON formatu (opcionalno, ali dobra praksa)
- Dodaj polje `source: "kafka"` kako bi razlikovao ova opažanja od onih iz audio klasifikacije

#### 2.4 Logika skripte (pseudokod)

```python
from kafka import KafkaConsumer
import json
from pymongo import MongoClient

def consume_kafka_observations(config):
    client = MongoClient(config["mongo_uri"])
    db = client[config["db_name"]]
    collection = db["observations"]

    consumer = KafkaConsumer(
        config["kafka_topic"],
        bootstrap_servers=config["kafka_broker"],
        auto_offset_reset='earliest',
        consumer_timeout_ms=10000,
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    observations = []
    for message in consumer:
        obs = message.value
        # Dodaj izvor i timestamp
        obs["source"] = "kafka"
        observations.append(obs)

    if observations:
        collection.insert_many(observations)
        print(f"Uneseno {len(observations)} opažanja iz Kafke.")

    consumer.close()
```

### Biblioteke

- `kafka-python` ili `confluent-kafka`
- `pymongo`

### Važne napomene

- **Različita opažanja mogu sadržavati različita biološka svojstva** — ne filtriraj i ne odbacuj nepoznata polja. MongoDB je schema-less, pa to radi u tvoju korist.
- Ako ista poruka dođe dva puta, razmisli o deduplication strategiji (npr. hash poruke).

---

## Korak 3 — Obrada audio datoteka (`03_process_audio.py`)

### Što treba napraviti

1. **Pročitati sve audio datoteke** iz ciljnog direktorija
2. **Uploadati svaku datoteku na MinIO** (S3-kompatibilna pohrana)
3. **Poslati svaku datoteku na klasifikacijski API** (`POST https://aves.regoch.net/api/classify`)
4. **Pohraniti log zahtjeva u MinIO** (u proizvoljnom formatu — npr. JSON)
5. **Pohraniti rezultate klasifikacije u MongoDB**, povezujući ih s taksonomskim podacima

### Detalji implementacije

#### 3.1 MinIO setup

- Pokreni MinIO lokalno (Docker je najlakši pristup):
  ```bash
  docker run -d --name minio \
    -p 9000:9000 -p 9001:9001 \
    -e MINIO_ROOT_USER=minioadmin \
    -e MINIO_ROOT_PASSWORD=minioadmin \
    minio/minio server /data --console-address ":9001"
  ```
- Kreiraj bucket (npr. `bird-audio` i `request-logs`):
  ```python
  from minio import Minio

  client = Minio("localhost:9000", access_key="minioadmin", secret_key="minioadmin", secure=False)
  if not client.bucket_exists("bird-audio"):
      client.make_bucket("bird-audio")
  if not client.bucket_exists("request-logs"):
      client.make_bucket("request-logs")
  ```

#### 3.2 Upload datoteka na MinIO

- Iteriraj kroz sve datoteke u ciljnom direktoriju
- Za svaku datoteku generiraj **jedinstveni identifikator** (npr. UUID ili hash + originalni naziv)
- Uploadaj na MinIO:
  ```python
  import uuid

  file_id = str(uuid.uuid4())
  object_name = f"{file_id}_{original_filename}"
  client.fput_object("bird-audio", object_name, local_file_path)
  ```
- Pohrani metapodatke u MongoDB (kolekcija `audio_files` ili unutar `observations`):
  ```json
  {
    "file_id": "uuid-...",
    "original_filename": "recording_01.mp3",
    "minio_bucket": "bird-audio",
    "minio_object_name": "uuid_recording_01.mp3",
    "location": {
      "type": "Point",
      "coordinates": [longitude, latitude]
    },
    "uploaded_at": "2025-04-14T10:00:00Z"
  }
  ```

#### 3.3 Geo lokacija audio datoteka

- Projekt kaže: "možete pretpostaviti da su sve datoteke u određenoj mapi povezane s jednim geografskim položajem"
- Definir aj lokaciju kao parametar (u konfiguraciji ili kao argument pri pokretanju):
  ```yaml
  # config.yaml
  audio_directory: "./audio_files"
  audio_location:
    latitude: 45.815
    longitude: 15.982
  ```

#### 3.4 Klasifikacija putem API-ja

- Za svaku uploadanu datoteku, pošalji `POST` zahtjev:
  ```python
  url = "https://aves.regoch.net/api/classify"

  with open(file_path, "rb") as f:
      response = requests.post(url, files={"file": (filename, f)})
  
  result = response.json()
  ```
- **Istraži API** — možda prima `multipart/form-data` s audio datotekom, možda prima i dodatne parametre
- **Odgovor klasifikatora** će sadržavati:
  - Identificirane vrste ptica
  - Confidence score (ocjena pouzdanosti)
  - Primjer odgovora (pretpostavljeni format):
    ```json
    {
      "classifications": [
        {
          "taxon_id": 12345,
          "scientific_name": "Parus major",
          "confidence": 0.87
        },
        {
          "taxon_id": 67890,
          "scientific_name": "Erithacus rubecula",
          "confidence": 0.12
        }
      ]
    }
    ```

#### 3.5 Pohrana loga zahtjeva u MinIO

- Za svaki API poziv, kreiraj log datoteku (JSON format je najjednostavniji):
  ```json
  {
    "request_id": "uuid-...",
    "file_id": "uuid-...",
    "timestamp": "2025-04-14T10:01:00Z",
    "endpoint": "https://aves.regoch.net/api/classify",
    "status_code": 200,
    "response_body": { ... },
    "duration_ms": 340
  }
  ```
- Uploadaj log na MinIO u bucket `request-logs`:
  ```python
  import json, io

  log_data = json.dumps(log_entry).encode("utf-8")
  client.put_object(
      "request-logs",
      f"{request_id}.json",
      io.BytesIO(log_data),
      len(log_data),
      content_type="application/json"
  )
  ```

#### 3.6 Pohrana rezultata klasifikacije u MongoDB

- Kolekcija: `observations` (ili zasebna `classifications`)
- Svaki rezultat klasifikacije postaje opažanje:
  ```json
  {
    "taxon_id": 12345,
    "scientific_name": "Parus major",
    "confidence": 0.87,
    "source": "audio_classification",
    "audio_file_id": "uuid-...",
    "minio_object_name": "uuid_recording_01.mp3",
    "location": {
      "type": "Point",
      "coordinates": [15.982, 45.815]
    },
    "classified_at": "2025-04-14T10:01:00Z"
  }
  ```
- **Poveži s taksonomskim podacima** — koristi `taxon_id` kao vezu prema `taxonomy` kolekciji. Možeš koristiti MongoDB `$lookup` ili referencu.

### Biblioteke

- `minio` — MinIO Python SDK
- `requests` — za API pozive
- `pymongo`

---

## Korak 4 — Generiranje CSV izvještaja (`04_generate_report.py`)

### Što treba napraviti

1. **Dohvatiti sve vrste ptica koje imaju barem jednu pozitivnu klasifikaciju** iz MongoDB-a
2. **Očistiti i transformirati podatke** prije generiranja izvještaja
3. **Generirati CSV** s nazivom vrste, brojem klasificiranih opažanja i relevantnim podacima
4. **Implementirati opcijski fuzzy filter** prema nazivu vrste

### Detalji implementacije

#### 4.1 Agregacija podataka iz MongoDB-a

- Koristi MongoDB **aggregation pipeline** za spajanje podataka:
  ```python
  pipeline = [
      # Filtriraj samo klasifikacije s pozitivnom pouzdanošću
      {"$match": {"source": "audio_classification", "confidence": {"$gt": 0}}},
      # Grupiraj po vrsti
      {"$group": {
          "_id": "$taxon_id",
          "count": {"$sum": 1},
          "avg_confidence": {"$avg": "$confidence"},
          "locations": {"$push": "$location"},
          "observations_data": {"$push": "$$ROOT"}
      }},
      # Spoji s taksonomskim podacima
      {"$lookup": {
          "from": "taxonomy",
          "localField": "_id",
          "foreignField": "taxon_id",
          "as": "species_info"
      }},
      {"$unwind": "$species_info"}
  ]

  results = list(db.observations.aggregate(pipeline))
  ```

#### 4.2 Čišćenje i transformacija podataka

Primijeni odgovarajuće transformacije — npr.:
- Ukloni duplikate
- Normaliziraj nazive vrsta (ujednači veliko/malo slovo)
- Zaokruži confidence na razumnu preciznost
- Ukloni outliere ili nevalidne zapise (npr. confidence = 0)
- Popuni nedostajuće vrijednosti ili ih označi
- Koristi `pandas` za manipulaciju:
  ```python
  import pandas as pd

  df = pd.DataFrame(results)
  df["species_name"] = df["species_info"].apply(lambda x: x.get("scientific_name", ""))
  df["common_name"] = df["species_info"].apply(lambda x: x.get("common_name", ""))
  # Čišćenje
  df = df.drop_duplicates(subset=["_id"])
  df["avg_confidence"] = df["avg_confidence"].round(4)
  ```

#### 4.3 Fuzzy filtriranje

- Implementiraj opcijski parametar (npr. `--species-filter "velika sje"`)
- Koristi `fuzzywuzzy` ili `thefuzz` biblioteku:
  ```python
  from thefuzz import fuzz, process

  def fuzzy_filter(df, query, threshold=60):
      if not query:
          return df
      
      # Usporedi query sa svim imenima vrsta
      matches = df["species_name"].apply(
          lambda name: fuzz.partial_ratio(query.lower(), name.lower())
      )
      return df[matches >= threshold]
  ```
- Threshold prilagodi po potrebi (60–80 je razumna vrijednost)

#### 4.4 Generiranje CSV-a

- Stupci u CSV-u:
  - `species_name` (znanstveni naziv)
  - `common_name` (uobičajeni naziv)
  - `taxon_id`
  - `classification_count` (broj pozitivnih klasifikacija)
  - `avg_confidence` (prosječna pouzdanost)
  - Svi relevantni podaci o promatranjima (lokacije, biološka svojstva...)
- Spremi:
  ```python
  df.to_csv("output/bird_report.csv", index=False, encoding="utf-8")
  ```

### Biblioteke

- `pandas` — manipulacija i čišćenje podataka
- `thefuzz` (ranije `fuzzywuzzy`) — fuzzy string matching
- `pymongo`

---

## Orkestracija sa Snakemake

### Snakefile

```python
configfile: "config.yaml"

rule all:
    input:
        "output/bird_report.csv"

rule fetch_taxonomy:
    output:
        touch("checkpoints/taxonomy_done.flag")
    shell:
        "python scripts/01_fetch_taxonomy.py --config config.yaml"

rule consume_kafka:
    output:
        touch("checkpoints/kafka_done.flag")
    shell:
        "python scripts/02_consume_kafka.py --config config.yaml"

rule process_audio:
    input:
        "checkpoints/taxonomy_done.flag"
    output:
        touch("checkpoints/audio_done.flag")
    shell:
        "python scripts/03_process_audio.py --config config.yaml"

rule generate_report:
    input:
        "checkpoints/taxonomy_done.flag",
        "checkpoints/kafka_done.flag",
        "checkpoints/audio_done.flag"
    output:
        "output/bird_report.csv"
    params:
        species_filter=config.get("species_filter", "")
    shell:
        "python scripts/04_generate_report.py --config config.yaml "
        "--species-filter '{params.species_filter}'"
```

### Pokretanje

```bash
# Bez filtera
snakemake --cores 1

# S fuzzy filterom
snakemake --cores 1 --config species_filter="Parus"
```

---

## Konfiguracija (`config.yaml`)

```yaml
# MongoDB
mongo_uri: "mongodb://localhost:27017"
db_name: "bird_pipeline"

# MinIO
minio_endpoint: "localhost:9000"
minio_access_key: "minioadmin"
minio_secret_key: "minioadmin"
minio_secure: false
audio_bucket: "bird-audio"
logs_bucket: "request-logs"

# Kafka
kafka_broker: "localhost:9092"
kafka_topic: "bird-observations"
kafka_group_id: "bird-pipeline-consumer"

# Audio
audio_directory: "./audio_files"
audio_location:
  latitude: 45.815
  longitude: 15.982

# Report
output_directory: "./output"
species_filter: ""  # Opcijski fuzzy filter
```

---

## Docker Compose za infrastrukturu

Preporučeno je koristiti Docker Compose za pokretanje svih servisa:

```yaml
version: '3.8'

services:
  mongodb:
    image: mongo:7
    ports:
      - "27017:27017"
    volumes:
      - mongo_data:/data/db

  minio:
    image: minio/minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    command: server /data --console-address ":9001"
    volumes:
      - minio_data:/data

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1

volumes:
  mongo_data:
  minio_data:
```

Pokretanje:
```bash
docker-compose up -d
```

---

## GitHub Actions Workflow (bonus bodovi)

Datoteka: `.github/workflows/pipeline.yml`

```yaml
name: Bird Pipeline

on:
  workflow_dispatch:
    inputs:
      species_filter:
        description: 'Fuzzy filter po nazivu vrste'
        required: false
        default: ''

jobs:
  run-pipeline:
    runs-on: ubuntu-latest

    services:
      mongodb:
        image: mongo:7
        ports:
          - 27017:27017
      minio:
        image: minio/minio
        ports:
          - 9000:9000
        env:
          MINIO_ROOT_USER: minioadmin
          MINIO_ROOT_PASSWORD: minioadmin
        options: >-
          --health-cmd "curl -f http://localhost:9000/minio/health/live"
          --health-interval 10s
          --health-timeout 5s
          --health-retries 3

    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install snakemake

      - name: Run pipeline
        run: |
          snakemake --cores 1 --config species_filter="${{ github.event.inputs.species_filter }}"

      - name: Upload report
        uses: actions/upload-artifact@v4
        with:
          name: bird-report
          path: output/bird_report.csv
```

---

## Python zavisnosti (`requirements.txt`)

```
pymongo>=4.6
minio>=7.2
requests>=2.31
kafka-python>=2.0
pandas>=2.1
thefuzz>=0.20
python-Levenshtein>=0.23
snakemake>=8.0
pyyaml>=6.0
```

---

## MongoDB kolekcije — pregled

| Kolekcija | Sadržaj | Ključna polja |
|---|---|---|
| `taxonomy` | Taksonomski podaci o vrstama ptica | `taxon_id` (unique index), `scientific_name`, `common_name`, `family`, `order` |
| `observations` | Sva opažanja (Kafka + klasifikacija) | `taxon_id`, `source` ("kafka" ili "audio_classification"), `location`, `confidence`, `audio_file_id` |
| `audio_files` | Metapodaci o uploadanim audio datotekama | `file_id`, `original_filename`, `minio_bucket`, `minio_object_name`, `location` |

---

## MinIO bucketi — pregled

| Bucket | Sadržaj | Format objekata |
|---|---|---|
| `bird-audio` | Audio datoteke (mp3, wav, ogg...) | `{uuid}_{original_filename}` |
| `request-logs` | Logovi API poziva klasifikatoru | `{request_uuid}.json` |

---

## Checklist za bodovanje

### IU2 Minimalni (10 bodova)
- [ ] Audio datoteke iz direktorija se uploadaju na MinIO
- [ ] Svaka datoteka se može dohvatiti i ima jedinstveni identifikator
- [ ] Metapodaci (lokacija, naziv datoteke) su povezani s datotekom i pohranjeni u MongoDB
- [ ] (Opcionalno) Podrška za datoteke s Google Drivea

### IU2 Željeni (10 bodova)
- [ ] Za svaku datoteku se šalje `POST` na `https://aves.regoch.net/api/classify`
- [ ] Log svakog API zahtjeva se pohranjuje u MinIO
- [ ] Rezultati klasifikacije se spremaju u MongoDB kolekciju
- [ ] Taksonomski podaci su povezani s opažanjima

### IU3 Minimalni (10 bodova)
- [ ] Taksonomski podaci se prikupljaju s `https://aves.regoch.net`
- [ ] Podaci se spremaju u MongoDB bez duplikata
- [ ] Rezultati klasifikacije su u MongoDB kolekciji i povezani s vrstama

### IU3 Željeni — Kafka (5 bodova)
- [ ] Kafka poruke se konzumiraju
- [ ] Opažanja se pohranjuju s `taxon_id`, lokacijom i svim biološkim svojstvima
- [ ] Podržana su različita biološka svojstva iz različitih izvora

### IU3 Željeni — CSV i fuzzy (5 bodova)
- [ ] CSV izvještaj se generira za vrste s barem jednom pozitivnom klasifikacijom
- [ ] Fuzzy string matching filter je implementiran
- [ ] Podaci su očišćeni i transformirani prije generiranja CSV-a

### Bonus
- [ ] Snakemake orkestracija s jednom ulaznom točkom
- [ ] Opcijski parametri za vrijeme izvođenja (npr. fuzzy filter)
- [ ] GitHub Actions workflow s ručnim okidanjem
- [ ] Vizualizacija generiranog izvješća

---

## Savjeti i česte greške

1. **Uvijek prvo istraži API** — otvori `https://aves.regoch.net` i pogledaj dokumentaciju, testiraj endpointe ručno (curl/Postman) prije pisanja koda
2. **Ne hardkodiraj shemu za Kafka poruke** — različiti ornitolozi šalju različita polja, spremi sve što dođe
3. **Unique indexi na MongoDB** — postavi ih odmah na početku, spriječit će duplikate bez dodatne logike
4. **Error handling** — API pozivi i Kafka mogu biti nestabilni, koristi `try/except` i `retries`
5. **Idempotentnost** — pipeline bi trebao dati isti rezultat ako se pokrene više puta (provjeri duplikate!)
6. **Testiranje** — pripremiti mock audio datoteke za testiranje uploada i klasifikacije
7. **MinIO konzola** — dostupna na `http://localhost:9001`, korisna za vizualnu provjeru uploadanih datoteka
