# Snakefile — orkestracija cijelog pipelinea za obradu podataka o pticama.
#
# Pokretanje (iz korijena projekta):
#   snakemake --cores 1
#   snakemake --cores 1 --config species_filter="grus"
#
# Snakemake kreće od cilja (rule all -> output/bird_report.csv) i sam posloži
# redoslijed koraka na temelju njihovih ulaza (input) i izlaza (output).

configfile: "config.yaml"


# Krajnji cilj pipelinea. Snakemake gleda što je potrebno da se ovo napravi.
rule all:
    input:
        "output/bird_report.csv"


# Korak 1 — Dohvat taksonomskih podataka.
# Izlaz je "flag" datoteka koja označava da je korak gotov.
rule fetch_taxonomy:
    output:
        touch("checkpoints/taxonomy_done.flag")
    shell:
        "python scripts/01_fetch_taxonomy.py --config config.yaml"


# Korak 2 — Konzumiranje Kafka poruka.
rule consume_kafka:
    output:
        touch("checkpoints/kafka_done.flag")
    shell:
        "python scripts/02_consume_kafka.py --config config.yaml"


# Korak 3 — Obrada audio datoteka (treba taksonomiju za povezivanje vrsta).
rule process_audio:
    input:
        "checkpoints/taxonomy_done.flag"
    output:
        touch("checkpoints/audio_done.flag")
    shell:
        "python scripts/03_process_audio.py --config config.yaml"


# Korak 4 — Generiranje CSV izvještaja (treba sve prethodne korake).
# species_filter se može zadati pri pokretanju: --config species_filter="grus"
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
        'python scripts/04_generate_report.py --config config.yaml '
        '--species-filter "{params.species_filter}"'
