"""Convert the raw IMDb TSV files into Parquet.

All columns are kept as strings at this stage. Types are enforced later,
during cleaning, so that malformed values can still be detected.
"""
from src.common.paths import PARQUET_DIR, RAW_DIR
from src.common.spark_session import get_spark

IMDB_TABLES = [
    "name.basics",
    "title.akas",
    "title.basics",
    "title.crew",
    "title.episode",
    "title.principals",
    "title.ratings",
]


def read_imdb_tsv(spark, table):
    """Read one IMDb TSV file, with '\\N' interpreted as null."""
    return (
        spark.read
        .option("header", True)
        .option("sep", "\t")
        .option("quote", "")  # Titles contain quotes: disable quoting.
        .option("nullValue", "\\N")
        .csv(str(RAW_DIR / f"{table}.tsv.gz"))
    )


def main():
    """Convert every IMDb table to Parquet and print its row count.

    Tables already converted (folder with a _SUCCESS marker) are skipped,
    so the script can be safely re-run after an interruption.
    """
    spark = get_spark("imdb-ingestion")
    for table in IMDB_TABLES:
        output_dir = PARQUET_DIR / table.replace(".", "_")
        if (output_dir / "_SUCCESS").exists():
            print(f"{table}: already converted, skipped", flush=True)
            continue
        read_imdb_tsv(spark, table).write.mode("overwrite").parquet(
            str(output_dir)
        )
        rows = spark.read.parquet(str(output_dir)).count()
        print(f"{table}: {rows} rows", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
