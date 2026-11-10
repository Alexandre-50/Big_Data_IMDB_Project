"""Measure null, missing and empty values for every column of the dataset.

A value is counted as missing when it is null (the IMDb '\\N' marker,
converted to null during ingestion) or an empty / blank string.
Each table is scanned only once: all the counters are computed in a
single aggregation.
"""
import csv

from pyspark.sql import functions as F

from src.common.paths import PARQUET_DIR, RESULTS_DIR
from src.common.spark_session import get_spark
from src.preparation.ingest_raw import IMDB_TABLES

OUTPUT_FILE = RESULTS_DIR / "preparation" / "missing_values.csv"
HEADER = ["table", "column", "total_rows", "null_count", "empty_count",
          "missing_count", "missing_pct"]


def missing_report(spark, table):
    """Return one row of missing-value statistics per column of a table."""
    df = spark.read.parquet(str(PARQUET_DIR / table.replace(".", "_")))
    aggregations = [F.count(F.lit(1)).alias("total_rows")]
    for column in df.columns:
        value = F.col(column)
        aggregations.append(
            F.sum(F.when(value.isNull(), 1).otherwise(0))
            .alias(f"{column}__null")
        )
        aggregations.append(
            F.sum(F.when(F.trim(value) == "", 1).otherwise(0))
            .alias(f"{column}__empty")
        )
    counts = df.agg(*aggregations).first()

    total = counts["total_rows"]
    report = []
    for column in df.columns:
        nulls = counts[f"{column}__null"]
        empties = counts[f"{column}__empty"]
        missing = nulls + empties
        pct = round(100 * missing / total, 2) if total else 0.0
        report.append([table, column, total, nulls, empties, missing, pct])
    return report


def print_markdown(rows):
    """Print rows as a Markdown table, ready to paste in REPORT.md."""
    print("| " + " | ".join(HEADER) + " |")
    print("|" + "---|" * len(HEADER))
    for row in rows:
        print("| " + " | ".join(str(value) for value in row) + " |")


def main():
    """Compute the report for every table, save it and print the top 5."""
    spark = get_spark("imdb-null-analysis")
    rows = []
    for table in IMDB_TABLES:
        rows.extend(missing_report(spark, table))
        print(f"{table}: done", flush=True)
    spark.stop()

    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_FILE, "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(HEADER)
        writer.writerows(rows)

    print("\n## Missing values per column\n")
    print_markdown(rows)
    print("\n## Top 5 columns with the highest missing percentage\n")
    print_markdown(sorted(rows, key=lambda row: row[-1], reverse=True)[:5])


if __name__ == "__main__":
    main()
