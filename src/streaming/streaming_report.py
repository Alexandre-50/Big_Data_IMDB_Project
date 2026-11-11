"""Summarize a streaming test run: volumes, throughput, lateness, top titles.

Reads the Parquet outputs and the progress log written by
streaming_pipeline.py and prints Markdown tables ready for REPORT.md.
The tables are also saved in results/streaming/summary.md.

Usage (inside the imdb_spark container):
    spark-submit src/streaming/streaming_report.py
"""
import json

from pyspark.sql import functions as F

from src.common.paths import RESULTS_DIR
from src.common.spark_session import get_spark

OUTPUT_DIR = RESULTS_DIR / "streaming"
PROGRESS_FILE = OUTPUT_DIR / "progress.jsonl"
SUMMARY_FILE = OUTPUT_DIR / "summary.md"
TOP_N = 10


def markdown_table(header, rows):
    """Return rows as a Markdown table."""
    lines = ["| " + " | ".join(header) + " |",
             "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(str(value) for value in row) + " |"
              for row in rows]
    return "\n".join(lines)


def mean(values):
    """Return the rounded mean of a list, or 0 for an empty list."""
    return round(sum(values) / len(values), 1) if values else 0.0


def progress_metrics():
    """Aggregate the StreamingQueryProgress reports per query."""
    stats = {}
    with open(PROGRESS_FILE) as file:
        for line in file:
            progress = json.loads(line)
            query = stats.setdefault(progress["name"], {
                "batches": 0, "rows": 0, "input_rps": [],
                "processed_rps": [], "dropped": 0, "first": None,
                "last": None})
            query["batches"] += 1
            query["rows"] += progress.get("numInputRows", 0)
            if progress.get("numInputRows"):
                query["input_rps"].append(progress["inputRowsPerSecond"])
                query["processed_rps"].append(
                    progress["processedRowsPerSecond"])
            for operator in progress.get("stateOperators", []):
                query["dropped"] += operator.get(
                    "numRowsDroppedByWatermark", 0)
            query["first"] = query["first"] or progress["timestamp"]
            query["last"] = progress["timestamp"]
    rows = []
    for name, query in sorted(stats.items()):
        rows.append([name, query["first"], query["last"], query["batches"],
                     query["rows"], mean(query["input_rps"]),
                     mean(query["processed_rps"]), query["dropped"]])
    return markdown_table(
        ["query", "first batch", "last batch", "batches", "input rows",
         "avg inputRowsPerSecond", "avg processedRowsPerSecond",
         "rows dropped by watermark"], rows)


def main():
    """Build every summary table, print it and save it."""
    spark = get_spark("imdb-streaming-report")
    matched = spark.read.parquet(str(OUTPUT_DIR / "matched_events"))
    windows = spark.read.parquet(str(OUTPUT_DIR / "stream_windows"))
    sections = []

    totals = windows.agg(F.sum("all_events"),
                         F.sum("enwiki_article_edits")).first()
    by_type = {row["match_type"]: row["count"]
               for row in matched.groupBy("match_type").count().collect()}
    n_matched = sum(by_type.values())
    sections.append("## Volumes\n\n" + markdown_table(
        ["metric", "value"],
        [["events received (closed windows)", totals[0]],
         ["English Wikipedia article edits (closed windows)", totals[1]],
         ["edits matched with IMDb (whole run)", n_matched],
         ["matched on a title", by_type.get("title", 0)],
         ["matched on a person", by_type.get("person", 0)],
         ["distinct IMDb entities",
          matched.select("imdb_id").distinct().count()]]))

    sections.append("## Throughput (query.recentProgress)\n\n"
                    + progress_metrics())

    delays = matched.agg(
        F.percentile_approx("arrival_delay_s", [0.5, 0.95, 0.99, 1.0])
    ).first()[0]
    late = matched.filter(F.col("arrival_delay_s") > 120).count()
    sections.append("## Arrival delay (Kafka time - edit time)\n\n"
                    + markdown_table(
                        ["median (s)", "p95 (s)", "p99 (s)", "max (s)",
                         "edits later than 2 min"], [delays + [late]]))

    top = (matched.groupBy("imdb_id", "match_type", "label")
           .agg(F.count(F.lit(1)).alias("edits"),
                F.countDistinct("user").alias("editors"))
           .orderBy(F.desc("edits"), "label").limit(TOP_N).collect())
    sections.append(f"## Top {TOP_N} most edited IMDb entities\n\n"
                    + markdown_table(
                        ["imdb_id", "type", "label", "edits", "editors"],
                        [list(row) for row in top]))

    examples = (matched.orderBy("event_time")
                .select("event_time", "title", "match_type", "label")
                .limit(TOP_N).collect())
    sections.append("## Sample of matched events\n\n" + markdown_table(
        ["event_time", "Wikipedia page", "type", "IMDb label"],
        [list(row) for row in examples]))
    spark.stop()

    summary = "\n\n".join(sections) + "\n"
    SUMMARY_FILE.write_text(summary, encoding="utf-8")
    print(summary)


if __name__ == "__main__":
    main()
