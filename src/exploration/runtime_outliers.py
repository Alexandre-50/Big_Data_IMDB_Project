"""Feature-length movies with an atypical runtime per genre (question 2.1).

A feature-length movie is a movie (titleType "movie") of at least
MIN_RUNTIME minutes. A movie is notably different from its genre when its
runtime is outside the Tukey fences of the genre:
    below Q1 - IQR_FACTOR * IQR  or  above Q3 + IQR_FACTOR * IQR,
with Q1 and Q3 the first and third quartiles and IQR = Q3 - Q1.
Quartiles are robust to the extreme runtimes found in some genres, unlike
the mean and the standard deviation. The number of movies flagged by the
z-score rule (|z| > Z_THRESHOLD) is kept in the summary for comparison.
A movie with several genres is evaluated in each of its genres.
The script reads the cleaned tables of data/processed (question 1.4).
"""
import csv

from pyspark.sql import functions as F

from src.common.paths import PROCESSED_DIR, RESULTS_DIR
from src.common.spark_session import get_spark

OUTPUT_DIR = RESULTS_DIR / "exploration"
MIN_RUNTIME = 40        # feature film: at least 40 minutes (Academy, AFI)
IQR_FACTOR = 1.5        # Tukey fences: Q1 - 1.5 IQR and Q3 + 1.5 IQR
Z_THRESHOLD = 2.0       # z-score rule, kept only for comparison


def load(spark, table):
    """Read one cleaned IMDb table."""
    return spark.read.parquet(str(PROCESSED_DIR / table.replace(".", "_")))


def feature_movies(spark):
    """Return one row per (feature-length movie, genre)."""
    return (
        load(spark, "title.basics")
        .where((F.col("titleType") == "movie")
               & (F.col("runtimeMinutes") >= MIN_RUNTIME)
               & F.col("genres").isNotNull())
        .select("tconst", "primaryTitle", "startYear", "runtimeMinutes",
                F.explode("genres").alias("genre"))
    )


def atypical_runtimes(movies):
    """Question 2.1: genre statistics and movies with an atypical runtime."""
    runtime = F.col("runtimeMinutes")
    stats = (
        movies.groupBy("genre")
        .agg(F.count("*").alias("movies"),
             F.avg(runtime).alias("mean"),
             F.stddev(runtime).alias("std"),
             F.percentile_approx(runtime, [0.25, 0.5, 0.75], 10000)
             .alias("quartiles"))
        .withColumn("q1", F.col("quartiles")[0])
        .withColumn("median", F.col("quartiles")[1])
        .withColumn("q3", F.col("quartiles")[2])
        .withColumn("iqr", F.col("q3") - F.col("q1"))
        .withColumn("low_fence", F.col("q1") - IQR_FACTOR * F.col("iqr"))
        .withColumn("high_fence", F.col("q3") + IQR_FACTOR * F.col("iqr"))
        .drop("quartiles")
    )
    scored = (
        movies.join(F.broadcast(stats), "genre")
        .withColumn("z_score", (runtime - F.col("mean")) / F.col("std"))
        .withColumn("position",
                    F.when(runtime < F.col("low_fence"), "shorter")
                    .when(runtime > F.col("high_fence"), "longer"))
    ).cache()

    summary = (
        scored.groupBy("genre", "movies", "mean", "std", "q1", "median",
                       "q3", "low_fence", "high_fence")
        .agg(F.sum(F.when(F.col("position") == "shorter", 1).otherwise(0))
             .alias("much_shorter"),
             F.sum(F.when(F.col("position") == "longer", 1).otherwise(0))
             .alias("much_longer"),
             F.sum(F.when(F.abs("z_score") > Z_THRESHOLD, 1).otherwise(0))
             .alias("zscore_atypical"))
        .withColumn("atypical_pct",
                    F.round(100 * (F.col("much_shorter")
                                   + F.col("much_longer"))
                            / F.col("movies"), 2))
        .select("genre", "movies", F.round("mean", 1).alias("mean"),
                F.round("std", 1).alias("std"), "q1", "median", "q3",
                "low_fence", "high_fence", "much_shorter", "much_longer",
                "atypical_pct", "zscore_atypical")
        .orderBy(F.desc("movies"))
    )
    details = (
        scored.where(F.col("position").isNotNull())
        .select("genre", "tconst", "primaryTitle", "startYear",
                "runtimeMinutes", "position", "median", "low_fence",
                "high_fence")
        .orderBy("genre", "position", "runtimeMinutes")
    )
    return summary, details


def save(df, name):
    """Write a small result DataFrame as one CSV file and return its rows."""
    rows = df.collect()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    with open(OUTPUT_DIR / f"{name}.csv", "w", newline="",
              encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(df.columns)
        writer.writerows(rows)
    print(f"{name}: {len(rows)} rows", flush=True)
    return rows


def print_markdown(title, columns, rows):
    """Print rows as a Markdown table, ready to paste in REPORT.md."""
    print(f"\n## {title}\n")
    print("| " + " | ".join(columns) + " |")
    print("|" + "---|" * len(columns))
    for row in rows:
        print("| " + " | ".join(str(value) for value in row) + " |")


def main():
    """Run question 2.1 and save its results."""
    spark = get_spark("imdb-runtime-outliers")
    movies = feature_movies(spark)
    summary, details = atypical_runtimes(movies)
    summary_rows = save(summary, "q2_1_runtime_by_genre")
    save(details, "q2_1_atypical_runtime_movies")
    columns = summary.columns
    spark.stop()
    print_markdown("2.1 Runtime statistics per genre", columns, summary_rows)


if __name__ == "__main__":
    main()
