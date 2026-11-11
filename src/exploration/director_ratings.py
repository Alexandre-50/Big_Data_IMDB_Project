"""Directors with exactly one hit and several low-rated movies (question 2.4).

A director is kept when, among the movies he or she directed:
- exactly one movie is rated HIT_RATING or more (the "hit");
- at least MIN_OTHER_MOVIES other movies exist;
- the average rating of these other movies is OTHER_MAX_AVG or less.

Only movies (titleType "movie") with at least MIN_VOTES votes are used,
so that a rating given by a handful of voters does not count as a hit.
The script reads the cleaned tables of data/processed (question 1.4).
"""
import csv

from pyspark.sql import functions as F

from src.common.paths import PROCESSED_DIR, RESULTS_DIR
from src.common.spark_session import get_spark

OUTPUT_DIR = RESULTS_DIR / "exploration"
HIT_RATING = 8.0
OTHER_MAX_AVG = 6.0
MIN_OTHER_MOVIES = 2
MIN_VOTES = 100         # a rating needs enough voters to be meaningful


def load(spark, table):
    """Read one cleaned IMDb table."""
    return spark.read.parquet(str(PROCESSED_DIR / table.replace(".", "_")))


def directed_movies(spark):
    """Return one row per (director, rated movie)."""
    movies = (
        load(spark, "title.basics")
        .where(F.col("titleType") == "movie")
        .select("tconst", "primaryTitle", "startYear")
    )
    ratings = (
        load(spark, "title.ratings")
        .where(F.col("numVotes") >= MIN_VOTES)
    )
    directors = (
        load(spark, "title.crew")
        .select("tconst", F.explode("directors").alias("nconst"))
    )
    return directors.join(movies.join(ratings, "tconst"), "tconst")


def one_hit_directors(spark, directed):
    """Question 2.4: directors with one hit and low-rated other movies."""
    is_hit = F.col("averageRating") >= HIT_RATING
    stats = (
        directed.groupBy("nconst")
        .agg(F.sum(F.when(is_hit, 1).otherwise(0)).alias("hits"),
             F.sum(F.when(~is_hit, 1).otherwise(0)).alias("other_movies"),
             F.avg(F.when(~is_hit, F.col("averageRating")))
             .alias("other_avg_rating"))
        .where((F.col("hits") == 1)
               & (F.col("other_movies") >= MIN_OTHER_MOVIES)
               & (F.col("other_avg_rating") <= OTHER_MAX_AVG))
    )
    hits = directed.where(is_hit).select(
        "nconst",
        F.col("primaryTitle").alias("hit_title"),
        F.col("startYear").alias("hit_year"),
        F.col("averageRating").alias("hit_rating"),
        F.col("numVotes").alias("hit_votes"),
    )
    names = load(spark, "name.basics").select("nconst", "primaryName")
    return (
        stats.join(hits, "nconst").join(names, "nconst", "left")
        .select("nconst", "primaryName", "hit_title", "hit_year",
                "hit_rating", "hit_votes", "other_movies",
                F.round("other_avg_rating", 2).alias("other_avg_rating"))
        .orderBy(F.desc("hit_votes"))
    )


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
    """Run question 2.4 and save its results."""
    spark = get_spark("imdb-one-hit-directors")
    result = one_hit_directors(spark, directed_movies(spark))
    rows = save(result, "q2_4_one_hit_directors")
    columns = result.columns
    spark.stop()
    print_markdown("2.4 One-hit directors (top 20 by votes of the hit)",
                   columns, rows[:20])


if __name__ == "__main__":
    main()
