"""Actors and actresses whose popularity peaked early (question 2.7).

Movies used: titleType "movie" with a release year and at least MIN_VOTES
votes, so that every rating is meaningful.
Eligible actor or actress: a career of at least MIN_CAREER_YEARS years
(from the first to the last movie) and at least MIN_MOVIES movies.
The career is split into three periods of equal length. The popularity
peaked early when:
- the best rated movie is in the first third of the career;
- the average rating of the first third is higher than the average
  rating of the last third.
The second rule prevents a single exceptional movie from deciding alone.
The script reads the cleaned tables of data/processed (question 1.4).
"""
import csv

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.paths import PROCESSED_DIR, RESULTS_DIR
from src.common.spark_session import get_spark

OUTPUT_DIR = RESULTS_DIR / "exploration"
ACTOR_CATEGORIES = ["actor", "actress"]
MIN_VOTES = 100
MIN_CAREER_YEARS = 20
MIN_MOVIES = 15


def load(spark, table):
    """Read one cleaned IMDb table."""
    return spark.read.parquet(str(PROCESSED_DIR / table.replace(".", "_")))


def actor_movies(spark):
    """Return one row per (actor, rated movie) with its year and rating."""
    movies = (
        load(spark, "title.basics")
        .where((F.col("titleType") == "movie")
               & F.col("startYear").isNotNull())
        .select("tconst", "primaryTitle", "startYear")
        .join(load(spark, "title.ratings")
              .where(F.col("numVotes") >= MIN_VOTES), "tconst")
    )
    return (
        load(spark, "title.principals")
        .where(F.col("category").isin(ACTOR_CATEGORIES))
        .select("tconst", "nconst")
        .join(F.broadcast(movies), "tconst")
        .dropDuplicates(["nconst", "tconst"])
    )


def career_periods(roles):
    """Keep eligible careers and tag each movie with its career third."""
    by_actor = Window.partitionBy("nconst")
    span = F.col("last_year") - F.col("first_year")
    position = (F.col("startYear") - F.col("first_year")) / span
    return (
        roles
        .withColumn("first_year", F.min("startYear").over(by_actor))
        .withColumn("last_year", F.max("startYear").over(by_actor))
        .withColumn("movies", F.count("*").over(by_actor))
        .where((span >= MIN_CAREER_YEARS)
               & (F.col("movies") >= MIN_MOVIES))
        .withColumn("period",
                    F.when(position < 1 / 3, "early")
                    .when(position < 2 / 3, "middle")
                    .otherwise("late"))
    )


def early_peaks(spark, periods):
    """Question 2.7: careers whose best movies are in the first third."""
    best_first = Window.partitionBy("nconst").orderBy(
        F.desc("averageRating"), F.desc("numVotes"))
    best = (
        periods.withColumn("rank", F.row_number().over(best_first))
        .where(F.col("rank") == 1)
        .select("nconst", "first_year", "last_year", "movies",
                F.col("primaryTitle").alias("best_title"),
                F.col("startYear").alias("best_year"),
                F.col("averageRating").alias("best_rating"),
                F.col("period").alias("best_period"))
    )
    averages = (
        periods.groupBy("nconst")
        .agg(F.avg(F.when(F.col("period") == "early",
                          F.col("averageRating"))).alias("early_avg"),
             F.avg(F.when(F.col("period") == "late",
                          F.col("averageRating"))).alias("late_avg"))
    )
    careers = best.join(averages, "nconst").cache()
    early = (
        careers.where((F.col("best_period") == "early")
                      & (F.col("early_avg") > F.col("late_avg")))
        .join(load(spark, "name.basics").select("nconst", "primaryName"),
              "nconst", "left")
        .select("nconst", "primaryName", "first_year", "last_year",
                (F.col("last_year") - F.col("first_year"))
                .alias("career_years"),
                "movies", "best_title", "best_year", "best_rating",
                F.round("early_avg", 2).alias("early_avg"),
                F.round("late_avg", 2).alias("late_avg"),
                F.round(F.col("early_avg") - F.col("late_avg"), 2)
                .alias("rating_drop"))
        .orderBy(F.desc("rating_drop"), "primaryName")
    )
    summary = careers.agg(
        F.count("*").alias("eligible_actors"),
        F.sum(F.when(F.col("best_period") == "early", 1).otherwise(0))
        .alias("best_movie_in_first_third"),
        F.sum(F.when((F.col("best_period") == "early")
                     & (F.col("early_avg") > F.col("late_avg")), 1)
              .otherwise(0)).alias("early_peak_actors"),
    )
    return early, summary


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
    """Run question 2.7 and save its results."""
    spark = get_spark("imdb-actor-career-peaks")
    spark.conf.set("spark.sql.shuffle.partitions", "64")
    early, summary = early_peaks(spark,
                                 career_periods(actor_movies(spark)))
    summary_rows = save(summary, "q2_7_summary")
    rows = save(early, "q2_7_early_peak_actors")
    columns = (summary.columns, early.columns)
    spark.stop()
    print_markdown("2.7 Summary", columns[0], summary_rows)
    print_markdown("2.7 Early-peak actors (top 20 by rating drop)",
                   columns[1], rows[:20])


if __name__ == "__main__":
    main()
