"""Most popular movie genres per year, 1980-2025 (questions 2.2 and 2.3).

2.2  Top three genres of each year, the popularity of a genre being the
     total number of IMDb votes of its feature-length movies that year.
2.3  Comparison of these top three genres with the other genres of the
     same year: share of votes and movies, votes per movie, rating, and
     how often each genre reaches the top three.

A movie with several genres counts once in each of its genres.
The script reads the cleaned tables of data/processed (question 1.4).
"""
import csv

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.paths import PROCESSED_DIR, RESULTS_DIR
from src.common.spark_session import get_spark

OUTPUT_DIR = RESULTS_DIR / "exploration"
MIN_RUNTIME = 40        # feature film: at least 40 minutes (Academy, AFI)
FIRST_YEAR, LAST_YEAR = 1980, 2025
TOP_N = 3


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


def genre_popularity(spark, movies):
    """Votes, movies and rating per (year, genre) for 1980-2025."""
    ratings = load(spark, "title.ratings")
    return (
        movies.where(F.col("startYear").between(FIRST_YEAR, LAST_YEAR))
        .join(ratings, "tconst")
        .groupBy(F.col("startYear").alias("year"), "genre")
        .agg(F.sum("numVotes").alias("total_votes"),
             F.count("*").alias("movies"),
             F.round(F.avg("averageRating"), 2).alias("avg_rating"))
        .withColumn("rank", F.row_number().over(
            Window.partitionBy("year").orderBy(F.desc("total_votes"))))
    )


def top_genres(popularity):
    """Question 2.2: the TOP_N most voted genres of each year."""
    return popularity.where(F.col("rank") <= TOP_N).orderBy("year", "rank")


def top_vs_rest(popularity):
    """Question 2.3: top genres compared with the other genres per year."""
    group = F.when(F.col("rank") <= TOP_N, "top").otherwise("rest")
    per_year = (
        popularity.withColumn("group", group)
        .groupBy("year", "group")
        .agg(F.sum("total_votes").alias("votes"),
             F.sum("movies").alias("movies"),
             F.count("*").alias("genres"),
             F.sum(F.col("avg_rating") * F.col("movies"))
             .alias("rating_x_movies"))
        .withColumn("avg_rating",
                    F.col("rating_x_movies") / F.col("movies"))
    )
    top = per_year.where(F.col("group") == "top").alias("t")
    rest = per_year.where(F.col("group") == "rest").alias("r")
    return (
        top.join(rest, "year")
        .select(
            "year",
            F.round(100 * F.col("t.votes")
                    / (F.col("t.votes") + F.col("r.votes")), 1)
            .alias("top3_vote_share_pct"),
            F.round(100 * F.col("t.movies")
                    / (F.col("t.movies") + F.col("r.movies")), 1)
            .alias("top3_movie_share_pct"),
            F.round(F.col("t.votes") / F.col("t.movies")).cast("long")
            .alias("top3_votes_per_movie"),
            F.round(F.col("r.votes") / F.col("r.movies")).cast("long")
            .alias("rest_votes_per_movie"),
            F.round("t.avg_rating", 2).alias("top3_avg_rating"),
            F.round("r.avg_rating", 2).alias("rest_avg_rating"),
            F.col("r.genres").alias("other_genres"),
        )
        .orderBy("year")
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
    """Run questions 2.2 and 2.3 and save their results."""
    spark = get_spark("imdb-genre-popularity")
    popularity = genre_popularity(spark, feature_movies(spark)).cache()
    top3 = top_genres(popularity)
    comparison = top_vs_rest(popularity)
    frequency = (
        top3.groupBy("genre")
        .agg(F.count("*").alias("years_in_top3"),
             F.sum(F.when(F.col("rank") == 1, 1).otherwise(0))
             .alias("years_ranked_first"))
        .orderBy(F.desc("years_in_top3"), "genre")
    )
    tables = [
        ("2.2 Top 3 genres per year", top3, "q2_2_top3_genres_per_year"),
        ("2.3 Top 3 genres vs other genres", comparison,
         "q2_3_top3_vs_rest"),
        ("2.3 Genres most often in the top 3", frequency,
         "q2_3_top3_frequency"),
    ]
    results = [(title, df.columns, save(df, name))
               for title, df, name in tables]
    spark.stop()
    for title, columns, rows in results:
        print_markdown(title, columns, rows)


if __name__ == "__main__":
    main()
