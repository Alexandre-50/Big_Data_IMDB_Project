"""TV series whose worst episode is in the last season (question 2.5).

A series is kept when:
- it has at least MIN_LONG_SEASONS seasons of MIN_EPISODES episodes or more;
- its worst rated episode belongs to its last season;
- its best rated episode belongs to one of its first two seasons.

Season 0 (specials, pilots) and episodes without a season number are
ignored. Every episode counts for the size of a season, but only rated
episodes can be the best or the worst one. Ties on the rating are broken
by the number of votes (the most voted episode wins).
The script reads the cleaned tables of data/processed (question 1.4).
"""
import csv

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.paths import PROCESSED_DIR, RESULTS_DIR
from src.common.spark_session import get_spark

OUTPUT_DIR = RESULTS_DIR / "exploration"
SERIES_TYPES = ["tvSeries", "tvMiniSeries"]
MIN_EPISODES = 10       # a "long" season has at least 10 episodes
MIN_LONG_SEASONS = 3
EARLY_SEASONS = 2       # the best episode must be in seasons 1 or 2


def load(spark, table):
    """Read one cleaned IMDb table."""
    return spark.read.parquet(str(PROCESSED_DIR / table.replace(".", "_")))


def series_episodes(spark):
    """Return every regular-season episode of a TV series."""
    series = (
        load(spark, "title.basics")
        .where(F.col("titleType").isin(SERIES_TYPES))
        .select(F.col("tconst").alias("parentTconst"),
                F.col("primaryTitle").alias("series_title"),
                F.col("startYear").alias("series_start"))
    )
    return (
        load(spark, "title.episode")
        .where(F.col("seasonNumber") >= 1)
        .join(series, "parentTconst")
    )


def eligible_series(episodes):
    """Series with enough long seasons, with their last season number."""
    season_sizes = (
        episodes.groupBy("parentTconst", "seasonNumber")
        .agg(F.count("*").alias("episodes"))
    )
    return (
        season_sizes.groupBy("parentTconst")
        .agg(F.sum(F.when(F.col("episodes") >= MIN_EPISODES, 1)
                   .otherwise(0)).alias("long_seasons"),
             F.max("seasonNumber").alias("last_season"),
             F.sum("episodes").alias("total_episodes"))
        .where(F.col("long_seasons") >= MIN_LONG_SEASONS)
    )


def extreme_episode(rated, order, prefix):
    """Keep, for each series, the episode ranked first by `order`."""
    window = Window.partitionBy("parentTconst").orderBy(*order)
    return (
        rated.withColumn("rank", F.row_number().over(window))
        .where(F.col("rank") == 1)
        .select("parentTconst",
                F.col("seasonNumber").alias(f"{prefix}_season"),
                F.col("episodeNumber").alias(f"{prefix}_episode"),
                F.col("averageRating").alias(f"{prefix}_rating"))
    )


def declining_series(spark, episodes):
    """Question 2.5: worst episode in the last season, best one early."""
    eligible = eligible_series(episodes)
    ratings = load(spark, "title.ratings")
    rated = episodes.join(eligible.select("parentTconst"),
                          "parentTconst").join(ratings, "tconst").cache()

    worst = extreme_episode(
        rated, [F.asc("averageRating"), F.desc("numVotes")], "worst")
    best = extreme_episode(
        rated, [F.desc("averageRating"), F.desc("numVotes")], "best")
    titles = episodes.select("parentTconst", "series_title",
                             "series_start").distinct()
    series_votes = ratings.select(
        F.col("tconst").alias("parentTconst"),
        F.col("averageRating").alias("series_rating"),
        F.col("numVotes").alias("series_votes"))

    return (
        eligible.join(worst, "parentTconst").join(best, "parentTconst")
        .where((F.col("worst_season") == F.col("last_season"))
               & (F.col("best_season") <= EARLY_SEASONS))
        .join(titles, "parentTconst")
        .join(series_votes, "parentTconst", "left")
        .select("parentTconst", "series_title", "series_start",
                "last_season", "long_seasons", "total_episodes",
                "best_season", "best_episode", "best_rating",
                "worst_season", "worst_episode", "worst_rating",
                "series_rating", "series_votes")
        .orderBy(F.desc_nulls_last("series_votes"))
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
    """Run question 2.5 and save its results."""
    spark = get_spark("imdb-series-decline")
    result = declining_series(spark, series_episodes(spark))
    rows = save(result, "q2_5_series_worst_episode_last_season")
    columns = result.columns
    spark.stop()
    print_markdown("2.5 Series with the worst episode in the last season "
                   "(top 20 by votes)", columns, rows[:20])


if __name__ == "__main__":
    main()
