"""Actors and actresses successful in several languages (question 2.6).

1. Successful movie: a movie (titleType "movie") rated at least
   MIN_RATING with at least MIN_VOTES votes.
2. Original language of a movie: IMDb has no such column, so it is
   inferred from title.akas. Only the alternative titles equal to the
   original title are used, compared without case, spaces or punctuation
   ("Shichi-nin no samurai" = "Shichinin no samurai"). Each of them votes
   for a language: its `language` column, or else its `region` translated
   with REGION_LANGUAGE (multilingual countries such as India are left
   out on purpose). Regions that often keep foreign original titles
   (KEEPER_REGIONS) only get a weak vote, so that the home country wins.
   Movies whose language cannot be inferred are reported and ignored.
3. For each actor or actress (category actor or actress in
   title.principals), the distinct original languages of his or her
   successful movies are counted: at least 2 languages for the first
   list, at least 3 for the second one.
The script reads the cleaned tables of data/processed (question 1.4).
"""
import csv

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.paths import PROCESSED_DIR, RESULTS_DIR
from src.common.spark_session import get_spark

OUTPUT_DIR = RESULTS_DIR / "exploration"
MIN_RATING = 7.0
MIN_VOTES = 10000       # success = well rated AND seen by many people
ACTOR_CATEGORIES = ["actor", "actress"]
KEEPER_REGIONS = ["NL"]  # often keeps the original title of foreign movies
KEEPER_WEIGHT = 0.1
REGION_LANGUAGE = {
    "US": "en", "GB": "en", "AU": "en", "CA": "en", "IE": "en", "NZ": "en",
    "FR": "fr", "BE": "fr", "DE": "de", "AT": "de", "IT": "it",
    "ES": "es", "MX": "es", "AR": "es", "CL": "es", "CO": "es",
    "PT": "pt", "BR": "pt", "JP": "ja", "KR": "ko", "CN": "zh",
    "TW": "zh", "HK": "zh", "RU": "ru", "SE": "sv", "DK": "da",
    "NO": "no", "FI": "fi", "NL": "nl", "PL": "pl", "TR": "tr",
    "IR": "fa", "GR": "el", "HU": "hu", "CZ": "cs", "IL": "he",
    "TH": "th", "EG": "ar", "RO": "ro", "UA": "uk",
}


def load(spark, table):
    """Read one cleaned IMDb table."""
    return spark.read.parquet(str(PROCESSED_DIR / table.replace(".", "_")))


def successful_movies(spark):
    """Return the successful movies with their original title."""
    ratings = load(spark, "title.ratings").where(
        (F.col("averageRating") >= MIN_RATING)
        & (F.col("numVotes") >= MIN_VOTES))
    return (
        load(spark, "title.basics")
        .where(F.col("titleType") == "movie")
        .select("tconst", "primaryTitle", "originalTitle", "startYear")
        .join(ratings, "tconst")
    )


def normalized(column):
    """Lower-case a title and remove spaces and punctuation."""
    return F.regexp_replace(F.lower(F.col(column)), r"[^\p{L}\p{N}]", "")


def movie_languages(spark, movies):
    """Infer the original language of each successful movie."""
    mapping = F.create_map(*[F.lit(value) for pair in
                             REGION_LANGUAGE.items() for value in pair])
    region_language = mapping[F.col("region")]
    weak = F.col("language").isNull() & F.col("region").isin(KEEPER_REGIONS)
    votes = (
        load(spark, "title.akas")
        .select(F.col("titleId").alias("tconst"), "title", "region",
                "language")
        .join(F.broadcast(movies.select("tconst", "originalTitle")),
              "tconst")
        .where(normalized("title") == normalized("originalTitle"))
        .withColumn("voted_language",
                    F.coalesce("language", region_language))
        .withColumn("source", F.when(F.col("language").isNotNull(),
                                     "language").otherwise("region"))
        .withColumn("weight", F.when(weak, KEEPER_WEIGHT).otherwise(1.0))
        .where(F.col("voted_language").isNotNull())
        .groupBy("tconst", "voted_language")
        .agg(F.sum("weight").alias("score"),
             F.max(F.col("source") == "language").alias("from_column"))
    )
    best = Window.partitionBy("tconst").orderBy(F.desc("score"),
                                                "voted_language")
    winner = (
        votes.withColumn("rank", F.row_number().over(best))
        .where(F.col("rank") == 1)
        .select("tconst", F.col("voted_language").alias("language"),
                F.when(F.col("from_column"), "language")
                .otherwise("region").alias("language_source"))
    )
    return (
        movies.join(winner, "tconst", "left")
        .select("tconst", "primaryTitle", "startYear", "averageRating",
                "numVotes", "language", "language_source")
    )


def multilingual_actors(spark, languages):
    """Question 2.6: actors with successful movies in several languages."""
    known = languages.where(F.col("language").isNotNull())
    actors = (
        load(spark, "title.principals")
        .where(F.col("category").isin(ACTOR_CATEGORIES))
        .select("tconst", "nconst")
        .join(F.broadcast(known.select("tconst", "language",
                                       "primaryTitle")), "tconst")
    )
    names = load(spark, "name.basics").select("nconst", "primaryName")
    return (
        actors.groupBy("nconst")
        .agg(F.countDistinct("language").alias("languages"),
             F.concat_ws(", ", F.array_sort(F.collect_set("language")))
             .alias("language_list"),
             F.countDistinct("tconst").alias("successful_movies"))
        .where(F.col("languages") >= 2)
        .join(names, "nconst", "left")
        .select("nconst", "primaryName", "languages", "language_list",
                "successful_movies")
        .orderBy(F.desc("languages"), F.desc("successful_movies"),
                 "primaryName")
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
    """Run question 2.6 and save its results."""
    spark = get_spark("imdb-actor-languages")
    spark.conf.set("spark.sql.shuffle.partitions", "64")
    languages = movie_languages(spark, successful_movies(spark)).cache()
    coverage = (
        languages.groupBy(F.coalesce("language_source", F.lit("unknown"))
                          .alias("language_source"))
        .agg(F.count("*").alias("movies")).orderBy(F.desc("movies"))
    )
    top_languages = (
        languages.groupBy("language").agg(F.count("*").alias("movies"))
        .orderBy(F.desc("movies")).limit(15)
    )
    save(languages, "q2_6_successful_movie_languages")
    coverage_rows = save(coverage, "q2_6_language_coverage")
    language_rows = save(top_languages, "q2_6_top_languages")

    actors = multilingual_actors(spark, languages).cache()
    two_rows = save(actors, "q2_6_actors_2plus_languages")
    three = actors.where(F.col("languages") >= 3)
    three_rows = save(three, "q2_6_actors_3plus_languages")
    columns = actors.columns
    spark.stop()

    print_markdown("2.6 How the original language was found",
                   ["language_source", "movies"], coverage_rows)
    print_markdown("2.6 Most frequent original languages",
                   ["language", "movies"], language_rows)
    print_markdown("2.6 Actors successful in 3+ languages (top 20)",
                   columns, three_rows[:20])
    print(f"\nActors successful in 2+ languages: {len(two_rows)}")


if __name__ == "__main__":
    main()
