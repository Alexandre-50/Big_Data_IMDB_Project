"""Select the films and people to enrich with Wikipedia (step 1 of 3).

For every genre and every year of the period, the TOP_N most popular
feature-length films are kept (popularity = number of IMDb votes, the
same definition as in the exploration part). For each selected film, the
first principals by billing order are kept as candidates; the build step
keeps the first three that have a Wikipedia biography.

The period, the number of films and the number of candidates are
command-line options, so the same code runs on any subset of the dataset.

Outputs (Parquet, in data/enrichment):
- top_films: one row per (genre, year, rank) slot;
- film_principals: one row per (film, principal).
"""
import argparse

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.paths import DATA_DIR, PARQUET_DIR
from src.common.spark_session import get_spark

ENRICHMENT_DIR = DATA_DIR / "enrichment"
MIN_RUNTIME = 40  # feature film: at least 40 minutes (Academy, AFI)


def parse_args():
    """Read the command-line options."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--first-year", type=int, default=1980)
    parser.add_argument("--last-year", type=int, default=2025)
    parser.add_argument("--top-n", type=int, default=10,
                        help="Films kept per genre and year.")
    parser.add_argument("--candidates", type=int, default=6,
                        help="Principals collected per film, so that the "
                             "3 kept can skip people without a page.")
    return parser.parse_args()


def load(spark, table):
    """Read one IMDb table from its Parquet conversion."""
    return spark.read.parquet(str(PARQUET_DIR / table.replace(".", "_")))


def top_films(spark, first_year, last_year, top_n):
    """Return the top_n most voted feature films per genre and year."""
    ratings = load(spark, "title.ratings").select(
        "tconst",
        F.col("averageRating").try_cast("double").alias("average_rating"),
        F.col("numVotes").try_cast("int").alias("num_votes"),
    )
    movies = (
        load(spark, "title.basics")
        .select(
            "tconst", "primaryTitle", "originalTitle",
            F.col("startYear").try_cast("int").alias("year"),
            F.col("runtimeMinutes").try_cast("int").alias("runtime"),
            F.col("titleType"), F.col("genres"),
        )
        .where((F.col("titleType") == "movie")
               & (F.col("runtime") >= MIN_RUNTIME)
               & F.col("year").between(first_year, last_year)
               & F.col("genres").isNotNull())
        .withColumn("genre", F.explode(F.split("genres", ",")))
        .join(ratings, "tconst")
    )
    by_votes = Window.partitionBy("genre", "year").orderBy(
        F.desc("num_votes"), F.desc("average_rating"), "tconst")
    return (
        movies.withColumn("rank_in_genre", F.row_number().over(by_votes))
        .where(F.col("rank_in_genre") <= top_n)
        .select("genre", "year", "rank_in_genre", "tconst",
                "primaryTitle", "originalTitle", "runtime",
                "average_rating", "num_votes")
    )


def film_principals(spark, films, candidates):
    """Return the first principals (billing order) of each selected film.

    A person credited twice on the same film (e.g. actress and producer)
    is kept once, with the first credit.
    """
    names = load(spark, "name.basics").select(
        "nconst", "primaryName", "primaryProfession")
    first_credit = Window.partitionBy("tconst", "nconst").orderBy("ordering")
    by_billing = Window.partitionBy("tconst").orderBy("ordering")
    return (
        load(spark, "title.principals")
        .withColumn("ordering", F.col("ordering").try_cast("int"))
        .join(films.select("tconst").distinct(), "tconst", "left_semi")
        .withColumn("credit", F.row_number().over(first_credit))
        .where(F.col("credit") == 1)
        # Keep only people that exist in name.basics.
        .join(names, "nconst")
        .withColumn("principal_rank", F.row_number().over(by_billing))
        .where(F.col("principal_rank") <= candidates)
        .select("tconst", "nconst", "principal_rank", "category",
                "primaryName", "primaryProfession")
    )


def main():
    """Select the targets and write them to data/enrichment."""
    args = parse_args()
    spark = get_spark("imdb-enrichment-select")
    films = top_films(spark, args.first_year, args.last_year, args.top_n)
    films.write.mode("overwrite").parquet(str(ENRICHMENT_DIR / "top_films"))
    films = spark.read.parquet(str(ENRICHMENT_DIR / "top_films"))

    principals = film_principals(spark, films, args.candidates)
    principals.write.mode("overwrite").parquet(
        str(ENRICHMENT_DIR / "film_principals"))
    principals = spark.read.parquet(str(ENRICHMENT_DIR / "film_principals"))

    print(f"Slots (genre, year, rank): {films.count()}", flush=True)
    print(f"Distinct films: {films.select('tconst').distinct().count()}",
          flush=True)
    print(f"Principal rows: {principals.count()}, distinct people: "
          f"{principals.select('nconst').distinct().count()}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
