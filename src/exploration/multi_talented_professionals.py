"""Multi-talented film professionals (question 2.8).

Roles: actor (actor or actress), director and writer, taken from
title.principals (category) and title.crew (directors and writers lists).
Movies used: titleType "movie" with at least MIN_VOTES votes.

A person is multi-talented when he or she held at least two of the three
roles in his or her career, each one on at least MIN_MOVIES_PER_ROLE
movies.

Performance: for every multi-talented person who has both kinds of
movies, the average rating of the movies where he or she held several
roles is compared with the average rating of his or her single-role
movies (paired comparison: each person is compared with himself or
herself). Results are also given per combination of roles.
The script reads the cleaned tables of data/processed (question 1.4).
"""
import csv

from pyspark.sql import functions as F

from src.common.paths import PROCESSED_DIR, RESULTS_DIR
from src.common.spark_session import get_spark

OUTPUT_DIR = RESULTS_DIR / "exploration"
MIN_VOTES = 100
MIN_MOVIES_PER_ROLE = 2
CATEGORY_ROLE = {"actor": "actor", "actress": "actor",
                 "director": "director", "writer": "writer"}


def load(spark, table):
    """Read one cleaned IMDb table."""
    return spark.read.parquet(str(PROCESSED_DIR / table.replace(".", "_")))


def rated_movies(spark):
    """Movies with enough votes, with their rating."""
    return (
        load(spark, "title.basics").where(F.col("titleType") == "movie")
        .select("tconst", "primaryTitle", "startYear")
        .join(load(spark, "title.ratings")
              .where(F.col("numVotes") >= MIN_VOTES), "tconst")
    )


def person_roles(spark, movies):
    """One row per (person, movie) with the set of roles held."""
    mapping = F.create_map(*[F.lit(value) for pair in
                             CATEGORY_ROLE.items() for value in pair])
    ids = F.broadcast(movies.select("tconst"))
    principals = (
        load(spark, "title.principals")
        .where(F.col("category").isin(list(CATEGORY_ROLE)))
        .join(ids, "tconst")
        .select("tconst", "nconst",
                mapping[F.col("category")].alias("role"))
    )
    crew = load(spark, "title.crew").join(ids, "tconst")
    directors = crew.select("tconst", F.explode("directors").alias("nconst"),
                            F.lit("director").alias("role"))
    writers = crew.select("tconst", F.explode("writers").alias("nconst"),
                          F.lit("writer").alias("role"))
    return (
        principals.unionByName(directors).unionByName(writers)
        .groupBy("nconst", "tconst")
        .agg(F.array_sort(F.collect_set("role")).alias("roles"))
        .join(F.broadcast(movies), "tconst")
    )


def multi_talented(spark, roles):
    """Question 2.8: multi-talented people and their two kinds of movies."""
    role_counts = (
        roles.select("nconst", F.explode("roles").alias("role"))
        .groupBy("nconst", "role").agg(F.count("*").alias("movies"))
        .where(F.col("movies") >= MIN_MOVIES_PER_ROLE)
        .groupBy("nconst")
        .agg(F.array_sort(F.collect_list("role")).alias("career_roles"))
        .where(F.size("career_roles") >= 2)
    )
    multi = F.size("roles") >= 2
    tagged = (
        roles.join(role_counts, "nconst")
        .withColumn("kind", F.when(multi, "multi").otherwise("single"))
        .withColumn("combination", F.concat_ws("+", "roles"))
    ).cache()
    by_person = (
        tagged.groupBy("nconst", "career_roles")
        .agg(F.count("*").alias("movies"),
             F.sum(F.when(multi, 1).otherwise(0)).alias("multi_movies"),
             F.avg(F.when(multi, F.col("averageRating"))).alias("multi_avg"),
             F.avg(F.when(~multi, F.col("averageRating")))
             .alias("single_avg"))
        .withColumn("difference", F.col("multi_avg") - F.col("single_avg"))
    )
    names = load(spark, "name.basics").select("nconst", "primaryName")
    people = (
        by_person.join(names, "nconst", "left")
        .select("nconst", "primaryName",
                F.concat_ws("+", "career_roles").alias("career_roles"),
                "movies", "multi_movies",
                F.round("multi_avg", 2).alias("multi_avg"),
                F.round("single_avg", 2).alias("single_avg"),
                F.round("difference", 2).alias("difference"))
        .orderBy(F.desc("multi_movies"), F.desc("movies"))
    )
    paired = by_person.where(F.col("difference").isNotNull())
    summary = (
        by_person.agg(F.count("*").alias("multi_talented_people"))
        .crossJoin(paired.agg(
            F.count("*").alias("people_with_both_kinds"),
            F.round(F.avg("single_avg"), 3).alias("avg_single_rating"),
            F.round(F.avg("multi_avg"), 3).alias("avg_multi_rating"),
            F.round(F.avg("difference"), 3).alias("avg_difference"),
            F.round(F.percentile_approx("difference", 0.5), 3)
            .alias("median_difference"),
            F.round(100 * F.avg(F.when(F.col("difference") > 0, 1)
                                .otherwise(0)), 1)
            .alias("pct_better_in_multi")))
    )
    combinations = (
        tagged.groupBy("combination")
        .agg(F.count("*").alias("person_movies"),
             F.countDistinct("nconst").alias("people"),
             F.round(F.avg("averageRating"), 2).alias("avg_rating"),
             F.round(F.avg("numVotes")).cast("long").alias("avg_votes"))
        .orderBy(F.desc("person_movies"))
    )
    return people, summary, combinations


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
    """Run question 2.8 and save its results."""
    spark = get_spark("imdb-multi-talented")
    spark.conf.set("spark.sql.shuffle.partitions", "64")
    people, summary, combinations = multi_talented(
        spark, person_roles(spark, rated_movies(spark)))
    tables = [
        ("2.8 Summary (paired comparison)", summary, "q2_8_summary", None),
        ("2.8 Ratings per combination of roles", combinations,
         "q2_8_by_combination", None),
        ("2.8 Multi-talented people (top 20 by multi-role movies)", people,
         "q2_8_multi_talented_people", 20),
    ]
    results = [(title, df.columns, save(df, name), limit)
               for title, df, name, limit in tables]
    spark.stop()
    for title, columns, rows, limit in results:
        print_markdown(title, columns, rows[:limit] if limit else rows)


if __name__ == "__main__":
    main()
