"""Open question: women in lead roles of popular films, 1980-2025.

Question: has the share of films led by an actress grown since 1980, in
which genres, and are actress-led films rated and voted differently from
actor-led films?

Definitions:
- popular feature film: titleType "movie", runtime >= 40 minutes,
  released between FIRST_YEAR and LAST_YEAR, at least MIN_VOTES votes;
- lead: the performer (category "actor" or "actress") with the smallest
  billing order in title.principals; the category gives the gender as
  credited by IMDb;
- top-billed cast: the first TOP_CAST performers by billing order.

To compare ratings without mixing genres and periods (e.g. horror is
both rated lower and more often actress-led), the rating gap is computed
inside each (genre, decade) group and then averaged, weighted by the
number of films of the group.

Outputs (results/open_question): by_year.csv, by_genre.csv,
rating_gap.csv and summary.md.
"""
import csv

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.paths import PARQUET_DIR, RESULTS_DIR
from src.common.spark_session import get_spark

OUTPUT_DIR = RESULTS_DIR / "open_question"
FIRST_YEAR, LAST_YEAR = 1980, 2025
MIN_RUNTIME = 40
MIN_VOTES = 1000
TOP_CAST = 3
MIN_GROUP_FILMS = 30  # smallest (genre, decade) group used for the gap


def load(spark, table):
    """Read one IMDb table from its Parquet conversion."""
    return spark.read.parquet(str(PARQUET_DIR / table.replace(".", "_")))


def popular_films(spark):
    """Return the popular feature films with year, genres and rating."""
    ratings = load(spark, "title.ratings").select(
        "tconst",
        F.col("averageRating").try_cast("double").alias("rating"),
        F.col("numVotes").try_cast("int").alias("votes"))
    return (
        load(spark, "title.basics")
        .where(F.col("titleType") == "movie")
        .select("tconst", "primaryTitle",
                F.col("startYear").try_cast("int").alias("year"),
                F.col("runtimeMinutes").try_cast("int").alias("runtime"),
                F.split("genres", ",").alias("genres"))
        .where((F.col("runtime") >= MIN_RUNTIME)
               & F.col("year").between(FIRST_YEAR, LAST_YEAR))
        .join(ratings, "tconst")
        .where(F.col("votes") >= MIN_VOTES)
    )


def cast_gender(spark, films):
    """Return, per film, the lead's gender and the top-billed female share."""
    by_billing = Window.partitionBy("tconst").orderBy("ordering")
    performers = (
        load(spark, "title.principals")
        .where(F.col("category").isin("actor", "actress"))
        .join(films.select("tconst"), "tconst", "left_semi")
        .withColumn("ordering", F.col("ordering").try_cast("int"))
        .withColumn("cast_rank", F.row_number().over(by_billing))
        .where(F.col("cast_rank") <= TOP_CAST)
    )
    is_female = (F.col("category") == "actress").cast("int")
    return performers.groupBy("tconst").agg(
        F.max(F.when(F.col("cast_rank") == 1, is_female))
        .alias("female_lead"),
        F.avg(is_female).alias("female_top_cast_share"),
    )


def by_year(films):
    """Share of actress-led films and their reception, per year."""
    return (
        films.groupBy("year")
        .agg(F.count("*").alias("films"),
             F.round(100 * F.avg("female_lead"), 1)
             .alias("female_lead_pct"),
             F.round(100 * F.avg("female_top_cast_share"), 1)
             .alias("female_top_cast_pct"),
             F.round(F.avg(F.when(F.col("female_lead") == 1, F.col("rating"))),
                     2).alias("rating_female_led"),
             F.round(F.avg(F.when(F.col("female_lead") == 0, F.col("rating"))),
                     2).alias("rating_male_led"),
             F.percentile_approx(F.when(F.col("female_lead") == 1,
                                        F.col("votes")), 0.5)
             .alias("median_votes_female_led"),
             F.percentile_approx(F.when(F.col("female_lead") == 0,
                                        F.col("votes")), 0.5)
             .alias("median_votes_male_led"))
        .orderBy("year")
    )


def by_genre(films):
    """Share of actress-led films per genre, 1980s versus 2020s."""
    exploded = films.withColumn("genre", F.explode("genres"))
    period = (F.when(F.col("year") < 1990, "1980s")
              .when(F.col("year") >= 2020, "2020s"))
    return (
        exploded.withColumn("period", period)
        .groupBy("genre")
        .agg(F.count("*").alias("films"),
             F.round(100 * F.avg("female_lead"), 1)
             .alias("female_lead_pct"),
             F.round(100 * F.avg(F.when(F.col("period") == "1980s",
                                        F.col("female_lead"))), 1)
             .alias("pct_1980s"),
             F.round(100 * F.avg(F.when(F.col("period") == "2020s",
                                        F.col("female_lead"))), 1)
             .alias("pct_2020s"))
        .withColumn("change_points",
                    F.round(F.col("pct_2020s") - F.col("pct_1980s"), 1))
        .where(F.col("films") >= 500)
        .orderBy(F.desc("female_lead_pct"))
    )


def rating_gap(films):
    """Rating and votes gap inside (genre, decade) groups, then weighted."""
    groups = (
        films.withColumn("genre", F.explode("genres"))
        .withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int"))
        .groupBy("genre", "decade")
        .agg(F.count("*").alias("films"),
             F.sum("female_lead").alias("female_led"),
             F.avg(F.when(F.col("female_lead") == 1, F.col("rating")))
             .alias("rating_f"),
             F.avg(F.when(F.col("female_lead") == 0, F.col("rating")))
             .alias("rating_m"),
             F.avg(F.when(F.col("female_lead") == 1, F.log10("votes")))
             .alias("log_votes_f"),
             F.avg(F.when(F.col("female_lead") == 0, F.log10("votes")))
             .alias("log_votes_m"))
        .where((F.col("female_led") >= MIN_GROUP_FILMS)
               & (F.col("films") - F.col("female_led") >= MIN_GROUP_FILMS))
        .withColumn("rating_gap", F.col("rating_f") - F.col("rating_m"))
        # Ratio of geometric mean votes, actress-led / actor-led.
        .withColumn("votes_ratio",
                    F.pow(F.lit(10.0),
                          F.col("log_votes_f") - F.col("log_votes_m")))
    )
    weighted = (
        groups.groupBy("decade")
        .agg(F.sum("films").alias("films"),
             F.count("*").alias("genre_groups"),
             F.round(F.sum(F.col("rating_gap") * F.col("films"))
                     / F.sum("films"), 3).alias("weighted_rating_gap"),
             F.round(F.sum(F.col("votes_ratio") * F.col("films"))
                     / F.sum("films"), 3).alias("weighted_votes_ratio"))
        .orderBy("decade")
    )
    overall = groups.agg(
        F.lit("all").alias("decade"), F.sum("films").alias("films"),
        F.count("*").alias("genre_groups"),
        F.round(F.sum(F.col("rating_gap") * F.col("films"))
                / F.sum("films"), 3).alias("weighted_rating_gap"),
        F.round(F.sum(F.col("votes_ratio") * F.col("films"))
                / F.sum("films"), 3).alias("weighted_votes_ratio"))
    return weighted.withColumn("decade", F.col("decade").cast("string")) \
        .unionByName(overall)


def save(rows, columns, name):
    """Write collected rows to a CSV file and return them."""
    with open(OUTPUT_DIR / name, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        writer.writerows([list(row) for row in rows])
    return rows


def markdown_table(columns, rows):
    """Return rows as a Markdown table."""
    lines = ["| " + " | ".join(columns) + " |", "|" + "---|" * len(columns)]
    lines += ["| " + " | ".join(str(value) for value in row) + " |"
              for row in rows]
    return "\n".join(lines)


def main():
    """Run the analysis and save the tables."""
    spark = get_spark("imdb-open-question")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    films = popular_films(spark)
    films = films.join(cast_gender(spark, films), "tconst") \
        .where(F.col("female_lead").isNotNull()).cache()

    sections = [f"Popular feature films with a credited lead: "
                f"{films.count()}"]
    for title, df, name in [
            ("Per year", by_year(films), "by_year.csv"),
            ("Per genre (genres with at least 500 films)", by_genre(films),
             "by_genre.csv"),
            ("Rating gap and votes ratio, actress-led vs actor-led, "
             "inside (genre, decade) groups", rating_gap(films),
             "rating_gap.csv")]:
        rows = save(df.collect(), df.columns, name)
        sections.append(f"## {title}\n\n" + markdown_table(df.columns, rows))
    spark.stop()

    summary = "\n\n".join(sections) + "\n"
    (OUTPUT_DIR / "summary.md").write_text(summary, encoding="utf-8")
    print(summary)


if __name__ == "__main__":
    main()
