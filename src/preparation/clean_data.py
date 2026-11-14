"""Clean the IMDb tables and write them to data/processed (question 1.4).

Handling strategy, decided from the data quality checks (question 1.3):
- Types: years, runtimes, ratings and votes become numbers, flags become
  booleans and comma-separated lists become arrays.
- Impossible values are set to null: runtimes <= 0, and birth and death
  years when the death year is before the birth year.
- Suspicious but plausible values are kept and flagged with a boolean
  column: titles released in the future (is_upcoming), people born more
  than 120 years ago without a death year (death_year_unknown), people
  with impossible life dates (has_invalid_life_dates).
- Season or episode number 0 is kept: IMDb uses it for specials and pilots.
- Rows of link tables pointing to a title or a person that does not exist
  are removed (title.principals, title.episode). Ids inside the crew and
  knownForTitles lists are kept: inner joins drop them naturally.
- Nulls that are not applicable (e.g. endYear of a movie) are kept.

Each cleaned table is written as soon as it is ready; tables already
written (folder with a _SUCCESS marker) are skipped when the script is
re-run, so it can resume after an interruption.
"""
import csv
from datetime import date

from pyspark.sql import functions as F

from src.common.paths import PARQUET_DIR, PROCESSED_DIR, RESULTS_DIR
from src.common.spark_session import get_spark

SUMMARY_FILE = RESULTS_DIR / "preparation" / "cleaning_summary.csv"
HEADER = ["table", "rows_before", "rows_after", "rows_removed"]
CURRENT_YEAR = date.today().year
MAX_AGE = 120


def load(spark, table):
    """Read one raw IMDb table from its Parquet folder."""
    return spark.read.parquet(str(PARQUET_DIR / table.replace(".", "_")))


def to_int(column):
    """Cast a string column to an integer (null when not a number)."""
    return F.col(column).try_cast("int")


def to_array(column):
    """Split a comma-separated string into an array (null stays null)."""
    return F.split(F.col(column), ",")


def existing_ids(spark, table, column):
    """Return the distinct ids of a parent table, used to drop orphans."""
    return load(spark, table).select(column).distinct()


def clean_name_basics(spark):
    """People: typed years, invalid life dates removed and flagged."""
    names = load(spark, "name.basics")
    birth, death = to_int("birthYear"), to_int("deathYear")
    invalid = death.isNotNull() & birth.isNotNull() & (death < birth)
    return (
        names
        .withColumn("has_invalid_life_dates",
                    F.coalesce(invalid, F.lit(False)))
        .withColumn("birthYear", F.when(~invalid | invalid.isNull(), birth))
        .withColumn("deathYear", F.when(~invalid | invalid.isNull(), death))
        .withColumn(
            "death_year_unknown",
            F.col("deathYear").isNull()
            & F.coalesce(F.col("birthYear") < CURRENT_YEAR - MAX_AGE,
                         F.lit(False)),
        )
        .withColumn("primaryProfession", to_array("primaryProfession"))
        .withColumn("knownForTitles", to_array("knownForTitles"))
    )


def clean_title_basics(spark):
    """Titles: typed columns, impossible runtimes removed, future flagged."""
    titles = load(spark, "title.basics")
    runtime = to_int("runtimeMinutes")
    return (
        titles
        .withColumn("isAdult", F.col("isAdult") == "1")
        .withColumn("startYear", to_int("startYear"))
        .withColumn("endYear", to_int("endYear"))
        .withColumn("runtimeMinutes", F.when(runtime > 0, runtime))
        .withColumn("genres", to_array("genres"))
        .withColumn(
            "is_upcoming",
            F.coalesce(F.col("startYear") > CURRENT_YEAR, F.lit(False)),
        )
    )


def clean_title_ratings(spark):
    """Ratings: typed rating and number of votes."""
    return (
        load(spark, "title.ratings")
        .withColumn("averageRating",
                    F.col("averageRating").try_cast("double"))
        .withColumn("numVotes", to_int("numVotes"))
    )


def clean_title_episode(spark):
    """Episodes: typed numbers, episodes of unknown series removed."""
    titles = existing_ids(spark, "title.basics", "tconst")
    parents = titles.withColumnRenamed("tconst", "parentTconst")
    return (
        load(spark, "title.episode")
        .join(titles, on="tconst", how="left_semi")
        .join(parents, on="parentTconst", how="left_semi")
        .withColumn("seasonNumber", to_int("seasonNumber"))
        .withColumn("episodeNumber", to_int("episodeNumber"))
    )


def clean_title_principals(spark):
    """Principals: rows with an unknown title or person removed."""
    titles = existing_ids(spark, "title.basics", "tconst")
    people = existing_ids(spark, "name.basics", "nconst")
    return (
        load(spark, "title.principals")
        .join(titles, on="tconst", how="left_semi")
        .join(people, on="nconst", how="left_semi")
        .withColumn("ordering", to_int("ordering"))
        .withColumn("characters",
                    F.from_json(F.col("characters"), "array<string>"))
    )


def clean_title_crew(spark):
    """Crew: director and writer lists turned into arrays."""
    return (
        load(spark, "title.crew")
        .withColumn("directors", to_array("directors"))
        .withColumn("writers", to_array("writers"))
    )


def clean_title_akas(spark):
    """Alternative titles: typed ordering and boolean original flag."""
    return (
        load(spark, "title.akas")
        .withColumn("ordering", to_int("ordering"))
        .withColumn("isOriginalTitle", F.col("isOriginalTitle") == "1")
    )


CLEANERS = {
    "name.basics": clean_name_basics,
    "title.basics": clean_title_basics,
    "title.ratings": clean_title_ratings,
    "title.episode": clean_title_episode,
    "title.principals": clean_title_principals,
    "title.crew": clean_title_crew,
    "title.akas": clean_title_akas,
}


def done_tables():
    """Return the summary rows already saved by a previous run."""
    if not SUMMARY_FILE.exists():
        return []
    with open(SUMMARY_FILE, newline="") as file:
        return list(csv.reader(file))[1:]


def main():
    """Clean every table, write it and save a before/after summary."""
    rows = done_tables()
    finished = {row[0] for row in rows}
    SUMMARY_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with open(SUMMARY_FILE, "w", newline="") as file:
            csv.writer(file).writerow(HEADER)

    spark = get_spark("imdb-cleaning")
    spark.conf.set("spark.sql.shuffle.partitions", "64")
    for table, cleaner in CLEANERS.items():
        output_dir = PROCESSED_DIR / table.replace(".", "_")
        if table in finished and (output_dir / "_SUCCESS").exists():
            print(f"{table}: already cleaned, skipped", flush=True)
            continue
        cleaner(spark).write.mode("overwrite").parquet(str(output_dir))
        before = load(spark, table).count()
        after = spark.read.parquet(str(output_dir)).count()
        row = [table, before, after, before - after]
        with open(SUMMARY_FILE, "a", newline="") as file:
            csv.writer(file).writerow(row)
        rows.append(row)
        print(f"{table}: {before} -> {after} rows", flush=True)
    spark.stop()

    print("\n## Cleaning summary\n")
    print("| " + " | ".join(HEADER) + " |")
    print("|" + "---|" * len(HEADER))
    for row in rows:
        print("| " + " | ".join(str(value) for value in row) + " |")


if __name__ == "__main__":
    main()
