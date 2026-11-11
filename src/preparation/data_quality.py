"""Detect data quality issues in the IMDb dataset (question 1.3).

Each check returns the rows of a table that break a rule. For every
check, the script reports the number and percentage of affected rows,
plus a few example identifiers.

Results are appended to the CSV file as soon as each check finishes, and
checks already present in the file are skipped. The script can therefore
be re-run after an interruption without redoing the finished checks.
"""
import csv
from datetime import date

from pyspark.sql import functions as F

from src.common.paths import PARQUET_DIR, RESULTS_DIR
from src.common.spark_session import get_spark

OUTPUT_FILE = RESULTS_DIR / "preparation" / "data_quality.csv"
HEADER = ["check", "table", "rows_checked", "issue_count", "issue_pct",
          "examples"]
CURRENT_YEAR = date.today().year
SERIES_TYPES = ["tvSeries", "tvMiniSeries"]


def load(spark, table):
    """Read one IMDb table from its Parquet folder."""
    return spark.read.parquet(str(PARQUET_DIR / table.replace(".", "_")))


def as_int(column):
    """Cast a string column to an integer (null when not a number)."""
    return F.col(column).try_cast("int")


def is_not_numeric(column):
    """True when a value is present but cannot be read as a number."""
    value = F.col(column)
    return value.isNotNull() & value.try_cast("double").isNull()


def orphans(child, key, parent, parent_key):
    """Return the rows of child whose key has no match in parent."""
    parent_keys = parent.select(F.col(parent_key).alias(key)).distinct()
    return child.where(F.col(key).isNotNull()).join(
        parent_keys, on=key, how="left_anti"
    )


def exploded(df, id_column, list_column, alias):
    """Split a comma-separated id list into one row per id."""
    return df.select(
        id_column,
        F.explode(F.split(F.col(list_column), ",")).alias(alias),
    )


def bad_list_items(df, id_column, list_column, pattern):
    """Return list items that are empty or do not match the expected format."""
    items = exploded(df, id_column, list_column, "item")
    return items.where(~F.trim(F.col("item")).rlike(pattern))


def duplicates(df, keys):
    """Return the key combinations that appear more than once."""
    return df.groupBy(*keys).count().where(F.col("count") > 1)


def build_checks(spark):
    """Return the list of (check, table, checked rows, issue rows)."""
    names = load(spark, "name.basics")
    titles = load(spark, "title.basics")
    ratings = load(spark, "title.ratings")
    episodes = load(spark, "title.episode")
    principals = load(spark, "title.principals")
    crew = load(spark, "title.crew")
    akas = load(spark, "title.akas")

    directors = exploded(crew, "tconst", "directors", "nconst")
    writers = exploded(crew, "tconst", "writers", "nconst")
    known_for = exploded(names, "nconst", "knownForTitles", "tconst")

    return [
        # Logical inconsistencies
        ("Death year before birth year", "name.basics", names,
         names.where(as_int("deathYear") < as_int("birthYear"))),
        ("Alive person older than 120 years", "name.basics", names,
         names.where(F.col("deathYear").isNull()
                     & (as_int("birthYear") < CURRENT_YEAR - 120))),
        ("End year before start year", "title.basics", titles,
         titles.where(as_int("endYear") < as_int("startYear"))),
        ("End year on a non-series title", "title.basics", titles,
         titles.where(F.col("endYear").isNotNull()
                      & ~F.col("titleType").isin(SERIES_TYPES))),
        ("Start year in the future", "title.basics", titles,
         titles.where(as_int("startYear") > CURRENT_YEAR)),
        # Invalid or malformed values
        ("Non-numeric birth or death year", "name.basics", names,
         names.where(is_not_numeric("birthYear")
                     | is_not_numeric("deathYear"))),
        ("Non-numeric start or end year", "title.basics", titles,
         titles.where(is_not_numeric("startYear")
                      | is_not_numeric("endYear"))),
        ("Runtime not numeric or <= 0", "title.basics", titles,
         titles.where(is_not_numeric("runtimeMinutes")
                      | (as_int("runtimeMinutes") <= 0))),
        ("isAdult not equal to 0 or 1", "title.basics", titles,
         titles.where(~F.col("isAdult").isin("0", "1")
                      | F.col("isAdult").isNull())),
        ("Rating outside 1-10 or votes <= 0", "title.ratings", ratings,
         ratings.where((F.col("averageRating").try_cast("double") < 1)
                       | (F.col("averageRating").try_cast("double") > 10)
                       | (as_int("numVotes") <= 0))),
        ("Season or episode number <= 0", "title.episode", episodes,
         episodes.where((as_int("seasonNumber") <= 0)
                        | (as_int("episodeNumber") <= 0))),
        # Foreign keys with missing parent records
        ("Rating of an unknown title", "title.ratings", ratings,
         orphans(ratings, "tconst", titles, "tconst")),
        ("Episode of an unknown title", "title.episode", episodes,
         orphans(episodes, "tconst", titles, "tconst")),
        ("Episode with an unknown parent series", "title.episode", episodes,
         orphans(episodes, "parentTconst", titles, "tconst")),
        ("Principal on an unknown title", "title.principals", principals,
         orphans(principals, "tconst", titles, "tconst")),
        ("Principal with an unknown person", "title.principals", principals,
         orphans(principals, "nconst", names, "nconst")),
        ("Director with an unknown person", "title.crew", directors,
         orphans(directors, "nconst", names, "nconst")),
        ("Writer with an unknown person", "title.crew", writers,
         orphans(writers, "nconst", names, "nconst")),
        ("Alternative title of an unknown title", "title.akas", akas,
         orphans(akas, "titleId", titles, "tconst")),
        ("Known-for title that does not exist", "name.basics", known_for,
         orphans(known_for, "tconst", titles, "tconst")),
        # Malformed items inside comma-separated list columns
        ("Malformed genre in genres list", "title.basics", titles,
         bad_list_items(titles, "tconst", "genres", r"^[A-Z][A-Za-z-]+$")),
        ("Malformed title id in knownForTitles", "name.basics", names,
         bad_list_items(names, "nconst", "knownForTitles", r"^tt[0-9]+$")),
        ("Malformed person id in directors", "title.crew", crew,
         bad_list_items(crew, "tconst", "directors", r"^nm[0-9]+$")),
        ("Malformed person id in writers", "title.crew", crew,
         bad_list_items(crew, "tconst", "writers", r"^nm[0-9]+$")),
        # Duplicated keys
        ("Duplicated tconst", "title.basics", titles,
         duplicates(titles, ["tconst"])),
        ("Duplicated nconst", "name.basics", names,
         duplicates(names, ["nconst"])),
        ("Duplicated (tconst, ordering)", "title.principals", principals,
         duplicates(principals, ["tconst", "ordering"])),
    ]


def run_check(check, table, checked, issues):
    """Count the issues of one check and keep three example ids."""
    total = checked.count()
    count = issues.count()
    pct = round(100 * count / total, 4) if total else 0.0
    first_column = issues.columns[0]
    examples = [str(row[0]) for row in
                issues.select(first_column).limit(3).collect()]
    return [check, table, total, count, pct, ", ".join(examples)]


def print_markdown(rows):
    """Print rows as a Markdown table, ready to paste in REPORT.md."""
    print("| " + " | ".join(HEADER) + " |")
    print("|" + "---|" * len(HEADER))
    for row in rows:
        print("| " + " | ".join(str(value) for value in row) + " |")


def done_checks():
    """Return the rows already saved by a previous (interrupted) run."""
    if not OUTPUT_FILE.exists():
        return []
    with open(OUTPUT_FILE, newline="") as file:
        return list(csv.reader(file))[1:]


def main():
    """Run every check, save each result immediately and print them."""
    rows = done_checks()
    finished = {row[0] for row in rows}
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        with open(OUTPUT_FILE, "w", newline="") as file:
            csv.writer(file).writerow(HEADER)

    spark = get_spark("imdb-data-quality")
    # More, smaller shuffle partitions keep the large joins within memory.
    spark.conf.set("spark.sql.shuffle.partitions", "64")
    for check, table, checked, issues in build_checks(spark):
        if check in finished:
            print(f"{check}: already done, skipped", flush=True)
            continue
        row = run_check(check, table, checked, issues)
        with open(OUTPUT_FILE, "a", newline="") as file:
            csv.writer(file).writerow(row)
        rows.append(row)
        print(f"{check}: {row[3]} issues", flush=True)
    spark.stop()

    print("\n## Data quality issues\n")
    print_markdown(rows)


if __name__ == "__main__":
    main()
