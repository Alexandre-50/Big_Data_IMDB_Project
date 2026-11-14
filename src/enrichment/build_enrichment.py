"""Clean the collected Wikipedia texts and build the enriched dataset.

Step 3 of 3: all cleaning and shaping is done with Spark.

- Synopsis: the Plot section (or Synopsis, Premise, Story...) is
  extracted from the plain-text film article; sub-headings and extra
  whitespace are removed. When the article has no such section, its
  introduction is used instead and flagged as such.
- Biography: the introduction of the person's article, cleaned the same
  way (empty parentheses left by removed pronunciations are dropped).
- Missing data is explicit: every film and every person gets a status
  (plot_section, lead_section, no_english_article, no_wikidata_item,
  page_missing, no_text) instead of a silent null.
- Lineage: each record keeps its source URL, the Wikidata item used to
  find the page, the retrieval time and the extraction method.

Outputs:
- data/enrichment/enriched_films (Parquet): one row per (genre, year,
  rank) slot, with the synopsis and an array of principals with their
  biographies;
- results/enrichment/coverage.csv and summary.md: coverage statistics;
- results/enrichment/sample_enriched.json: a readable sample.
"""
import json

from pyspark.sql import Window
from pyspark.sql import functions as F

from src.common.paths import DATA_DIR, RESULTS_DIR
from src.common.spark_session import get_spark

ENRICHMENT_DIR = DATA_DIR / "enrichment"
RAW_DIR = ENRICHMENT_DIR / "raw"
OUTPUT_DIR = RESULTS_DIR / "enrichment"
PRINCIPALS_PER_FILM = 3
SAMPLE_YEAR = 2010
SAMPLE_GENRES = ["Drama", "Sci-Fi"]
SAMPLE_RANKS = 3

# "== Plot ==" heading (and its usual variants) up to the next top-level
# heading or the end of the article.
PLOT_REGEX = (r"(?s)(?:^|\n)== *(?:Plot|Plot summary|Synopsis|Premise|"
              r"Story|Storyline|Summary) *==\n(.*?)(?=\n== [^=]|$)")
# Introduction = everything before the first heading.
LEAD_REGEX = r"(?s)^(.*?)(?=\n==|$)"


def clean_text(column):
    """Remove sub-headings, empty parentheses and repeated whitespace."""
    text = F.regexp_replace(column, r"(?m)^=+[^=\n]*=+\s*$", " ")
    text = F.regexp_replace(text, r"\(\s*[;,]?\s*\)", "")
    text = F.regexp_replace(text, r"\s+", " ")
    text = F.trim(text)
    return F.when(F.length(text) > 0, text)


def word_count(column):
    """Number of words of a text column (0 when null)."""
    return F.coalesce(F.size(F.split(column, " ")), F.lit(0))


def link_status(links, pages):
    """Join links and pages and derive one status column."""
    joined = links.join(pages, "imdb_id", "left")
    return joined.withColumn(
        "link_status",
        F.when(F.col("status") != "linked", F.col("status"))
        .when(F.col("page_missing"), "page_missing")
        .when(F.col("text").isNull(), "no_text")
        .otherwise("linked"),
    )


def build_films(spark, links):
    """Return one row per film with its cleaned synopsis and lineage."""
    pages = spark.read.json(str(RAW_DIR / "film_pages.jsonl")).select(
        "imdb_id", "page_missing", "text",
        F.col("wikipedia_title").alias("page_title"),
        F.col("source_url").alias("page_url"),
        F.col("retrieved_at").alias("page_retrieved_at"))
    films = link_status(links, pages)
    plot = clean_text(F.regexp_extract("text", PLOT_REGEX, 1))
    lead = clean_text(F.regexp_extract("text", LEAD_REGEX, 1))
    status = (F.when(F.col("link_status") != "linked", F.col("link_status"))
              .when(plot.isNotNull(), "plot_section")
              .when(lead.isNotNull(), "lead_section")
              .otherwise("no_text"))
    return (
        films
        .withColumn("synopsis_status", status)
        .withColumn("synopsis",
                    F.when(status == "plot_section", plot)
                    .when(status == "lead_section", lead))
        .withColumn("synopsis_words", word_count("synopsis"))
        .select(
            F.col("imdb_id").alias("tconst"), "synopsis", "synopsis_status",
            "synopsis_words",
            F.struct(
                F.coalesce("page_url", "source_url").alias("source_url"),
                F.coalesce("page_title", "wikipedia_title")
                .alias("wikipedia_title"),
                "wikidata_qid",
                F.coalesce("page_retrieved_at", "retrieved_at")
                .alias("retrieved_at"),
                F.when(status == "plot_section",
                       "Wikipedia article, Plot section")
                .when(status == "lead_section",
                      "Wikipedia article, introduction (no Plot section)")
                .otherwise("none").alias("method"),
            ).alias("synopsis_lineage"),
        )
    )


def build_people(spark, links):
    """Return one row per person with the cleaned biography and lineage."""
    pages = spark.read.json(str(RAW_DIR / "person_intros.jsonl")).select(
        "imdb_id", "page_missing", "text",
        F.col("wikipedia_title").alias("page_title"),
        F.col("source_url").alias("page_url"),
        F.col("retrieved_at").alias("page_retrieved_at"))
    people = link_status(links, pages)
    bio = clean_text(F.col("text"))
    status = (F.when(F.col("link_status") != "linked", F.col("link_status"))
              .when(bio.isNotNull(), "biography")
              .otherwise("no_text"))
    return (
        people
        .withColumn("biography_status", status)
        .withColumn("biography", F.when(status == "biography", bio))
        .select(
            F.col("imdb_id").alias("nconst"), "biography",
            "biography_status",
            F.struct(
                F.coalesce("page_url", "source_url").alias("source_url"),
                F.coalesce("page_title", "wikipedia_title")
                .alias("wikipedia_title"),
                "wikidata_qid",
                F.coalesce("page_retrieved_at", "retrieved_at")
                .alias("retrieved_at"),
            ).alias("biography_lineage"),
        )
    )


def coverage(df, status_column, entity):
    """Return (entity, status, count, pct) rows for a status column."""
    total = df.count()
    rows = df.groupBy(status_column).count().orderBy(F.desc("count"))
    return [[entity, row[0], row[1], round(100 * row[1] / total, 2)]
            for row in rows.collect()]


def markdown_table(header, rows):
    """Return rows as a Markdown table."""
    lines = ["| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    lines += ["| " + " | ".join(str(value) for value in row) + " |"
              for row in rows]
    return "\n".join(lines)


def main():
    """Build the enriched dataset and its coverage statistics."""
    spark = get_spark("imdb-enrichment-build")
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    links = spark.read.json(str(RAW_DIR / "wikidata_links.jsonl"))
    slots = spark.read.parquet(str(ENRICHMENT_DIR / "top_films"))
    principals = spark.read.parquet(str(ENRICHMENT_DIR / "film_principals"))

    films = build_films(spark, links).cache()
    people = build_people(spark, links).cache()

    # Keep the first PRINCIPALS_PER_FILM candidates (billing order) that
    # have a biography; people without one are used only as a last resort.
    has_bio = (F.col("biography_status") == "biography").cast("int")
    best_first = Window.partitionBy("tconst").orderBy(
        F.desc("has_bio"), "principal_rank")
    cast = (
        principals.join(people, "nconst", "left")
        .withColumn("biography_status",
                    F.coalesce("biography_status", F.lit("not_collected")))
        .withColumn("has_bio", has_bio)
        .withColumn("pick", F.row_number().over(best_first))
        .where(F.col("pick") <= PRINCIPALS_PER_FILM)
        .groupBy("tconst")
        .agg(F.sort_array(F.collect_list(F.struct(
            "principal_rank", "nconst", "primaryName", "category",
            "biography", "biography_status", "biography_lineage",
        ))).alias("principals"))
        .withColumn("principals_with_bio", F.size(F.filter(
            "principals", lambda p: p["biography_status"] == "biography")))
    )
    enriched = (
        slots.join(films, "tconst", "left")
        .join(cast, "tconst", "left")
        .withColumn("principals", F.coalesce("principals", F.array()))
        .withColumn("principals_with_bio",
                    F.coalesce("principals_with_bio", F.lit(0)))
        .orderBy("genre", "year", "rank_in_genre")
    )
    enriched.write.mode("overwrite").parquet(
        str(ENRICHMENT_DIR / "enriched_films"))
    enriched = spark.read.parquet(str(ENRICHMENT_DIR / "enriched_films"))

    distinct_films = enriched.dropDuplicates(["tconst"])
    kept = distinct_films.select(F.explode("principals.nconst")
                                 .alias("nconst")).distinct()
    used_people = people.join(kept, "nconst")
    rows = (coverage(distinct_films, "synopsis_status", "film")
            + coverage(used_people, "biography_status", "person")
            + coverage(distinct_films.withColumn(
                "bios", F.least(F.col("principals_with_bio"), F.lit(3))
                .cast("string")), "bios", "film: principals with bio"))
    header = ["entity", "status", "count", "pct"]
    with open(OUTPUT_DIR / "coverage.csv", "w", encoding="utf-8") as file:
        file.write(",".join(header) + "\n")
        for row in rows:
            file.write(",".join(str(value) for value in row) + "\n")

    totals = [
        ["slots (genre, year, rank)", enriched.count()],
        ["distinct films", distinct_films.count()],
        ["distinct principals", used_people.count()],
        ["median synopsis length (words)", distinct_films.where(
            F.col("synopsis_words") > 0).agg(
            F.percentile_approx("synopsis_words", 0.5)).first()[0]],
    ]
    summary = ("## Volumes\n\n" + markdown_table(["metric", "value"], totals)
               + "\n\n## Coverage\n\n" + markdown_table(header, rows) + "\n")
    (OUTPUT_DIR / "summary.md").write_text(summary, encoding="utf-8")
    print(summary)

    sample = (enriched
              .where((F.col("year") == SAMPLE_YEAR)
                     & F.col("genre").isin(SAMPLE_GENRES)
                     & (F.col("rank_in_genre") <= SAMPLE_RANKS))
              .orderBy("genre", "rank_in_genre"))
    with open(OUTPUT_DIR / "sample_enriched.json", "w",
              encoding="utf-8") as file:
        json.dump([json.loads(row) for row in sample.toJSON().collect()],
                  file, ensure_ascii=False, indent=2)
    spark.stop()


if __name__ == "__main__":
    main()
