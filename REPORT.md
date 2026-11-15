# IMDb Big Data Processing Report

## 1. Data Preparation

### 1.1 Null and Missing Values per Column

| Table | Column | Total rows | Null | Empty | Missing % |
|---|---|---|---|---|---|
| name.basics | nconst | 15765984 | 0 | 0 | 0.0 |
| name.basics | primaryName | 15765984 | 87 | 0 | 0.0 |
| name.basics | birthYear | 15765984 | 15081377 | 0 | 95.66 |
| name.basics | deathYear | 15765984 | 15501152 | 0 | 98.32 |
| name.basics | primaryProfession | 15765984 | 3203725 | 0 | 20.32 |
| name.basics | knownForTitles | 15765984 | 1909359 | 0 | 12.11 |
| title.akas | titleId | 59949689 | 0 | 0 | 0.0 |
| title.akas | ordering | 59949689 | 0 | 0 | 0.0 |
| title.akas | title | 59949689 | 0 | 0 | 0.0 |
| title.akas | region | 59949689 | 12939076 | 0 | 21.58 |
| title.akas | language | 59949689 | 19549363 | 0 | 32.61 |
| title.akas | types | 59949689 | 40116604 | 0 | 66.92 |
| title.akas | attributes | 59949689 | 59633311 | 0 | 99.47 |
| title.akas | isOriginalTitle | 59949689 | 0 | 0 | 0.0 |
| title.basics | tconst | 12898691 | 0 | 0 | 0.0 |
| title.basics | titleType | 12898691 | 0 | 0 | 0.0 |
| title.basics | primaryTitle | 12898691 | 0 | 0 | 0.0 |
| title.basics | originalTitle | 12898691 | 0 | 0 | 0.0 |
| title.basics | isAdult | 12898691 | 0 | 0 | 0.0 |
| title.basics | startYear | 12898691 | 1487504 | 0 | 11.53 |
| title.basics | endYear | 12898691 | 12735133 | 0 | 98.73 |
| title.basics | runtimeMinutes | 12898691 | 8033359 | 0 | 62.28 |
| title.basics | genres | 12898691 | 547433 | 0 | 4.24 |
| title.crew | tconst | 12898691 | 0 | 0 | 0.0 |
| title.crew | directors | 12898691 | 5746998 | 0 | 44.55 |
| title.crew | writers | 12898691 | 6387208 | 0 | 49.52 |
| title.episode | tconst | 9978123 | 0 | 0 | 0.0 |
| title.episode | parentTconst | 9978123 | 0 | 0 | 0.0 |
| title.episode | seasonNumber | 9978123 | 2096402 | 0 | 21.01 |
| title.episode | episodeNumber | 9978123 | 2096402 | 0 | 21.01 |
| title.principals | tconst | 102512651 | 0 | 0 | 0.0 |
| title.principals | ordering | 102512651 | 0 | 0 | 0.0 |
| title.principals | nconst | 102512651 | 0 | 0 | 0.0 |
| title.principals | category | 102512651 | 0 | 0 | 0.0 |
| title.principals | job | 102512651 | 82720617 | 0 | 80.69 |
| title.principals | characters | 102512651 | 52510212 | 0 | 51.22 |
| title.ratings | tconst | 1725891 | 0 | 0 | 0.0 |
| title.ratings | averageRating | 1725891 | 0 | 0 | 0.0 |
| title.ratings | numVotes | 1725891 | 0 | 0 | 0.0 |

We started to download the IMdB dataset with all files as title.ratings, title.principals, title.basics. We download the different files (.tsv.gz) on 26th October 2026 on the website datasets.imdbws.com where frequently IMDb updates them daily, so the figures depend on that date. The files contains 7 tables with 39 columns for a count approximately 215.7 million rows in total.

To obtain the result, we used the IMDb marker `\N` which is converted to null upon reading the file, a value is considered "empty" if it is an empty string or contains only spaces. To calculate the percentage, we used the formula Percentage = (nulls + empty values) / total number of rows × 100.

In the results, we got the percentage of nulls, missing, or empty records. We see that `empty_count` is 0 everywhere because IMDb never uses empty strings, only `\N`. The columns (`tconst`, `nconst`, `titleId`, `ordering`) have a 0% null rate, however, this does not guarantee that they point to existing rows this will be verified in step 1.3. Also, some columns are always populated: `titleType`, `primaryTitle`, `originalTitle`, `isAdult`, `category`, and `isOriginalTitle`.

The table `title.ratings` has a 0% null rate, simply because the file only contains titles that have been rated. The "missing ratings" are evident elsewhere: only 1.73 million titles are rated out of 12.9 million (approximately 13%). The column `primaryName` contains 87 null values, representing individuals without a name. While this is a very small number, it is nonetheless surprising.

Certain columns will have a direct impact on the exploration phase as `runtimeMinutes` (62.28%) for question 2.1, regarding film durations, `startYear` (11.53%) for questions involving years (2.2 and 2.3), `directors` (44.55%) for question 2.4, regarding directors, `genres` (4.24%) for all questions involving genres.

Finally `seasonNumber` and `episodeNumber` have the exact same number of null values ​​(2,096,402). These likely correspond to the same episodes that lack both a season number and an episode number.

`characters` has a null rate of 51.22%: individuals who are not actors (such as directors, composers, etc.) do not have an associated character.

### 1.2 Top Five Columns with the Highest Null Percentage

| Rank | Table | Column | Null % |
|---|---|---|---|
| 1 | title.akas | attributes | 99.47 |
| 2 | title.basics | endYear | 98.73 |
| 3 | name.basics | deathYear | 98.32 |
| 4 | name.basics | birthYear | 95.66 |
| 5 | title.principals | job | 80.69 |

To get the five columns across the entire dataset that have the highest percentage of null values, we needed to understand the question well because it refers exclusively to null values, but since `empty_count` is 0 throughout, the ranking is the same as it would be for missing values.

Then the core idea, is that these nulls do not all carry the same meaning where some represent unknown data, while others correspond to information that is simply not applicable to a given row. 

- The column `attributes` is an optional label for an alternative title (“literal title,” “working title,” etc.), which applies only in rare cases.

- The column `endYear` this one, only concerns series (tvSeries, tvMiniSeries). It is meaningless for a film, and it does not yet exist for an ongoing series. 

- The column `deathYear` which is left blank for living individuals, but also when the date is unknown—and there is no way to distinguish between the two cases.

- The column `birthYear` for this instance, it is a genuine case of missing data, as most of the individuals listed are lesser-known crew members. 

- The column `job` which is a free-text specification of a role, which is often redundant with the `category` field.

Looking ahead to question 1.4, this distinction will guide our processing strategy. An absent `endYear` for a film is not treated the same way as an unknown `birthYear`.

### 1.3 Other Data Quality Issues

After we obtained the different null and missing values, in this step we searched the other issues such as people who died before they were born, foreign keys with missing parent records or some others.
For this, we got 27 checks across the 7 tables, grouped into 5 categories: impossible values, consistency between columns, non-existent references, list formatting, and duplicates. These data correspond to the "unfinished" Parquet tables located in `data/parquet` where all these columns are just text (type string) in Parquet files. We made a Python script `data_quality.py` located in `src/preparation/` with the 27 checks.

- Impossible Values: death before birth, "living" person over 120 years old, future year, duration ≤ 0, season or episode ≤ 0, rating outside the 1–10 range, votes ≤ 0, isAdult value other than 0 or 1.

- Consistency between columns: endYear precedes startYear; endYear appears for a title that is not a series.

- Referential integrity: identifiers that point to a non-existent row, identified using `left_anti` joins (retaining rows from the "child" table that have no corresponding match in the "parent" table).

- List format: comma-separated lists are split using `explode`, and then each element is validated against a regular expression (tt…, nm…, or a genre starting with an uppercase letter).

- Duplicates: use `groupBy` on the key, then keep the keys that have more than one occurrence.

In the script, we converted the numbers with `try_cast` which returns null if the value is not a number, instead of causing Spark 4 to crash. In output, we got the values located in the CSV file `data_quality.csv` where for each check, it includes the number of rows verified, the number of issues found, the percentage, and three example identifiers. The file is written line by line, which allows the script to resume from where it left off in the event of a crash—something we experienced due to memory issues.

Results: 

For the first results, out of 27 checks, only 11 identified issues, while the remaining 16 found none (no duplicates, no invalid formats, no inconsistencies between endYear and startYear, and no invalid ratings).

The primary issues relate to references to non-existent individuals or titles, as well as values that are suspicious yet plausible (e.g., future release dates, "season 0," or individuals of an advanced age). All percentages are below 0.1%.

We got in output the different rows checked: 

| Check | Table | Rows checked | Issues | Issues % |
|---|---|---|---|---|
| Principal with an unknown person | title.principals | 102512651 | 7403 | 0.0072 |
| Season or episode number <= 0 | title.episode | 9978123 | 7108 | 0.0712 |
| Start year in the future | title.basics | 12898691 | 5180 | 0.0402 |
| Alive person older than 120 years | name.basics | 15765984 | 2796 | 0.0177 |
| Writer with an unknown person | title.crew | 15201667 | 1146 | 0.0075 |
| Director with an unknown person | title.crew | 9568274 | 419 | 0.0044 |
| Principal on an unknown title | title.principals | 102512651 | 206 | 0.0002 |
| Death year before birth year | name.basics | 15765984 | 27 | 0.0002 |
| Known-for title that does not exist | name.basics | 25513329 | 10 | < 0.0001 |
| Episode with an unknown parent series | title.episode | 9978123 | 5 | 0.0001 |
| Runtime not numeric or <= 0 | title.basics | 12898691 | 4 | < 0.0001 |

For example, for the Check "Director with an unknown person", "Write" or "Known-for", the count refers to the items in the lists, not the rows in the table, since a single row may mention multiple people or multiple titles.

Based on the table, we can understand that there are actual errors and potential errors because for the first case, we have for example 27 deaths occurring before birth, 4 durations ≤ 0, and broken references (7,403, 1,146, 419, 206, 10, 5—these 5 episodes all originate from the series tt28606619). Indeed, for this the cause is probably about the broken references where IMDb files are exported separately.

For the 2nd case, we got only 5,180 upcoming titles (announcements), 7,108 seasons or episodes numeroted of 0 (special episodes and pilots), 2,796 individuals over 120 years old with no recorded death (death date not provided) which linked with the case deathYear we observed in the question 1.2. 

### 1.4 Handling Strategy

For this part, after found and observed the different issues on the Parquet tables, we used another Python script `clean_data.py` located in `src/preparation` will reads Parquet tables located `data/parquet` to write cleaned tables in Parquet in the path `data/processed`. There are three possible actions as "deleted", "correct" (set to null), or mark (add a Boolean column) to got the final results.
The issues value in this table shows, the treatment chosen for each problem: 

| Issue (1.3) | Count | Handling | Action |
|---|---|---|---|
| Principal with an unknown person | 7403 | rows removed | remove |
| Principal on an unknown title | 206 | rows removed | remove |
| Episode with an unknown parent series | 5 | rows removed | remove |
| Runtime <= 0 | 4 | `runtimeMinutes` set to null | correct |
| Death year before birth year | 27 | both years set to null, `has_invalid_life_dates = true` | correct and flag |
| Start year in the future | 5180 | kept, `is_upcoming = true` | flag |
| Alive person older than 120 years | 2796 | kept, `death_year_unknown = true` | flag |
| Season or episode number <= 0 | 7108 | kept (specials and pilots) | none |
| Director / writer with an unknown person | 419 / 1146 | kept in the lists, dropped by inner joins later | none |
| Known-for title that does not exist | 10 | kept in the list, dropped by inner joins later | none |
| Not applicable nulls (e.g. `endYear` of a movie) | - | kept | none |

At the output, for example years, duration, ordering, and votes become integers, the rating becomes a decimal number. the variable isAdult and isOriginalTitle become booleans or genres, primaryProfession, knownForTitles, directors, and writers become arrays (split by comma).

After this, in output, the script cleans the values and the CSV file contains only the number of rows before and after for each table `cleaning_summary.csv` in `results/preparation`. 
We got the new table with the comparison of the number of rows before and after cleaning:

| Table | Rows before | Rows after | Rows removed |
|---|---|---|---|
| name.basics | 15765984 | 15765984 | 0 |
| title.basics | 12898691 | 12898691 | 0 |
| title.ratings | 1725891 | 1725891 | 0 |
| title.episode | 9978123 | 9978118 | 5 |
| title.principals | 102512651 | 102505042 | 7609 |
| title.crew | 12898691 | 12898691 | 0 |
| title.akas | 59949689 | 59949689 | 0 |

In the table, we got at the column Rows removed 7609, the result from 7403 + 206 which corresponds to the numbers got in the previous question (1.3) with no row contained both an unknown person and an unknown title. The 5 deleted episodes belong to the unknown series tt28606619, only 2 out of 7 tables lost any rows, and in each case, it was less than 0.01% of their total rows. Finally, we have the other issues were either corrected or flagged without deleting any rows.

### Methodological Explanation

For the question 1.1 to the part 2 (Data Exploration), to ingest the different values on the tables, the Python script `ingest_raw.py` located in `src/preparation/` reads the 7 `.tsv.gz` files using Spark (sep="\t", nullValue="\N", quote="") and writes them out once in Parquet format to `data/parquet` where the number of rows is verified after the write operation and also tables that already have a `_SUCCESS` marker are skipped if the script is run again. We used this method because as we know Parquet is columnar and compressed, where a script that reads only 2 columns does not read the entire file, and subsequent reads are much faster than with compressed TSV (since gzip files cannot be split, Spark processes each file using a single task).
As alternative solution, we also could reread the TSV for every script which is simple but it changes the process time.

To the question 1.2, we used the Python script from the path `src/preparation/null_analysis.py` which reads the Parquet tables from `data/parquet`, where for each table we have just one aggregation which calculate, for all columns simultaneously firstly the number of nulls (isNull)—that is, the `\N` values ​​converted during ingestion and secondly the number of empty or whitespace-only strings (`trim(column) == ""`). We obtain in output the file `missing_values.csv` from `results/preparation`. 
We used this method because we want to have a single pass per table—7 passes in total for the 39 columns, With tables containing over 100 million rows and each pass is costly. Counting empty strings also makes it possible to verify that IMDb uses only `\N` for missing values, rather than simply assuming so. The result `empty_count = 0` confirms this. The other advantage is that at the question 1.2 we do not trigguer any new Spark computations because it reuses just the result from the previous question.
For this as alternative solution, we could use just a request per column which is moresimple to write and read but with 39 readings contrary to 7 is much slower.

For the question 1.3, we chose the method explained previously such as `left_anti` join for broken references because this is a direct way to keep only those rows that have no corresponding match in the other table. We used the Spark function `try_cast` because we wanted to check that if a value is not a number, it becomes `null` instead of causing the job to fail—which, in fact, makes it possible to count such instances. For the Spark treatment, we got a line entry written after each check because Spark crashed due to the fact that the computer only has 8 GB of RAM, which is not enough for these volumes. This ensures that only the current check is lost, and the script can resume from where it left off. 
Alternatively, we could used a standard join followed by a function`isNull` which filter yields the same result as a `left_anti` join, but it retains the columns from both tables and consumes more memory.

For the question 1.4, we used `left_semi` via the method `.join` to retain only those rows where the title or the person exists, also `when`to set impossible values ​​to `null`, adding new Boolean columns as markers and writing each table as soon as it is ready, accompanied by a `_SUCCESS` marker. 

![Spark tasks during the cleaning step](images/cleaning_spark_tasks.png)

We used this method to handle different things because we want to remove only what would break the joins, we keep plausible values ​​because they are real and the flagging columns allow each question in Part 2 (Data Exploration) to decide whether or not to filter. Alternatively, we also could have chosen not to perform any cleaning or filtering within each script but the same task would be repeated everywhere, with a risk of something being overlooked.

## 2. Data Exploration

### 2.1 Feature-Length Movies with Atypical Runtime per Genre

### 2.2 Top Three Genres per Year (1980-2025)

### 2.3 Top Three Genres Compared to the Others

### 2.4 Directors with One Hit and Several Low-Rated Movies

### 2.5 TV Series with Worst Episode in the Last Season

### 2.6 Actors Successful in Multiple Languages

### 2.7 Actors Whose Popularity Peaked Early

### 2.8 Multi-Talented Professionals

### Methodological Explanation

## 3. Data Enrichment

### Results

Scope: for every genre and year from 1980 to 2025, the 10 most voted
feature films (movie, at least 40 minutes). Sources: Wikidata (IMDb ID
property P345) to find the English Wikipedia article, then Wikipedia for
the texts. Collected on 14 November 2026. Coverage table:
`results/enrichment/coverage.csv`, readable sample (Drama and Sci-Fi,
2010, top 3): `results/enrichment/sample_enriched.json`.

| Metric | Value |
|---|---|
| Slots (genre, year, rank) | 10 198 |
| Distinct films | 7 185 |
| Distinct principals kept | 10 948 |
| Median synopsis length | 593 words |

Synopsis coverage (distinct films):

| Status | Films | % |
|---|---|---|
| Plot section of the Wikipedia article | 5 986 | 83.31 |
| Introduction only (article without a Plot section) | 380 | 5.29 |
| No Wikidata item for the IMDb id | 490 | 6.82 |
| Wikidata item but no English article | 328 | 4.57 |
| Article without text | 1 | 0.01 |

Biography coverage (principals kept):

| Status | People | % |
|---|---|---|
| Biography (introduction of the Wikipedia article) | 8 943 | 81.69 |
| No Wikidata item for the IMDb id | 1 404 | 12.82 |
| Wikidata item but no English article | 600 | 5.48 |
| Article without text | 1 | 0.01 |

Films by number of principals with a biography (up to 6 candidates per
film, the first 3 with a biography are kept):

| Principals with a biography | Films | % |
|---|---|---|
| 3 | 6 101 | 84.91 |
| 2 | 376 | 5.23 |
| 1 | 377 | 5.25 |
| 0 | 331 | 4.61 |

Fields of an enriched record: genre, year, rank in genre, IMDb id,
title, rating, votes, `synopsis`, `synopsis_status`, `synopsis_words`,
`synopsis_lineage` (source URL, Wikipedia title, Wikidata item, retrieval
time, extraction method), and `principals`: array of (billing rank, IMDb
id, name, category, `biography`, `biography_status`,
`biography_lineage`).

### Methodological Explanation

**Film selection.** In this project, a film counts as a full movie when it is an IMDb title of type movie and lasts at least 40 minutes. This is the same rule we used in Section 2.1, and it follows the definition of the Academy and the AFI. For each genre and each year, we keep the 10 films with the most votes, like we did in Section 2.2. We chose to rank films by number of votes and not by rating, and this was on purpose. A film rated 9 out of 10 by only 50 people is usually unknown, and very often it does not even have a Wikipedia page. The number of votes is a better way to see how many people have really watched a film. Because a film can have several genres, it is counted once in each of its genres. This is why our selection has 10,198 slots but only 7,185 different films.

**Finding the right Wikipedia page.** If we search Wikipedia only with the title of a film, it often fails, because many titles are ambiguous. Good examples are "Heat", "Crash", or any film that has a remake. To solve this problem, we use Wikidata. Wikidata has a property called P345 that stores the IMDb identifier of a film. The Wikidata item that has this identifier links directly to the English Wikipedia article of the film. This gives us an exact link, and we never have to guess. We also thought about searching with the title and the release year together, but we decided not to do it because it is less reliable.

**Only texts written by humans.** We only use Wikidata to find the link to the right article, and all the texts in our dataset come from Wikipedia, which is written by real people. So our dataset does not contain any text generated by a machine. The synopsis of each film comes from the "Plot" section of its Wikipedia article. When a film has no Plot section, we use the introduction of the article instead, and we mark it as lead_section. The biographies of people come from the introduction of their own Wikipedia article.

**Ethics and rate limits.** We wanted to be respectful of the Wikimedia servers. Every request we send has a clear User Agent that explains who we are and gives the address of our project. We send the requests one after the other, with a short pause between each one. We also use the maxlag parameter with a value of 5, and we follow the Retry After header. During the collection, Wikidata did send us the HTTP error 429, which means too many requests, and our script waited for the time that the server asked. Our script can also restart where it stopped: when we run it again, it skips all the identifiers that were already collected.

**Collection outside Spark, cleaning inside Spark.** We collect the data with simple Python code, because Wikimedia asks to send the requests one after the other with pauses. If we used Spark for this, many workers would send requests at the same time, which would go against the rate limits. All the other steps are done in Spark, as the assignment asks. In Spark, we extract the Plot section with regular expressions, we remove the small headings and the extra spaces, we give a status to each record, we join the data with the IMDb tables, and we build the nested array of principals.

**Missing data is always visible.** We never hide missing information. Each record has a status that explains what happened: no_wikidata_item, no_english_article, lead_section or no_text. In total, 83% of the films have a Plot section and 5% only have an introduction. The other 11% have no English article at all: 7% have no Wikidata item, and 5% have a Wikidata item but no English Wikipedia article.

**At least three principals.** For each film, we look at up to six people in the order of the credits, and we keep the first three who have a biography. Thanks to this rule, 85% of the films have three principals. If we had only looked at the first three people in the credits, we would have reached only 74%. We also remove duplicates when the same person appears twice in the credits. For example, Natalie Portman is credited both as an actress and as a producer on Black Swan, but we keep her only once.

**Lineage.** Every text keeps its source URL, its Wikidata item, the date when we collected it and the method we used to extract it. This way, it is always possible to find where each value comes from.

**Generic code.** The time period, the size of the top list and the number of candidates are options that we give on the command line. Thanks to this, the same code can process the full dataset without any change.

**Limitations.** Recent films and films that are not in English have less chance of having an English Wikipedia article. Also, Wikipedia pages change over time, so our results depend on the day we collected the data, which was 14 November 2026.

## 4. Streaming

### Streaming Metrics

Test run: 10 November 2026, 16:33 to 17:03 UTC (30 minutes), on an 8 GB
laptop. Window: 5 minutes, watermark: 2 minutes, trigger: 30 seconds.
Full output: `results/streaming/summary.md`.

| Metric | Value |
|---|---|
| Events read from Kafka | 60 970 |
| Events in closed windows | 53 991 |
| English Wikipedia article edits (closed windows) | 2 360 |
| Edits matched with IMDb | 137 |
| Matched on a title / on a person | 90 / 47 |
| Distinct IMDb entities | 94 |
| IMDb lookup table | 58 542 titles, 226 714 people |

| Query | Batches | Input rows | Avg inputRowsPerSecond | Avg processedRowsPerSecond | Rows dropped by watermark |
|---|---|---|---|---|---|
| matched_events | 61 | 60 970 | 33.7 | 303.2 | 0 |
| title_windows | 61 | 60 970 | 33.8 | 273.8 | 0 |
| stream_windows | 61 | 60 955 | 33.8 | 313.3 | 0 |

Arrival delay of the matched edits (Kafka time minus edit time):

| Median | p95 | p99 | Max | Edits later than 2 min |
|---|---|---|---|---|
| 3 s | 9 s | 15 s | 20 s | 0 |

Top 10 most edited IMDb entities:

| IMDb id | Type | Label | Edits | Editors |
|---|---|---|---|---|
| nm0909144 | person | David Walliams | 6 | 3 |
| tt0054545 | title | Hazel | 4 | 1 |
| tt0388629 | title | One Piece | 4 | 3 |
| tt0070842 | title | Turkish Delight | 4 | 1 |
| nm2908492 | person | Grant Harvey | 3 | 1 |
| nm0001001 | person | James Caan | 3 | 2 |
| tt32641461 | title | Mid-Century Modern | 3 | 2 |
| nm1104787 | person | Mutya Buena | 3 | 2 |
| tt0162866 | title | The Emperor and the Assassin | 3 | 1 |
| tt0113613 | title | The Last Supper | 3 | 1 |

### Late Arriving Data

We use event time and not processing time. This means that the time of an event is the timestamp sent by Wikipedia, which is the moment when the edit was made, and not the moment when the event arrives in Spark. Because of this, if an event is late because of the network or because of a reconnection, it is still counted in the window where it really belongs.

We group the events in windows of 5 minutes, with a watermark of 2 minutes. The watermark is equal to the latest event time seen by Spark minus 2 minutes. Spark keeps a window open until the watermark goes past the end of this window. At that moment, the window is closed, it is written to the output, and its state is removed from memory. Any event that is older than the watermark is thrown away.

We chose the value of 2 minutes based on real measurements. We measured the delay between the moment of the edit and the moment the event arrived in Kafka. The median delay was 3 seconds, the 95th percentile was 9 seconds, the 99th percentile was 15 seconds, and the maximum was 20 seconds. No event was dropped during our test, since numRowsDroppedByWatermark was equal to 0. So two minutes gives us a large safety margin compared to the delays we observed.

This choice is a compromise. With a longer watermark, the results would be more complete, but they would be published later and Spark would need to keep more data in memory. With a shorter watermark, the results would come faster, but we would lose more late events, for example the events that the producer sends again when it reconnects with the Last Event ID.

We write the results in append mode. This means that each window is written only one time, when it is final, and it is never changed after that. As an extra protection, the raw table called matched_events has no watermark, so it keeps every event, even the late ones.

### Methodological Explanation

**Architecture.** The data starts from Wikimedia EventStreams, which uses Server Sent Events. A Python producer reads these events and sends them to a Kafka topic. Then Spark Structured Streaming reads the events from Kafka. Kafka separates the source from Spark, which has two big advantages: we can replay the topic, and if Spark restarts, it continues from its checkpoint without losing any data.

**Filtering.** We only keep events from English Wikipedia, in the article namespace, which is namespace 0, and only events of type edit and new. In the closed windows, this left 2,360 article edits out of about 54,000 events.

**Matching with IMDb.** We first normalise the article titles. We remove the disambiguation part at the end of the title and we put all the text in lower case, so "Inception (film)" becomes simply "inception". Then we join the stream with a static IMDb lookup table. This table is broadcast to all the tasks, so the join does not need any shuffle.

**Lookup table.** The table contains 58,542 titles that have at least 1,000 votes, and 226,714 people. These people are actors, actresses, directors or writers who are known for a title with more than 50,000 votes. When two entries have the same name, we keep the one with the most votes. We also remove titles that are only numbers, because a title like "1917" would also match articles about years.

**Iterations.** Our first version had 1.66 million people in the table, and this created a lot of false matches. So we made the table smaller, and we added a rule about the end of the title. For example, "Gemini (language model)" and "Mike Myers (basketball)" are rejected, but endings like "(film)" or "(actor)" are accepted.

**Robustness.** We use checkpoints, longer Kafka timeouts and automatic restarts. We added these settings because a Kafka timeout stopped our first test after only 14 minutes.

**Results.** During a test of 30 minutes, we got 137 matches for 94 different entities. The input rate was about 34 events per second, and the processing rate was about 270 to 315 rows per second.

**Limitations and alternatives.** Some false matches are still there, such as "Turkish delight", which is a sweet and not a film, or names like "Andy" or "Jim Jones". We could improve this by comparing the titles with the exact case of the letters, or by using Wikidata like in the Enrichment part, to get an exact link between Wikipedia articles and IMDb identifiers.

## 5. Open Question

### Question and Motivation

**Question:** has the share of popular films led by an actress grown
between 1980 and 2025, in which genres, and are actress-led films rated
and voted differently from actor-led films?

The place of women on screen is a topic that people discuss a lot, from the #MeToo movement in 2017 to the many studies about representation in cinema. We wanted to check if IMDb data shows a real change over time in how often films have an actress in the main role, and if these films are received differently by the public.

This question is also interesting from a data point of view. IMDb does not have a column for gender, but we can guess it thanks to the categories actor and actress in the table title.principals.

### Results

Scope: 38 297 feature films (movie, at least 40 minutes, 1980-2025, at
least 1 000 votes) with a credited lead. Lead = performer (actor or
actress) with the smallest billing order in `title.principals`. Full
tables: `results/open_question/`.

Share of actress-led films, every five years:

| Year | Films | Actress-led % | Actresses in top-3 cast % | Avg rating, actress-led | Avg rating, actor-led | Median votes, actress-led | Median votes, actor-led |
|---|---|---|---|---|---|---|---|
| 1980 | 287 | 22.3 | 35.2 | 6.01 | 6.32 | 2 652 | 3 781 |
| 1985 | 314 | 21.3 | 32.5 | 6.38 | 6.27 | 3 277 | 4 354 |
| 1990 | 364 | 20.3 | 33.2 | 6.19 | 6.16 | 3 366 | 4 618 |
| 1995 | 429 | 27.7 | 34.8 | 6.21 | 6.32 | 5 518 | 5 377 |
| 2000 | 578 | 31.0 | 37.8 | 6.21 | 6.27 | 4 118 | 4 833 |
| 2005 | 843 | 27.6 | 37.4 | 6.07 | 6.17 | 3 739 | 4 261 |
| 2010 | 1 054 | 27.7 | 35.5 | 5.99 | 6.15 | 3 771 | 3 757 |
| 2015 | 1 383 | 31.0 | 38.1 | 5.93 | 5.99 | 3 187 | 3 512 |
| 2020 | 1 238 | 38.7 | 41.2 | 5.70 | 5.85 | 3 975 | 3 188 |
| 2025 | 1 308 | 32.9 | 37.7 | 6.05 | 6.17 | 2 865 | 2 997 |

Highest yearly share: 38.7 % (2020). Lowest: 18.9 % (1989).

Share of actress-led films per genre (genres with at least 500 films):

| Genre | Films | Actress-led % | 1980s % | 2020s % | Change (points) |
|---|---|---|---|---|---|
| Horror | 5 434 | 44.7 | 32.2 | 52.2 | +20.0 |
| Mystery | 3 427 | 39.9 | 27.8 | 48.7 | +20.9 |
| Romance | 6 332 | 39.3 | 32.4 | 46.0 | +13.6 |
| Animation | 1 508 | 35.6 | 25.2 | 39.2 | +14.0 |
| Fantasy | 2 196 | 32.5 | 22.6 | 42.7 | +20.1 |
| Drama | 21 734 | 32.0 | 25.7 | 36.7 | +11.0 |
| Music | 1 012 | 31.4 | 24.8 | 34.2 | +9.4 |
| Family | 1 554 | 31.3 | 27.0 | 36.2 | +9.2 |
| Thriller | 6 641 | 31.1 | 28.2 | 36.0 | +7.8 |
| Comedy | 13 597 | 27.2 | 19.1 | 35.5 | +16.4 |
| Biography | 1 848 | 27.2 | 29.9 | 30.3 | +0.4 |
| Sci-Fi | 1 878 | 27.1 | 16.2 | 40.9 | +24.7 |
| Documentary | 714 | 26.8 | 29.4 | 30.7 | +1.3 |
| History | 1 337 | 24.0 | 21.8 | 27.2 | +5.4 |
| Adventure | 4 090 | 23.8 | 16.3 | 31.3 | +15.0 |
| War | 682 | 21.4 | 15.2 | 25.0 | +9.8 |
| Crime | 6 297 | 20.1 | 14.4 | 26.7 | +12.3 |
| Action | 7 767 | 16.6 | 10.0 | 21.6 | +11.6 |
| Sport | 692 | 14.0 | 8.6 | 19.5 | +10.9 |

Rating gap (actress-led minus actor-led average rating) and votes ratio
(geometric mean votes, actress-led / actor-led), computed inside each
(genre, decade) group with at least 30 films of each kind, then weighted
by the number of films:

| Decade | Film-genre rows | Groups | Weighted rating gap | Weighted votes ratio |
|---|---|---|---|---|
| 1980s | 7 377 | 14 | -0.178 | 0.734 |
| 1990s | 10 012 | 15 | -0.111 | 0.766 |
| 2000s | 19 013 | 18 | -0.167 | 0.896 |
| 2010s | 31 883 | 20 | -0.147 | 0.922 |
| 2020s | 19 530 | 18 | -0.214 | 1.076 |
| All | 87 815 | 85 | -0.165 | 0.917 |

### Key Takeaways

- **A clear progression.** In 1980, only 22% of films had an actress in the main role. In the 2020s, this share is between 33% and 39%, with a peak of 38.7% in 2020. This increase happened in two steps: a first rise from 20% in 1990 to 31% in 2000, then a plateau between 28% and 31% until 2016, and a second rise up to 38.7% in 2020, at the time of the #MeToo movement.
- **Big differences between genres.** Horror and Mystery are the genres with the most films led by an actress, with 52% for Horror in the 2020s. On the other side, Action with 22% and Sport with 20% are still mostly led by men.
- **The biggest increase is in Science Fiction.** The share of films led by an actress went from 16% to 41%, which is a gain of 25 points. In comparison, Biography and Documentary have almost not changed.
- **Ratings.** When we compare films of the same genre and the same decade, the films led by an actress get a rating that is 0.17 points lower out of 10. The difference is small, but it is the same in every decade.
- **Votes.** Films led by an actress get about 8% fewer votes on average, but this difference is getting smaller. The ratio went from 0.73 in the 1980s to 1.08 in the 2020s, which means that today these films actually get a little more votes.

### Methodological Explanation

**Scope.** We study films that last at least 40 minutes, that were released between 1980 and 2025, and that have at least 1,000 votes. This gives us 38,297 films. We use the limit of 1,000 votes to focus on films that the public really watches, and to avoid ratings given by only a few people.

**Main role.** The main role is the actor or actress with the smallest ordering value. We also compute a second measure: the share of actresses among the first three people in the cast. This way, our results do not depend on only one person.

**Composition bias.** This is the most important point of our analysis. If we simply compared the averages, we would mix all the genres together. For example, horror films get lower ratings and are also more often led by an actress, so this would make films led by an actress look worse in general. To avoid this problem, we compute the difference inside each pair of genre and decade. We only keep the groups that have at least 30 films of each type, and then we compute the average weighted by the number of films.

**Votes.** For the votes, we use the geometric mean, which is the mean of the logarithms. We do this because the number of votes is very unbalanced: a few blockbusters have several million votes, while most films have much fewer.

**Limitations.** Our analysis has some limits. First, gender is guessed from the IMDb category, so it is only binary. Second, the ordering in IMDb does not always follow the official billing order of the film. Third, IMDb ratings can be affected by review bombing campaigns, where many people give bad ratings on purpose. Finally, our results show correlations and not causes, so we cannot say that one thing causes the other.