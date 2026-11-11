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

### 1.4 Handling Strategy

### Methodological Explanation

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

### Methodological Explanation

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

### Methodological Explanation

## 5. Open Question

### Question and Motivation

### Results

### Key Takeaways

### Methodological Explanation