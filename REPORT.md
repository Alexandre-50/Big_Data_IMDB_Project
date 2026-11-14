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

**Question:** has the share of popular films led by an actress grown
between 1980 and 2025, in which genres, and are actress-led films rated
and voted differently from actor-led films?

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

### Methodological Explanation