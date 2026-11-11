# IMDb Big Data Processing Report

## 1. Data Preparation

### 1.1 Null and Missing Values per Column

### 1.2 Top Five Columns with the Highest Null Percentage

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