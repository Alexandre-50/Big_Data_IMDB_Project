## Volumes

| metric | value |
|---|---|
| events received (closed windows) | 53991 |
| English Wikipedia article edits (closed windows) | 2360 |
| edits matched with IMDb (whole run) | 137 |
| matched on a title | 90 |
| matched on a person | 47 |
| distinct IMDb entities | 94 |

## Throughput (query.recentProgress)

| query | first batch | last batch | batches | input rows | avg inputRowsPerSecond | avg processedRowsPerSecond | rows dropped by watermark |
|---|---|---|---|---|---|---|---|
| console | 2026-11-10T16:33:25.623Z | 2026-11-10T17:03:00.000Z | 61 | 60955 | 34.4 | 1124.8 | 0 |
| matched_events | 2026-11-10T16:33:24.592Z | 2026-11-10T17:03:00.000Z | 61 | 60970 | 33.7 | 303.2 | 0 |
| stream_windows | 2026-11-10T16:33:25.585Z | 2026-11-10T17:03:00.000Z | 61 | 60955 | 33.8 | 313.3 | 0 |
| title_windows | 2026-11-10T16:33:25.180Z | 2026-11-10T17:03:00.000Z | 61 | 60970 | 33.8 | 273.8 | 0 |

## Arrival delay (Kafka time - edit time)

| median (s) | p95 (s) | p99 (s) | max (s) | edits later than 2 min |
|---|---|---|---|---|
| 3 | 9 | 15 | 20 | 0 |

## Top 10 most edited IMDb entities

| imdb_id | type | label | edits | editors |
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

## Sample of matched events

| event_time | Wikipedia page | type | IMDb label |
|---|---|---|---|
| 2026-11-10 16:33:53 | Turkish delight | title | Turkish Delight |
| 2026-11-10 16:34:07 | Polly Bergen | person | Polly Bergen |
| 2026-11-10 16:34:12 | Smash Mouth | person | Smash Mouth |
| 2026-11-10 16:34:41 | Andy | person | Andy |
| 2026-11-10 16:34:46 | Napoleon (actor) | title | Napoleon |
| 2026-11-10 16:35:09 | Jim Jones | person | Jim Jones |
| 2026-11-10 16:35:14 | Mutya Buena | person | Mutya Buena |
| 2026-11-10 16:35:42 | Kevin Spencer (TV series) | person | Kevin Spencer |
| 2026-11-10 16:35:59 | SiX | title | Six |
| 2026-11-10 16:36:01 | Screamers (2006 film) | title | Screamers |
