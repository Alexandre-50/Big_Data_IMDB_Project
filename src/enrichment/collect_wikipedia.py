"""Collect Wikipedia texts for the selected films and people (step 2 of 3).

Collection runs outside Spark (plain HTTP requests); the raw answers are
stored as JSON lines and cleaned with Spark in build_enrichment.py.

1. Link: every IMDb id is resolved to its English Wikipedia article
   through Wikidata (property P345 = IMDb ID), with batched SPARQL
   queries. This avoids guessing pages from titles, which fails on
   homonyms ("Heat", "Crash").
2. Films: the full plain-text article is downloaded (one request per
   film); the Plot section is extracted later with Spark.
3. People: the introduction of the article (biographical summary) is
   downloaded, 20 people per request.

Only human-written sources are used (Wikipedia, Wikidata). Ids without a
Wikidata item or without an English article are recorded explicitly.

Ethics and rate limits: descriptive User-Agent, sequential requests with
a pause, maxlag=5 (the API asks bots to back off when the servers are
lagging), retries honouring Retry-After. Every saved record has its
source URL and retrieval time (lineage). The script is resumable: ids
already present in the output files are skipped.

Usage (inside the imdb_spark container):
    python -m src.enrichment.collect_wikipedia
"""
import json
import time
from datetime import datetime, timezone
from urllib.parse import quote

import pandas as pd
import requests

from src.common.paths import DATA_DIR

ENRICHMENT_DIR = DATA_DIR / "enrichment"
RAW_DIR = ENRICHMENT_DIR / "raw"
LINKS_FILE = RAW_DIR / "wikidata_links.jsonl"
FILMS_FILE = RAW_DIR / "film_pages.jsonl"
PEOPLE_FILE = RAW_DIR / "person_intros.jsonl"

SPARQL_URL = "https://query.wikidata.org/sparql"
WIKI_API_URL = "https://en.wikipedia.org/w/api.php"
WIKI_PAGE_URL = "https://en.wikipedia.org/wiki/"
USER_AGENT = (
    "ECE-BigData-IMDb-Project/1.0 "
    "(https://github.com/Alexandre-50/Big_Data_IMDB_Project; "
    "student project) python-requests"
)
SPARQL_BATCH = 200
INTRO_BATCH = 20          # API limit for intro extracts per request
PAUSE_SECONDS = 0.2       # pause between two requests
MAX_RETRIES = 5

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})


def now():
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def page_url(title):
    """Return the public URL of an English Wikipedia article."""
    return WIKI_PAGE_URL + quote(title.replace(" ", "_"), safe="()_,'")


def http_get(url, params):
    """GET with retries on rate limiting, server errors and maxlag."""
    for attempt in range(MAX_RETRIES):
        try:
            response = SESSION.get(url, params=params, timeout=60)
        except requests.RequestException as error:
            wait = 2 ** attempt
            print(f"  network error ({error}), retry in {wait}s", flush=True)
            time.sleep(wait)
            continue
        if response.status_code in (429, 500, 502, 503, 504):
            wait = int(response.headers.get("Retry-After", 2 ** attempt))
            print(f"  HTTP {response.status_code}, retry in {wait}s",
                  flush=True)
            time.sleep(wait)
            continue
        response.raise_for_status()
        data = response.json()
        if data.get("error", {}).get("code") == "maxlag":
            wait = int(response.headers.get("Retry-After", 5))
            print(f"  servers lagging, retry in {wait}s", flush=True)
            time.sleep(wait)
            continue
        time.sleep(PAUSE_SECONDS)
        return data
    raise RuntimeError(f"Giving up on {url} after {MAX_RETRIES} attempts")


def done_ids(path, key):
    """Return the ids already saved in a JSON lines file."""
    if not path.exists():
        return set()
    with open(path, encoding="utf-8") as file:
        return {json.loads(line)[key] for line in file if line.strip()}


def append(path, records):
    """Append records to a JSON lines file."""
    with open(path, "a", encoding="utf-8") as file:
        for record in records:
            file.write(json.dumps(record, ensure_ascii=False) + "\n")


def link_batch(imdb_ids):
    """Resolve a batch of IMDb ids to Wikidata items and enwiki titles."""
    values = " ".join(f'"{imdb_id}"' for imdb_id in imdb_ids)
    query = f"""
        SELECT ?imdb ?item ?article WHERE {{
          VALUES ?imdb {{ {values} }}
          ?item wdt:P345 ?imdb .
          OPTIONAL {{
            ?article schema:about ?item ;
                     schema:isPartOf <https://en.wikipedia.org/> .
          }}
        }}"""
    data = http_get(SPARQL_URL, {"query": query, "format": "json"})
    found = {}
    for row in data["results"]["bindings"]:
        imdb_id = row["imdb"]["value"]
        article = row.get("article", {}).get("value")
        title = None
        if article:
            title = requests.utils.unquote(
                article.rsplit("/wiki/", 1)[1]).replace("_", " ")
        # Keep the row with an article when an id has several items.
        if imdb_id not in found or (title and not found[imdb_id]["title"]):
            found[imdb_id] = {"qid": row["item"]["value"].rsplit("/", 1)[1],
                              "title": title}
    retrieved = now()
    records = []
    for imdb_id in imdb_ids:
        match = found.get(imdb_id, {"qid": None, "title": None})
        records.append({
            "imdb_id": imdb_id,
            "wikidata_qid": match["qid"],
            "wikipedia_title": match["title"],
            "status": ("linked" if match["title"] else
                       "no_english_article" if match["qid"] else
                       "no_wikidata_item"),
            "source_url": (f"https://www.wikidata.org/wiki/{match['qid']}"
                           if match["qid"] else SPARQL_URL),
            "retrieved_at": retrieved,
        })
    return records


def collect_links(imdb_ids):
    """Step 1: link every IMDb id to its English Wikipedia article."""
    todo = sorted(set(imdb_ids) - done_ids(LINKS_FILE, "imdb_id"))
    print(f"Wikidata links: {len(todo)} ids to resolve", flush=True)
    for start in range(0, len(todo), SPARQL_BATCH):
        append(LINKS_FILE, link_batch(todo[start:start + SPARQL_BATCH]))
        print(f"  {min(start + SPARQL_BATCH, len(todo))}/{len(todo)}",
              flush=True)


def load_links():
    """Return {imdb_id: wikipedia_title or None} from the links file."""
    with open(LINKS_FILE, encoding="utf-8") as file:
        return {record["imdb_id"]: record["wikipedia_title"]
                for record in map(json.loads, file)}


def collect_films(film_ids, links):
    """Step 2: download the full plain-text article of every film."""
    todo = sorted(set(film_ids) - done_ids(FILMS_FILE, "imdb_id"))
    todo = [film for film in todo if links.get(film)]
    print(f"Film pages: {len(todo)} articles to download", flush=True)
    for count, imdb_id in enumerate(todo, start=1):
        title = links[imdb_id]
        data = http_get(WIKI_API_URL, {
            "action": "query", "format": "json", "formatversion": 2,
            "prop": "extracts", "explaintext": 1,
            "exsectionformat": "wiki", "redirects": 1, "maxlag": 5,
            "titles": title,
        })
        page = data["query"]["pages"][0]
        append(FILMS_FILE, [{
            "imdb_id": imdb_id,
            "wikipedia_title": page.get("title", title),
            "page_missing": bool(page.get("missing")),
            "text": page.get("extract"),
            "source_url": page_url(page.get("title", title)),
            "retrieved_at": now(),
        }])
        if count % 200 == 0:
            print(f"  {count}/{len(todo)}", flush=True)


def collect_people(person_ids, links):
    """Step 3: download the introduction of every person's article."""
    todo = sorted(set(person_ids) - done_ids(PEOPLE_FILE, "imdb_id"))
    todo = [person for person in todo if links.get(person)]
    print(f"Person intros: {len(todo)} articles to download", flush=True)
    for start in range(0, len(todo), INTRO_BATCH):
        batch = todo[start:start + INTRO_BATCH]
        data = http_get(WIKI_API_URL, {
            "action": "query", "format": "json", "formatversion": 2,
            "prop": "extracts", "exintro": 1, "explaintext": 1,
            "exlimit": INTRO_BATCH, "redirects": 1, "maxlag": 5,
            "titles": "|".join(links[person] for person in batch),
        })
        query = data["query"]
        # Follow the title changes made by the API (normalization and
        # redirects) to map each answer back to the requested title.
        renamed = {item["from"]: item["to"]
                   for key in ("normalized", "redirects")
                   for item in query.get(key, [])}
        pages = {page["title"]: page for page in query["pages"]}
        retrieved = now()
        records = []
        for person in batch:
            title = links[person]
            while title in renamed:
                title = renamed[title]
            page = pages.get(title, {"missing": True})
            records.append({
                "imdb_id": person,
                "wikipedia_title": title,
                "page_missing": bool(page.get("missing")),
                "text": page.get("extract"),
                "source_url": page_url(title),
                "retrieved_at": retrieved,
            })
        append(PEOPLE_FILE, records)
        print(f"  {min(start + INTRO_BATCH, len(todo))}/{len(todo)}",
              flush=True)


def main():
    """Run the three collection steps."""
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    films = pd.read_parquet(ENRICHMENT_DIR / "top_films")
    people = pd.read_parquet(ENRICHMENT_DIR / "film_principals")
    film_ids = films["tconst"].unique().tolist()
    person_ids = people["nconst"].unique().tolist()

    collect_links(film_ids + person_ids)
    links = load_links()
    collect_people(person_ids, links)
    collect_films(film_ids, links)
    print("Collection finished", flush=True)


if __name__ == "__main__":
    main()
