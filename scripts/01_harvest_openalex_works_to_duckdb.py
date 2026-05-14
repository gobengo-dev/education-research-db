import csv
import json
import time
import hashlib
import unicodedata
from pathlib import Path
from datetime import datetime

import requests
import duckdb

# =========================
# CONFIG
# =========================

API_KEY = ""  # paste NEW OpenAlex API key here if using one

CURATED_TOPICS_FILE = "OpenAlex_topic_selection.csv"
DUCKDB_FILE = "research_database.duckdb"

RAW_JSONL_FILE = "raw_openalex_works.jsonl"
HARVEST_LOG_FILE = "openalex_harvest_log.csv"

COUNTRY_CODES = ["AU", "NZ"]
MIN_PUBLICATION_YEAR = 2016

RESET_DATABASE_ON_START = True

REQUEST_SLEEP_SECONDS = 1.0
MAX_RETRIES = 7
PER_PAGE = 100

# Use 1 or 2 for testing. Set to None for full harvest.
MAX_PAGES_PER_TOPIC = None

WORKS_URL = "https://api.openalex.org/works"

WORK_SELECT_FIELDS = [
    "id",
    "doi",
    "display_name",
    "title",
    "publication_year",
    "publication_date",
    "type",
    "type_crossref",
    "cited_by_count",
    "citation_normalized_percentile",
    "fwci",
    "authorships",
    "primary_topic",
    "topics",
    "keywords",
    "concepts",
    "primary_location",
    "locations",
    "best_oa_location",
    "open_access",
    "language",
    "abstract_inverted_index",
    "referenced_works",
    "related_works",
    "counts_by_year",
    "funders",
    "awards",
    "created_date",
    "updated_date",
]


# =========================
# HELPERS
# =========================

def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def clean(value):
    if value is None:
        return ""
    value = unicodedata.normalize("NFC", str(value))
    return value.strip()


def short_openalex_id(value, prefix=None):
    value = clean(value)
    if not value:
        return ""

    if "/" in value:
        value = value.rsplit("/", 1)[-1]

    if prefix and value.isdigit():
        return f"{prefix}{value}"

    return value


def add_api_key(params):
    params = dict(params)
    if API_KEY:
        params["api_key"] = API_KEY
    return params


def get_json_with_retries(url, params):
    params = add_api_key(params)

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=120)

            if response.status_code == 400:
                print("\n400 Bad Request")
                print("URL:")
                print(response.url)
                print("Response text:")
                print(response.text[:2000])
                raise RuntimeError("400 Bad Request - check filter/select fields")

            if response.status_code == 403:
                print("\n403 Forbidden")
                print("URL:")
                print(response.url)
                print("Response text:")
                print(response.text[:2000])
                raise RuntimeError("403 Forbidden - check API key/access")

            if response.status_code in (429, 500, 502, 503, 504):
                wait = min(300, 10 * (2 ** attempt))
                print(f"HTTP {response.status_code}; waiting {wait}s...")
                time.sleep(wait)
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            wait = min(300, 10 * (2 ** attempt))
            print(f"Request failed: {e}")
            print(f"Waiting {wait}s...")
            time.sleep(wait)

    raise RuntimeError(f"Failed after {MAX_RETRIES} retries")


def stable_hash(text):
    return hashlib.sha256(str(text).encode("utf-8")).hexdigest()


def write_log_row(row):
    file_exists = Path(HARVEST_LOG_FILE).exists()

    with open(HARVEST_LOG_FILE, "a", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "timestamp",
            "topic_id",
            "topic_display_name",
            "topic_tier",
            "page",
            "works_returned",
            "cursor_hash",
            "status",
            "message",
        ])

        if not file_exists:
            writer.writeheader()

        writer.writerow(row)


def load_curated_topics():
    with open(CURATED_TOPICS_FILE, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    selected = []

    for row in rows:
        include_decision = clean(row.get("include_decision")).lower()
        tier = clean(row.get("education_relevance_tier")).lower()

        # Your file uses:
        # include_decision = Y/N
        # education_relevance_tier = core/adjacent/exclude
        should_include = (
            include_decision in {"y", "yes", "include", "included", "true", "1"}
            or tier in {"core", "adjacent", "strong_adjacent"}
        )

        if not should_include:
            continue

        raw_topic_id = clean(row.get("topic_id") or row.get("id"))

        if not raw_topic_id:
            continue

        topic_id = short_openalex_id(raw_topic_id, prefix="T")

        selected.append({
            "topic_id": topic_id,
            "topic_display_name": clean(row.get("topic_name") or row.get("display_name")),
            "education_relevance_tier": tier,
            "include_decision": include_decision,
            "topic_family": clean(row.get("topic_family")),
            "school_phase": clean(row.get("school_phase")),
        })

    seen = set()
    deduped = []

    for topic in selected:
        if topic["topic_id"] not in seen:
            deduped.append(topic)
            seen.add(topic["topic_id"])

    return deduped


def init_duckdb():
    con = duckdb.connect(DUCKDB_FILE)

    con.execute("""
        CREATE TABLE IF NOT EXISTS curated_topics (
            topic_id VARCHAR PRIMARY KEY,
            topic_display_name VARCHAR,
            education_relevance_tier VARCHAR,
            include_decision VARCHAR,
            topic_family VARCHAR,
            school_phase VARCHAR,
            loaded_at VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS raw_works (
            work_id VARCHAR PRIMARY KEY,
            raw_json VARCHAR,
            harvested_at VARCHAR,
            first_source_topic_id VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS harvest_seen_work_topics (
            work_id VARCHAR,
            source_topic_id VARCHAR,
            source_topic_display_name VARCHAR,
            education_relevance_tier VARCHAR,
            harvested_at VARCHAR,
            PRIMARY KEY (work_id, source_topic_id)
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS works_flat (
            work_id VARCHAR PRIMARY KEY,
            doi VARCHAR,
            title VARCHAR,
            publication_year INTEGER,
            publication_date VARCHAR,
            work_type VARCHAR,
            type_crossref VARCHAR,
            cited_by_count INTEGER,
            fwci DOUBLE,
            citation_percentile DOUBLE,
            is_in_top_1_percent BOOLEAN,
            is_in_top_10_percent BOOLEAN,
            primary_topic_id VARCHAR,
            primary_topic_name VARCHAR,
            language VARCHAR,
            updated_date VARCHAR,
            created_date VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS authorships (
            work_id VARCHAR,
            author_order INTEGER,
            author_position VARCHAR,
            author_id VARCHAR,
            author_name VARCHAR,
            author_orcid VARCHAR,
            is_corresponding BOOLEAN,
            institutions_json VARCHAR,
            countries VARCHAR,
            is_au_nz_affiliated BOOLEAN
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS work_topics (
            work_id VARCHAR,
            topic_id VARCHAR,
            topic_name VARCHAR,
            subfield_id VARCHAR,
            subfield_name VARCHAR,
            field_id VARCHAR,
            field_name VARCHAR,
            domain_id VARCHAR,
            domain_name VARCHAR,
            topic_score DOUBLE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS work_keywords (
            work_id VARCHAR,
            keyword_id VARCHAR,
            keyword VARCHAR,
            score DOUBLE
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS work_sources (
            work_id VARCHAR PRIMARY KEY,
            source_id VARCHAR,
            source_name VARCHAR,
            source_type VARCHAR,
            publisher VARCHAR,
            issn_l VARCHAR,
            issn VARCHAR,
            is_oa BOOLEAN,
            oa_status VARCHAR,
            landing_page_url VARCHAR,
            pdf_url VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS work_funders (
            work_id VARCHAR,
            funder_id VARCHAR,
            funder_name VARCHAR
        )
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS work_awards (
            work_id VARCHAR,
            award_id VARCHAR,
            funder_id VARCHAR,
            funder_name VARCHAR
        )
    """)

    con.close()


def upsert_curated_topics(topics):
    con = duckdb.connect(DUCKDB_FILE)

    for topic in topics:
        con.execute("""
            INSERT OR REPLACE INTO curated_topics VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [
            topic["topic_id"],
            topic["topic_display_name"],
            topic["education_relevance_tier"],
            topic["include_decision"],
            topic["topic_family"],
            topic["school_phase"],
            now_iso(),
        ])

    con.close()


def append_raw_jsonl_once(work):
    raw_json = json.dumps(work, ensure_ascii=False)

    with open(RAW_JSONL_FILE, "a", encoding="utf-8") as f:
        f.write(raw_json + "\n")


def insert_work(work, source_topic):
    work_id = work.get("id")
    if not work_id:
        return

    harvested_at = now_iso()
    raw_json = json.dumps(work, ensure_ascii=False)

    primary_topic = work.get("primary_topic") or {}
    citation_pct = work.get("citation_normalized_percentile") or {}

    con = duckdb.connect(DUCKDB_FILE)

    already_seen = con.execute(
        "SELECT COUNT(*) FROM raw_works WHERE work_id = ?",
        [work_id]
    ).fetchone()[0] > 0

    con.execute("""
        INSERT OR IGNORE INTO raw_works VALUES (?, ?, ?, ?)
    """, [
        work_id,
        raw_json,
        harvested_at,
        source_topic["topic_id"],
    ])

    con.execute("""
        INSERT OR IGNORE INTO harvest_seen_work_topics VALUES (?, ?, ?, ?, ?)
    """, [
        work_id,
        source_topic["topic_id"],
        source_topic["topic_display_name"],
        source_topic["education_relevance_tier"],
        harvested_at,
    ])

    con.execute("""
        INSERT OR REPLACE INTO works_flat VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        work_id,
        work.get("doi"),
        work.get("display_name") or work.get("title"),
        work.get("publication_year"),
        work.get("publication_date"),
        work.get("type"),
        work.get("type_crossref"),
        work.get("cited_by_count"),
        work.get("fwci"),
        citation_pct.get("value") if isinstance(citation_pct, dict) else None,
        citation_pct.get("is_in_top_1_percent") if isinstance(citation_pct, dict) else None,
        citation_pct.get("is_in_top_10_percent") if isinstance(citation_pct, dict) else None,
        primary_topic.get("id"),
        primary_topic.get("display_name"),
        work.get("language"),
        work.get("updated_date"),
        work.get("created_date"),
    ])

    # Authorships
    con.execute("DELETE FROM authorships WHERE work_id = ?", [work_id])

    for idx, authorship in enumerate(work.get("authorships") or [], start=1):
        author = authorship.get("author") or {}
        institutions = authorship.get("institutions") or []

        countries = sorted({
            inst.get("country_code")
            for inst in institutions
            if inst.get("country_code")
        })

        is_au_nz = any(c in COUNTRY_CODES for c in countries)

        con.execute("""
            INSERT INTO authorships VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            work_id,
            idx,
            authorship.get("author_position"),
            author.get("id"),
            author.get("display_name"),
            author.get("orcid"),
            authorship.get("is_corresponding"),
            json.dumps(institutions, ensure_ascii=False),
            "; ".join(countries),
            is_au_nz,
        ])

    # Topics
    con.execute("DELETE FROM work_topics WHERE work_id = ?", [work_id])

    for topic in work.get("topics") or []:
        subfield = topic.get("subfield") or {}
        field = topic.get("field") or {}
        domain = topic.get("domain") or {}

        con.execute("""
            INSERT INTO work_topics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            work_id,
            topic.get("id"),
            topic.get("display_name"),
            subfield.get("id"),
            subfield.get("display_name"),
            field.get("id"),
            field.get("display_name"),
            domain.get("id"),
            domain.get("display_name"),
            topic.get("score"),
        ])

    # Keywords
    con.execute("DELETE FROM work_keywords WHERE work_id = ?", [work_id])

    for kw in work.get("keywords") or []:
        con.execute("""
            INSERT INTO work_keywords VALUES (?, ?, ?, ?)
        """, [
            work_id,
            kw.get("id"),
            kw.get("display_name"),
            kw.get("score"),
        ])

    # Source / journal
    primary_location = work.get("primary_location") or {}
    source = primary_location.get("source") or {}
    open_access = work.get("open_access") or {}

    con.execute("""
        INSERT OR REPLACE INTO work_sources VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        work_id,
        source.get("id"),
        source.get("display_name"),
        source.get("type"),
        source.get("publisher"),
        source.get("issn_l"),
        "; ".join(source.get("issn") or []),
        open_access.get("is_oa") if isinstance(open_access, dict) else None,
        open_access.get("oa_status") if isinstance(open_access, dict) else None,
        primary_location.get("landing_page_url"),
        primary_location.get("pdf_url"),
    ])

    # Funders
    con.execute("DELETE FROM work_funders WHERE work_id = ?", [work_id])
    for funder in work.get("funders") or []:
        con.execute("""
            INSERT INTO work_funders VALUES (?, ?, ?)
        """, [
            work_id,
            funder.get("id"),
            funder.get("display_name"),
        ])

    # Awards
    con.execute("DELETE FROM work_awards WHERE work_id = ?", [work_id])
    for award in work.get("awards") or []:
        funder = award.get("funder") or {}
        con.execute("""
            INSERT INTO work_awards VALUES (?, ?, ?, ?)
        """, [
            work_id,
            award.get("id"),
            funder.get("id"),
            funder.get("display_name"),
        ])

    con.close()

    if not already_seen:
        append_raw_jsonl_once(work)


# =========================
# MAIN HARVEST
# =========================

def main():
    if RESET_DATABASE_ON_START:
        for path in [DUCKDB_FILE, RAW_JSONL_FILE, HARVEST_LOG_FILE]:
            p = Path(path)
            if p.exists():
                p.unlink()
                print(f"Deleted old file: {path}")

    init_duckdb()

    topics = load_curated_topics()
    upsert_curated_topics(topics)

    print(f"Loaded {len(topics)} curated included topics.")
    print(f"DuckDB: {DUCKDB_FILE}")
    print(f"Raw JSONL: {RAW_JSONL_FILE}")
    print(f"MAX_PAGES_PER_TOPIC: {MAX_PAGES_PER_TOPIC}")

    if not topics:
        print("No topics loaded. Check OpenAlex_topic_selection.csv column names and include values.")
        return

    for topic_index, topic in enumerate(topics, start=1):
        print()
        print(f"[{topic_index}/{len(topics)}] {topic['topic_display_name']} ({topic['topic_id']})")

        cursor = "*"
        page = 0

        while True:
            page += 1

            if MAX_PAGES_PER_TOPIC is not None and page > MAX_PAGES_PER_TOPIC:
                print(f"  page cap reached: {MAX_PAGES_PER_TOPIC}")
                break

            params = {
                "filter": (
                    f"topics.id:{topic['topic_id']},"
                    f"authorships.institutions.country_code:{'|'.join(COUNTRY_CODES)},"
                    f"from_publication_date:{MIN_PUBLICATION_YEAR}-01-01"
                ),
                "per_page": PER_PAGE,
                "cursor": cursor,
                "select": ",".join(WORK_SELECT_FIELDS),
            }

            try:
                data = get_json_with_retries(WORKS_URL, params)
            except Exception as e:
                print(f"  ERROR: {e}")
                write_log_row({
                    "timestamp": now_iso(),
                    "topic_id": topic["topic_id"],
                    "topic_display_name": topic["topic_display_name"],
                    "topic_tier": topic["education_relevance_tier"],
                    "page": page,
                    "works_returned": 0,
                    "cursor_hash": stable_hash(cursor),
                    "status": "ERROR",
                    "message": str(e),
                })
                break

            works = data.get("results") or []
            print(f"  page {page}: {len(works)} works")

            write_log_row({
                "timestamp": now_iso(),
                "topic_id": topic["topic_id"],
                "topic_display_name": topic["topic_display_name"],
                "topic_tier": topic["education_relevance_tier"],
                "page": page,
                "works_returned": len(works),
                "cursor_hash": stable_hash(cursor),
                "status": "OK",
                "message": "",
            })

            if not works:
                break

            for work in works:
                insert_work(work, topic)

            next_cursor = data.get("meta", {}).get("next_cursor")
            if not next_cursor:
                break

            cursor = next_cursor
            time.sleep(REQUEST_SLEEP_SECONDS)

    print()
    print("Harvest complete.")
    print(f"Database saved to: {DUCKDB_FILE}")


if __name__ == "__main__":
    main()