from pathlib import Path
from datetime import datetime
import json
import time
import re

import duckdb
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_FILE = PROJECT_ROOT / "data" / "research_database.duckdb"

MAX_WORKS = 100
SLEEP_SECONDS = 0.2

CONTACT_EMAIL = "gobengo@gmail.com"
USER_AGENT = f"education-research-db/0.1 (mailto:{CONTACT_EMAIL})"

DROP_EXISTING_TEST_TABLES = True


def q(sql: str) -> str:
    return sql.strip()


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def clean_doi(doi):
    if not doi:
        return None

    doi = str(doi).strip()
    doi = re.sub(r"^https?://(dx\\.)?doi\\.org/", "", doi, flags=re.I)
    doi = doi.strip()

    return doi or None


def first_list_value(value):
    if isinstance(value, list) and value:
        return value[0]
    return None


def get_crossref_record(doi):
    url = f"https://api.crossref.org/works/{doi}"

    response = requests.get(
        url,
        headers={"User-Agent": USER_AGENT},
        params={"mailto": CONTACT_EMAIL},
        timeout=60,
    )

    if response.status_code == 200:
        return response.status_code, response.json(), None

    return response.status_code, None, response.text[:2000]


def main():
    con = duckdb.connect(str(DUCKDB_FILE))

    if DROP_EXISTING_TEST_TABLES:
        con.execute("DROP TABLE IF EXISTS crossref_missing_source_test_raw")
        con.execute("DROP TABLE IF EXISTS crossref_missing_source_test_flat")

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS crossref_missing_source_test_raw (
            work_id VARCHAR,
            doi VARCHAR,
            doi_clean VARCHAR,
            retrieved_at VARCHAR,
            response_status INTEGER,
            raw_json VARCHAR,
            error_message VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS crossref_missing_source_test_flat (
            work_id VARCHAR,
            doi VARCHAR,
            doi_clean VARCHAR,
            crossref_type VARCHAR,
            crossref_title VARCHAR,
            container_title VARCHAR,
            publisher VARCHAR,
            published_year INTEGER,
            issn VARCHAR,
            isbn VARCHAR,
            subject_json VARCHAR,
            author_count INTEGER,
            editor_count INTEGER,
            raw_has_author BOOLEAN,
            raw_has_editor BOOLEAN
        )
    """))

    candidates = con.execute(q("""
        SELECT
            work_id,
            doi,
            title,
            work_type,
            publication_year
        FROM work_output_tags
        WHERE is_missing_source = TRUE
          AND doi IS NOT NULL
          AND doi != ''
        ORDER BY publication_year DESC NULLS LAST, work_id
        LIMIT ?
    """), [MAX_WORKS]).fetchall()

    print(f"Crossref missing-source test candidates: {len(candidates):,}")

    for i, (work_id, doi, title, work_type, publication_year) in enumerate(candidates, start=1):
        doi_clean = clean_doi(doi)

        print(f"[{i}/{len(candidates)}] {doi_clean} | {work_type} | {title[:80] if title else ''}")

        if not doi_clean:
            continue

        try:
            status, payload, error = get_crossref_record(doi_clean)

            con.execute(q("""
                INSERT INTO crossref_missing_source_test_raw
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """), [
                work_id,
                doi,
                doi_clean,
                now_iso(),
                status,
                json.dumps(payload, ensure_ascii=False) if payload else None,
                error,
            ])

            if status == 200 and payload:
                msg = payload.get("message", {})

                crossref_title = first_list_value(msg.get("title"))
                container_title = first_list_value(msg.get("container-title"))
                publisher = msg.get("publisher")
                crossref_type = msg.get("type")

                published = (
                    msg.get("published-print")
                    or msg.get("published-online")
                    or msg.get("published")
                    or {}
                )

                published_year = None
                date_parts = published.get("date-parts")
                if isinstance(date_parts, list) and date_parts and date_parts[0]:
                    published_year = date_parts[0][0]

                issn = "; ".join(msg.get("ISSN", [])) if isinstance(msg.get("ISSN"), list) else None
                isbn = "; ".join(msg.get("ISBN", [])) if isinstance(msg.get("ISBN"), list) else None

                authors = msg.get("author")
                editors = msg.get("editor")

                con.execute(q("""
                    INSERT INTO crossref_missing_source_test_flat
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """), [
                    work_id,
                    doi,
                    doi_clean,
                    crossref_type,
                    crossref_title,
                    container_title,
                    publisher,
                    published_year,
                    issn,
                    isbn,
                    json.dumps(msg.get("subject"), ensure_ascii=False) if msg.get("subject") else None,
                    len(authors) if isinstance(authors, list) else 0,
                    len(editors) if isinstance(editors, list) else 0,
                    isinstance(authors, list),
                    isinstance(editors, list),
                ])

        except Exception as e:
            con.execute(q("""
                INSERT INTO crossref_missing_source_test_raw
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """), [
                work_id,
                doi,
                doi_clean,
                now_iso(),
                -1,
                None,
                str(e),
            ])
            print(f"  ERROR: {e}")

        time.sleep(SLEEP_SECONDS)

    print()
    print("Crossref test complete.")
    print()

    checks = [
        ("raw rows", "SELECT COUNT(*) FROM crossref_missing_source_test_raw"),
        ("successful rows", "SELECT COUNT(*) FROM crossref_missing_source_test_raw WHERE response_status = 200"),
        ("flat rows", "SELECT COUNT(*) FROM crossref_missing_source_test_flat"),
        ("rows with container title", "SELECT COUNT(*) FROM crossref_missing_source_test_flat WHERE container_title IS NOT NULL"),
        ("rows with ISSN", "SELECT COUNT(*) FROM crossref_missing_source_test_flat WHERE issn IS NOT NULL"),
        ("rows with ISBN", "SELECT COUNT(*) FROM crossref_missing_source_test_flat WHERE isbn IS NOT NULL"),
        ("rows with editors", "SELECT COUNT(*) FROM crossref_missing_source_test_flat WHERE editor_count > 0"),
    ]

    for label, sql in checks:
        print(f"{label}: {con.execute(sql).fetchone()[0]:,}")

    print()
    print("Status breakdown:")
    for row in con.execute(q("""
        SELECT response_status, COUNT(*) AS n
        FROM crossref_missing_source_test_raw
        GROUP BY response_status
        ORDER BY n DESC
    """)).fetchall():
        print(row)

    print()
    print("Sample Crossref enrichments:")
    rows = con.execute(q("""
        SELECT
            crossref_type,
            container_title,
            publisher,
            issn,
            isbn,
            author_count,
            editor_count
        FROM crossref_missing_source_test_flat
        LIMIT 30
    """)).fetchall()

    for row in rows:
        print(row)

    con.close()


if __name__ == "__main__":
    main()