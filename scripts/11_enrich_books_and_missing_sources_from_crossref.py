from pathlib import Path
from datetime import datetime
import json
import time
import re

import duckdb
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_FILE = PROJECT_ROOT / "data" / "research_database.duckdb"

MAX_WORKS = None  # Set to None for full run after testing.
SLEEP_SECONDS = 0.2

CONTACT_EMAIL = "gobengo@gmail.com"
USER_AGENT = f"education-research-db/0.1 (mailto:{CONTACT_EMAIL})"

DROP_EXISTING_TABLES = False


def q(sql: str) -> str:
    return sql.strip()


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def get_year(message):
    for key in ["published-print", "published-online", "published", "issued", "created"]:
        obj = message.get(key)
        if not isinstance(obj, dict):
            continue

        date_parts = obj.get("date-parts")
        if isinstance(date_parts, list) and date_parts and date_parts[0]:
            return date_parts[0][0]

    return None


def first_list_value(value):
    if isinstance(value, list) and value:
        return value[0]
    return None


def join_list(value):
    if isinstance(value, list) and value:
        return "; ".join(str(x) for x in value if x is not None)
    return None


def get_crossref_record(doi_clean):
    url = f"https://api.crossref.org/works/{doi_clean}"

    response = requests.get(
        url,
        headers={"User-Agent": USER_AGENT},
        params={"mailto": CONTACT_EMAIL},
        timeout=60,
    )

    if response.status_code == 200:
        return response.status_code, response.json(), None

    return response.status_code, None, response.text[:2000]


def init_tables(con):
    if DROP_EXISTING_TABLES:
        for table in [
            "crossref_enrichment_raw",
            "crossref_enrichment_flat",
            "crossref_contributors",
            "crossref_subjects",
            "crossref_enrichment_scope",
        ]:
            con.execute(f"DROP TABLE IF EXISTS {table}")

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS crossref_enrichment_scope (
            work_id VARCHAR PRIMARY KEY,
            doi VARCHAR,
            doi_clean VARCHAR,
            title VARCHAR,
            work_type VARCHAR,
            derived_output_class VARCHAR,
            broad_output_family VARCHAR,
            is_missing_source BOOLEAN,
            is_book_or_chapter_like BOOLEAN,
            scope_reason VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS crossref_enrichment_raw (
            work_id VARCHAR,
            doi VARCHAR,
            doi_clean VARCHAR PRIMARY KEY,
            retrieved_at VARCHAR,
            response_status INTEGER,
            raw_json VARCHAR,
            error_message VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS crossref_enrichment_flat (
            work_id VARCHAR,
            doi VARCHAR,
            doi_clean VARCHAR PRIMARY KEY,

            crossref_type VARCHAR,
            crossref_title VARCHAR,
            container_title VARCHAR,
            publisher VARCHAR,
            member VARCHAR,

            published_year INTEGER,
            issn VARCHAR,
            issn_type_json VARCHAR,
            isbn VARCHAR,

            volume VARCHAR,
            issue VARCHAR,
            page VARCHAR,
            article_number VARCHAR,

            reference_count INTEGER,
            is_referenced_by_count INTEGER,

            license_json VARCHAR,
            link_json VARCHAR,

            author_count INTEGER,
            editor_count INTEGER,
            raw_has_author BOOLEAN,
            raw_has_editor BOOLEAN
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS crossref_contributors (
            doi_clean VARCHAR,
            contributor_role VARCHAR,
            sequence VARCHAR,
            given VARCHAR,
            family VARCHAR,
            name VARCHAR,
            affiliation_json VARCHAR,
            orcid VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS crossref_subjects (
            doi_clean VARCHAR,
            subject VARCHAR
        )
    """))


def build_scope(con):
    print("Building/refreshing Crossref enrichment scope...")

    con.execute("DELETE FROM crossref_enrichment_scope")

    con.execute(q("""
        INSERT INTO crossref_enrichment_scope
        SELECT
            work_id,
            doi,
            REGEXP_REPLACE(
                doi,
                '^https?://(dx\\.)?doi\\.org/',
                '',
                'i'
            ) AS doi_clean,
            title,
            work_type,
            derived_output_class,
            broad_output_family,
            is_missing_source,
            is_book_or_chapter_like,

            CASE
                WHEN is_missing_source = TRUE
                 AND is_book_or_chapter_like = TRUE
                    THEN 'missing_source_and_book_like'

                WHEN is_missing_source = TRUE
                    THEN 'missing_source'

                WHEN is_book_or_chapter_like = TRUE
                    THEN 'book_or_chapter_like'

                WHEN work_type IN ('book', 'book-chapter')
                    THEN 'book_work_type'

                WHEN derived_output_class IN (
                    'book',
                    'book_chapter',
                    'ebook_platform_record',
                    'book_series_record'
                )
                    THEN 'book_output_class'

                ELSE 'other'
            END AS scope_reason

        FROM work_output_tags
        WHERE doi IS NOT NULL
          AND doi != ''
          AND (
                is_missing_source = TRUE
             OR is_book_or_chapter_like = TRUE
             OR work_type IN ('book', 'book-chapter')
             OR derived_output_class IN (
                    'book',
                    'book_chapter',
                    'ebook_platform_record',
                    'book_series_record'
                )
          )
    """))


def flatten_record(con, work_id, doi, doi_clean, payload):
    msg = payload.get("message", {})

    crossref_title = first_list_value(msg.get("title"))
    container_title = first_list_value(msg.get("container-title"))
    publisher = msg.get("publisher")
    member = str(msg.get("member")) if msg.get("member") is not None else None
    crossref_type = msg.get("type")

    authors = msg.get("author")
    editors = msg.get("editor")

    con.execute("DELETE FROM crossref_enrichment_flat WHERE doi_clean = ?", [doi_clean])
    con.execute("DELETE FROM crossref_contributors WHERE doi_clean = ?", [doi_clean])
    con.execute("DELETE FROM crossref_subjects WHERE doi_clean = ?", [doi_clean])

    con.execute(q("""
        INSERT INTO crossref_enrichment_flat
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """), [
        work_id,
        doi,
        doi_clean,

        crossref_type,
        crossref_title,
        container_title,
        publisher,
        member,

        get_year(msg),
        join_list(msg.get("ISSN")),
        json.dumps(msg.get("issn-type"), ensure_ascii=False) if msg.get("issn-type") else None,
        join_list(msg.get("ISBN")),

        msg.get("volume"),
        msg.get("issue"),
        msg.get("page"),
        msg.get("article-number"),

        msg.get("reference-count"),
        msg.get("is-referenced-by-count"),

        json.dumps(msg.get("license"), ensure_ascii=False) if msg.get("license") else None,
        json.dumps(msg.get("link"), ensure_ascii=False) if msg.get("link") else None,

        len(authors) if isinstance(authors, list) else 0,
        len(editors) if isinstance(editors, list) else 0,
        isinstance(authors, list),
        isinstance(editors, list),
    ])

    contributor_rows = []

    for role, people in [("author", authors), ("editor", editors)]:
        if isinstance(people, list) and people:
            for person in people:
                if not isinstance(person, dict):
                    continue

                contributor_rows.append((
                    doi_clean,
                    role,
                    person.get("sequence"),
                    person.get("given"),
                    person.get("family"),
                    person.get("name"),
                    json.dumps(person.get("affiliation"), ensure_ascii=False) if person.get("affiliation") else None,
                    person.get("ORCID"),
                ))

    if contributor_rows:
        con.executemany(
            "INSERT INTO crossref_contributors VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            contributor_rows,
        )

    subjects = msg.get("subject")

    if isinstance(subjects, list) and subjects:
        con.executemany(
            "INSERT INTO crossref_subjects VALUES (?, ?)",
            [(doi_clean, subject) for subject in subjects],
        )


def main():
    con = duckdb.connect(str(DUCKDB_FILE))
    init_tables(con)
    build_scope(con)

    already_successful = {
        row[0]
        for row in con.execute(q("""
            SELECT doi_clean
            FROM crossref_enrichment_raw
            WHERE response_status = 200
        """)).fetchall()
    }

    candidates = [
        row for row in con.execute(q("""
            SELECT
                work_id,
                doi,
                doi_clean,
                title,
                work_type,
                scope_reason
            FROM crossref_enrichment_scope
            WHERE doi_clean IS NOT NULL
              AND doi_clean != ''
            ORDER BY scope_reason, work_type, work_id
        """)).fetchall()
        if row[2] not in already_successful
    ]

    if MAX_WORKS is not None:
        candidates = candidates[:MAX_WORKS]

    print(f"Crossref records already successful: {len(already_successful):,}")
    print(f"Crossref records to harvest this run: {len(candidates):,}")

    for i, (work_id, doi, doi_clean, title, work_type, scope_reason) in enumerate(candidates, start=1):
        print(f"[{i}/{len(candidates)}] {doi_clean} | {scope_reason} | {work_type} | {(title or '')[:70]}")

        try:
            status, payload, error = get_crossref_record(doi_clean)

            con.execute("DELETE FROM crossref_enrichment_raw WHERE doi_clean = ?", [doi_clean])

            con.execute(q("""
                INSERT INTO crossref_enrichment_raw
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
                flatten_record(con, work_id, doi, doi_clean, payload)
            else:
                print(f"  Non-200 response: {status}")

        except Exception as e:
            con.execute("DELETE FROM crossref_enrichment_raw WHERE doi_clean = ?", [doi_clean])
            con.execute(q("""
                INSERT INTO crossref_enrichment_raw
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
    print("Crossref enrichment run complete.")
    print()

    checks = [
        ("scope rows", "SELECT COUNT(*) FROM crossref_enrichment_scope"),
        ("raw rows", "SELECT COUNT(*) FROM crossref_enrichment_raw"),
        ("successful rows", "SELECT COUNT(*) FROM crossref_enrichment_raw WHERE response_status = 200"),
        ("flat rows", "SELECT COUNT(*) FROM crossref_enrichment_flat"),
        ("rows with container title", "SELECT COUNT(*) FROM crossref_enrichment_flat WHERE container_title IS NOT NULL"),
        ("rows with ISSN", "SELECT COUNT(*) FROM crossref_enrichment_flat WHERE issn IS NOT NULL"),
        ("rows with ISBN", "SELECT COUNT(*) FROM crossref_enrichment_flat WHERE isbn IS NOT NULL"),
        ("contributor rows", "SELECT COUNT(*) FROM crossref_contributors"),
        ("editor contributor rows", "SELECT COUNT(*) FROM crossref_contributors WHERE contributor_role = 'editor'"),
        ("subject rows", "SELECT COUNT(*) FROM crossref_subjects"),
    ]

    for label, sql in checks:
        print(f"{label}: {con.execute(q(sql)).fetchone()[0]:,}")

    print()
    print("Status breakdown:")
    for row in con.execute(q("""
        SELECT response_status, COUNT(*) AS n
        FROM crossref_enrichment_raw
        GROUP BY response_status
        ORDER BY n DESC
    """)).fetchall():
        print(row)

    print()
    print("Crossref type breakdown:")
    for row in con.execute(q("""
        SELECT crossref_type, COUNT(*) AS n
        FROM crossref_enrichment_flat
        GROUP BY crossref_type
        ORDER BY n DESC
        LIMIT 30
    """)).fetchall():
        print(row)

    con.close()


if __name__ == "__main__":
    main()