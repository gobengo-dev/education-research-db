from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import re
import time
import threading

import duckdb
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_FILE = PROJECT_ROOT / "data" / "research_database.duckdb"

MAX_WORKS = None          # Use 500 for testing; None for full run.
MAX_WORKERS = 6           # Polite parallelism. Try 6 first; 8 is probably okay.
BATCH_SIZE = 100          # Write results in batches.
REQUEST_TIMEOUT = 60
MIN_REQUEST_INTERVAL = 0.12  # Global throttle: about 8.3 request starts/sec.
RETRY_NON_200 = False
DROP_EXISTING_TABLES = False

CONTACT_EMAIL = "gobengo@gmail.com"
USER_AGENT = f"education-research-db/0.1 (mailto:{CONTACT_EMAIL})"


thread_local = threading.local()
last_request_time = 0.0
rate_lock = threading.Lock()


def q(sql: str) -> str:
    return sql.strip()


def now_iso():
    return datetime.now().isoformat(timespec="seconds")




def wait_for_rate_limit():
    """Globally throttle request starts across all worker threads.

    This keeps concurrency for network latency, but prevents bursts that
    trigger Crossref 429 rate-limit responses.
    """
    global last_request_time

    with rate_lock:
        now = time.time()
        elapsed = now - last_request_time
        wait_time = MIN_REQUEST_INTERVAL - elapsed

        if wait_time > 0:
            time.sleep(wait_time)

        last_request_time = time.time()

def get_session():
    if not hasattr(thread_local, "session"):
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        thread_local.session = session
    return thread_local.session


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


def fetch_crossref(candidate):
    work_id, doi, doi_clean, title, work_type, scope_reason = candidate

    url = f"https://api.crossref.org/works/{doi_clean}"
    retrieved_at = now_iso()

    try:
        session = get_session()

        wait_for_rate_limit()

        response = session.get(
            url,
            params={"mailto": CONTACT_EMAIL},
            timeout=REQUEST_TIMEOUT,
        )

        if response.status_code == 200:
            payload = response.json()
            return {
                "candidate": candidate,
                "retrieved_at": retrieved_at,
                "response_status": 200,
                "payload": payload,
                "error_message": None,
            }

        return {
            "candidate": candidate,
            "retrieved_at": retrieved_at,
            "response_status": response.status_code,
            "payload": None,
            "error_message": response.text[:2000],
        }

    except Exception as e:
        return {
            "candidate": candidate,
            "retrieved_at": retrieved_at,
            "response_status": -1,
            "payload": None,
            "error_message": str(e),
        }


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
    print("Building/refreshing all-DOI Crossref enrichment scope...")

    con.execute("DELETE FROM crossref_enrichment_scope")

    con.execute(q("""
        INSERT INTO crossref_enrichment_scope
        WITH doi_scope AS (
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
                        THEN 'all_dois_missing_source_and_book_like'

                    WHEN is_missing_source = TRUE
                        THEN 'all_dois_missing_source'

                    WHEN is_book_or_chapter_like = TRUE
                        THEN 'all_dois_book_or_chapter_like'

                    WHEN broad_output_family = 'core_scholarly'
                        THEN 'all_dois_core_scholarly'

                    WHEN broad_output_family = 'repository_or_open_output'
                        THEN 'all_dois_repository_or_open_output'

                    WHEN broad_output_family = 'grey_literature'
                        THEN 'all_dois_grey_literature'

                    WHEN broad_output_family = 'scholarly_service_or_minor_output'
                        THEN 'all_dois_minor_or_service_output'

                    ELSE 'all_dois_other'
                END AS scope_reason,

                ROW_NUMBER() OVER (
                    PARTITION BY REGEXP_REPLACE(
                        doi,
                        '^https?://(dx\\.)?doi\\.org/',
                        '',
                        'i'
                    )
                    ORDER BY
                        CASE WHEN is_missing_source THEN 0 ELSE 1 END,
                        CASE WHEN is_book_or_chapter_like THEN 0 ELSE 1 END,
                        publication_year DESC NULLS LAST,
                        work_id
                ) AS doi_rank

            FROM work_output_tags
            WHERE doi IS NOT NULL
              AND doi != ''
        )

        SELECT
            work_id,
            doi,
            doi_clean,
            title,
            work_type,
            derived_output_class,
            broad_output_family,
            is_missing_source,
            is_book_or_chapter_like,
            scope_reason
        FROM doi_scope
        WHERE doi_rank = 1
          AND doi_clean IS NOT NULL
          AND doi_clean != ''
    """))


def flatten_payload(result):
    work_id, doi, doi_clean, title, work_type, scope_reason = result["candidate"]
    payload = result["payload"]

    if result["response_status"] != 200 or not payload:
        return None, [], []

    msg = payload.get("message", {})

    authors = msg.get("author")
    editors = msg.get("editor")

    flat_row = (
        work_id,
        doi,
        doi_clean,

        msg.get("type"),
        first_list_value(msg.get("title")),
        first_list_value(msg.get("container-title")),
        msg.get("publisher"),
        str(msg.get("member")) if msg.get("member") is not None else None,

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
    )

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

    subject_rows = []
    subjects = msg.get("subject")

    if isinstance(subjects, list) and subjects:
        subject_rows = [(doi_clean, subject) for subject in subjects]

    return flat_row, contributor_rows, subject_rows


def write_batch(con, results):
    if not results:
        return

    raw_rows = []
    flat_rows = []
    contributor_rows = []
    subject_rows = []
    doi_cleans = []

    for result in results:
        work_id, doi, doi_clean, title, work_type, scope_reason = result["candidate"]
        doi_cleans.append(doi_clean)

        raw_rows.append((
            work_id,
            doi,
            doi_clean,
            result["retrieved_at"],
            result["response_status"],
            json.dumps(result["payload"], ensure_ascii=False) if result["payload"] else None,
            result["error_message"],
        ))

        flat_row, contributors, subjects = flatten_payload(result)

        if flat_row:
            flat_rows.append(flat_row)
            contributor_rows.extend(contributors)
            subject_rows.extend(subjects)

    con.execute("BEGIN TRANSACTION")

    try:
        for doi_clean in doi_cleans:
            con.execute("DELETE FROM crossref_enrichment_raw WHERE doi_clean = ?", [doi_clean])
            con.execute("DELETE FROM crossref_enrichment_flat WHERE doi_clean = ?", [doi_clean])
            con.execute("DELETE FROM crossref_contributors WHERE doi_clean = ?", [doi_clean])
            con.execute("DELETE FROM crossref_subjects WHERE doi_clean = ?", [doi_clean])

        con.executemany(
            "INSERT INTO crossref_enrichment_raw VALUES (?, ?, ?, ?, ?, ?, ?)",
            raw_rows,
        )

        if flat_rows:
            con.executemany(
                """
                INSERT INTO crossref_enrichment_flat
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                flat_rows,
            )

        if contributor_rows:
            con.executemany(
                "INSERT INTO crossref_contributors VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                contributor_rows,
            )

        if subject_rows:
            con.executemany(
                "INSERT INTO crossref_subjects VALUES (?, ?)",
                subject_rows,
            )

        con.execute("COMMIT")

    except Exception:
        con.execute("ROLLBACK")
        raise


def print_summary(con):
    print()
    print("All-DOI Crossref enrichment run complete.")
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
        LIMIT 40
    """)).fetchall():
        print(row)

    print()
    print("Remaining not attempted:")
    for row in con.execute(q("""
        SELECT
            scope_reason,
            COUNT(*) AS n
        FROM crossref_enrichment_scope s
        LEFT JOIN crossref_enrichment_raw r
            ON s.doi_clean = r.doi_clean
        WHERE r.doi_clean IS NULL
        GROUP BY scope_reason
        ORDER BY n DESC
    """)).fetchall():
        print(row)


def main():
    con = duckdb.connect(str(DUCKDB_FILE))
    init_tables(con)
    build_scope(con)

    if RETRY_NON_200:
        already_done = {
            row[0]
            for row in con.execute(q("""
                SELECT doi_clean
                FROM crossref_enrichment_raw
                WHERE response_status = 200
            """)).fetchall()
        }
    else:
        already_done = {
            row[0]
            for row in con.execute(q("""
                SELECT doi_clean
                FROM crossref_enrichment_raw
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
            ORDER BY
                CASE
                    WHEN scope_reason LIKE '%missing_source%' THEN 0
                    WHEN scope_reason LIKE '%book_or_chapter%' THEN 1
                    WHEN scope_reason LIKE '%core_scholarly%' THEN 2
                    ELSE 3
                END,
                work_type,
                work_id
        """)).fetchall()
        if row[2] not in already_done
    ]

    if MAX_WORKS is not None:
        candidates = candidates[:MAX_WORKS]

    print(f"Crossref DOI scope rows: {con.execute('SELECT COUNT(*) FROM crossref_enrichment_scope').fetchone()[0]:,}")
    print(f"Crossref records already skipped: {len(already_done):,}")
    print(f"Crossref records to harvest this run: {len(candidates):,}")
    print(f"RETRY_NON_200: {RETRY_NON_200}")
    print(f"MAX_WORKERS: {MAX_WORKERS}")
    print(f"BATCH_SIZE: {BATCH_SIZE}")
    print(f"MIN_REQUEST_INTERVAL: {MIN_REQUEST_INTERVAL}")
    print()

    completed = 0
    batch = []
    started = time.time()

    try:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_candidate = {
                executor.submit(fetch_crossref, candidate): candidate
                for candidate in candidates
            }

            for future in as_completed(future_to_candidate):
                result = future.result()
                batch.append(result)
                completed += 1

                work_id, doi, doi_clean, title, work_type, scope_reason = result["candidate"]
                status = result["response_status"]

                if completed % 25 == 0 or status != 200:
                    elapsed = time.time() - started
                    rate = completed / elapsed if elapsed else 0
                    remaining = len(candidates) - completed
                    eta_hours = remaining / rate / 3600 if rate else 0

                    print(
                        f"[{completed}/{len(candidates)}] "
                        f"status={status} "
                        f"rate={rate:.2f}/sec "
                        f"ETA={eta_hours:.2f}h "
                        f"{doi_clean} | {(title or '')[:60]}"
                    )

                if len(batch) >= BATCH_SIZE:
                    write_batch(con, batch)
                    batch = []

        if batch:
            write_batch(con, batch)

    except KeyboardInterrupt:
        print()
        print("Interrupted by user. Writing completed in-memory batch before exit...")
        if batch:
            write_batch(con, batch)
        print_summary(con)
        con.close()
        raise

    print_summary(con)
    con.close()


if __name__ == "__main__":
    main()