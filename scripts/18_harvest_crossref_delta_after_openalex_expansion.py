from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import time
import threading
import os

import duckdb
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_FILE = PROJECT_ROOT / "data" / "research_database.duckdb"
EXPORT_DIR = PROJECT_ROOT / "data" / "exports" / "crossref_delta"

# Use 500 for testing; None for full run.
MAX_WORKS = None

MAX_WORKERS = 6
BATCH_SIZE = 100
REQUEST_TIMEOUT = 60

# Global throttle across all worker threads.
# 0.12 = about 8.3 request starts/second.
MIN_REQUEST_INTERVAL = 0.12

# False = skip every DOI already attempted in crossref_enrichment_raw.
# True = retry previous non-200 records too.
RETRY_NON_200 = False

CONTACT_EMAIL = os.environ.get("CROSSREF_EMAIL", os.environ.get("OPENALEX_EMAIL", "gobengo@gmail.com")).strip()
USER_AGENT = f"education-research-db/0.2 (mailto:{CONTACT_EMAIL})"

HARVEST_RUN_ID = f"hr_crossref_delta_openalex_expansion_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
HARVEST_SOURCE_ID = "hs_crossref_api"

thread_local = threading.local()
last_request_time = 0.0
rate_lock = threading.Lock()


def q(sql: str) -> str:
    return sql.strip()


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def table_exists(con, table_name: str) -> bool:
    return con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchone()[0] > 0


def count_rows(con, table_name: str) -> int:
    if not table_exists(con, table_name):
        return 0
    return con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def wait_for_rate_limit():
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


def first_list_value(value):
    if isinstance(value, list) and value:
        return value[0]
    return None


def join_list(value):
    if isinstance(value, list) and value:
        return "; ".join(str(x) for x in value if x is not None)
    return None


def get_year(message):
    for key in ["published-print", "published-online", "published", "issued", "created"]:
        obj = message.get(key)
        if not isinstance(obj, dict):
            continue

        date_parts = obj.get("date-parts")
        if isinstance(date_parts, list) and date_parts and date_parts[0]:
            return date_parts[0][0]

    return None


def init_tables(con):
    # Existing Crossref tables. Do not drop anything.
    con.execute(q("""
        CREATE TABLE IF NOT EXISTS crossref_delta_enrichment_scope (
            work_id VARCHAR,
            doi VARCHAR,
            doi_clean VARCHAR PRIMARY KEY,
            title VARCHAR,
            work_type VARCHAR,
            derived_output_class VARCHAR,
            broad_output_family VARCHAR,
            is_missing_source BOOLEAN,
            is_book_or_chapter_like BOOLEAN,
            source_name VARCHAR,
            source_type VARCHAR,
            scope_reason VARCHAR,
            built_at VARCHAR
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

    # Provenance tables. These should already exist from script 15, but create
    # defensively so this script fails less mysteriously on a fresh clone.
    con.execute(q("""
        CREATE TABLE IF NOT EXISTS harvest_runs (
            harvest_run_id VARCHAR PRIMARY KEY,
            source_system VARCHAR,
            source_subtype VARCHAR,
            script_name VARCHAR,
            script_version VARCHAR,
            started_at VARCHAR,
            completed_at VARCHAR,
            status VARCHAR,
            parameters_json VARCHAR,
            notes VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS harvest_sources (
            harvest_source_id VARCHAR PRIMARY KEY,
            source_system VARCHAR,
            source_name VARCHAR,
            source_url VARCHAR,
            source_identifier_type VARCHAR,
            source_identifier_value VARCHAR,
            access_method VARCHAR,
            notes VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS raw_source_records (
            raw_record_id VARCHAR PRIMARY KEY,
            harvest_run_id VARCHAR,
            harvest_source_id VARCHAR,
            source_system VARCHAR,
            source_record_id VARCHAR,
            source_record_type VARCHAR,
            raw_table_name VARCHAR,
            raw_table_key VARCHAR,
            retrieved_at VARCHAR,
            response_status INTEGER,
            content_hash VARCHAR,
            error_message VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS source_work_claims (
            claim_id VARCHAR PRIMARY KEY,
            source_system VARCHAR,
            harvest_run_id VARCHAR,
            raw_record_id VARCHAR,

            source_work_id VARCHAR,
            canonical_work_id VARCHAR,

            doi VARCHAR,
            title VARCHAR,
            publication_year INTEGER,
            publication_date VARCHAR,
            work_type VARCHAR,
            container_title VARCHAR,
            source_name VARCHAR,
            publisher VARCHAR,

            claim_confidence VARCHAR,
            match_method VARCHAR,
            created_at VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS work_identifier_claims (
            claim_id VARCHAR PRIMARY KEY,
            canonical_work_id VARCHAR,
            source_system VARCHAR,
            harvest_run_id VARCHAR,
            raw_record_id VARCHAR,

            identifier_type VARCHAR,
            identifier_value VARCHAR,
            identifier_normalised VARCHAR,

            claim_confidence VARCHAR,
            match_method VARCHAR,
            created_at VARCHAR
        )
    """))


def seed_harvest_run(con):
    started_at = now_iso()

    con.execute(
        """
        INSERT OR REPLACE INTO harvest_sources
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            HARVEST_SOURCE_ID,
            "crossref",
            "Crossref REST API",
            "https://api.crossref.org",
            "api",
            "crossref",
            "api",
            "Crossref API used for DOI delta enrichment after OpenAlex expansion harvest.",
        ),
    )

    params = {
        "max_works": MAX_WORKS,
        "max_workers": MAX_WORKERS,
        "batch_size": BATCH_SIZE,
        "request_timeout": REQUEST_TIMEOUT,
        "min_request_interval": MIN_REQUEST_INTERVAL,
        "retry_non_200": RETRY_NON_200,
        "contact_email_present": bool(CONTACT_EMAIL),
    }

    con.execute(
        """
        INSERT OR REPLACE INTO harvest_runs
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            HARVEST_RUN_ID,
            "crossref",
            "doi_delta_after_openalex_expansion",
            "18_harvest_crossref_delta_after_openalex_expansion.py",
            "v1",
            started_at,
            None,
            "running",
            json.dumps(params, ensure_ascii=False),
            "Harvests Crossref only for DOI values present in the current database but absent from crossref_enrichment_raw.",
        ),
    )


def complete_harvest_run(con, status: str, notes: str = None):
    con.execute(
        """
        UPDATE harvest_runs
        SET completed_at = ?,
            status = ?,
            notes = COALESCE(?, notes)
        WHERE harvest_run_id = ?
        """,
        [now_iso(), status, notes, HARVEST_RUN_ID],
    )


def normalize_doi_sql(expr: str) -> str:
    return f"""
        LOWER(
            REGEXP_REPLACE(
                REGEXP_REPLACE(TRIM({expr}), '^https?://(dx\\.)?doi\\.org/', '', 'i'),
                '^doi:',
                '',
                'i'
            )
        )
    """


def build_delta_scope(con):
    print("Building Crossref delta enrichment scope...")

    con.execute("DELETE FROM crossref_delta_enrichment_scope")

    if not table_exists(con, "work_output_tags"):
        raise RuntimeError("work_output_tags does not exist. Run script 10 first.")

    doi_clean_expr = normalize_doi_sql("doi")

    already_filter = """
        NOT EXISTS (
            SELECT 1
            FROM crossref_enrichment_raw r
            WHERE r.doi_clean = doi_scope.doi_clean
        )
    """

    if RETRY_NON_200:
        already_filter = """
            NOT EXISTS (
                SELECT 1
                FROM crossref_enrichment_raw r
                WHERE r.doi_clean = doi_scope.doi_clean
                  AND r.response_status = 200
            )
        """

    con.execute(q(f"""
        INSERT INTO crossref_delta_enrichment_scope
        WITH doi_scope AS (
            SELECT
                work_id,
                doi,
                {doi_clean_expr} AS doi_clean,
                title,
                work_type,
                derived_output_class,
                broad_output_family,
                is_missing_source,
                is_book_or_chapter_like,
                source_name,
                source_type,

                CASE
                    WHEN broad_output_family = 'core_scholarly'
                        THEN 'crossref_delta_core_scholarly'

                    WHEN broad_output_family = 'book_based_scholarly'
                        THEN 'crossref_delta_book_based_scholarly'

                    WHEN broad_output_family = 'repository_or_open_output'
                        THEN 'crossref_delta_repository_or_open_output'

                    WHEN broad_output_family = 'grey_literature'
                        THEN 'crossref_delta_grey_literature'

                    WHEN broad_output_family = 'scholarly_service_or_minor_output'
                        THEN 'crossref_delta_minor_or_service_output'

                    WHEN is_missing_source = TRUE
                        THEN 'crossref_delta_missing_source'

                    ELSE 'crossref_delta_other'
                END AS scope_reason,

                ROW_NUMBER() OVER (
                    PARTITION BY {doi_clean_expr}
                    ORDER BY
                        CASE WHEN broad_output_family = 'core_scholarly' THEN 0 ELSE 1 END,
                        CASE WHEN is_missing_source THEN 0 ELSE 1 END,
                        publication_year DESC NULLS LAST,
                        work_id
                ) AS doi_rank

            FROM work_output_tags
            WHERE doi IS NOT NULL
              AND TRIM(doi) != ''
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
            source_name,
            source_type,
            scope_reason,
            CURRENT_TIMESTAMP::VARCHAR AS built_at
        FROM doi_scope
        WHERE doi_rank = 1
          AND doi_clean IS NOT NULL
          AND doi_clean != ''
          AND {already_filter}
    """))


def fetch_crossref(candidate):
    (
        work_id,
        doi,
        doi_clean,
        title,
        work_type,
        scope_reason,
    ) = candidate

    url = f"https://api.crossref.org/works/{doi_clean}"
    retrieved_at = now_iso()

    try:
        session = get_session()

        wait_for_rate_limit()

        response = session.get(
            url,
            params={"mailto": CONTACT_EMAIL} if CONTACT_EMAIL else None,
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

            con.execute("DELETE FROM raw_source_records WHERE raw_record_id = 'raw_crossref_work_' || SHA1(?)", [doi_clean])
            con.execute("DELETE FROM source_work_claims WHERE claim_id = 'claim_crossref_work_' || SHA1(?)", [doi_clean])
            con.execute("DELETE FROM work_identifier_claims WHERE claim_id = 'idclaim_crossref_doi_' || SHA1(?)", [doi_clean])
            con.execute("DELETE FROM work_identifier_claims WHERE claim_id = 'idclaim_crossref_isbn_' || SHA1(?)", [doi_clean])

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

        # Provenance: raw_source_records for every attempted Crossref DOI.
        con.execute(q("""
            INSERT OR REPLACE INTO raw_source_records
            SELECT
                'raw_crossref_work_' || SHA1(doi_clean) AS raw_record_id,
                ? AS harvest_run_id,
                ? AS harvest_source_id,
                'crossref' AS source_system,
                doi_clean AS source_record_id,
                'work' AS source_record_type,
                'crossref_enrichment_raw' AS raw_table_name,
                doi_clean AS raw_table_key,
                retrieved_at,
                response_status,
                SHA1(COALESCE(raw_json, '')) AS content_hash,
                error_message
            FROM crossref_enrichment_raw
            WHERE doi_clean IN (
                SELECT UNNEST(?)
            )
        """), [HARVEST_RUN_ID, HARVEST_SOURCE_ID, doi_cleans])

        # Provenance: work claims for successful Crossref payloads.
        con.execute(q("""
            INSERT OR REPLACE INTO source_work_claims
            SELECT
                'claim_crossref_work_' || SHA1(doi_clean) AS claim_id,
                'crossref' AS source_system,
                ? AS harvest_run_id,
                'raw_crossref_work_' || SHA1(doi_clean) AS raw_record_id,

                doi_clean AS source_work_id,
                work_id AS canonical_work_id,

                doi,
                crossref_title AS title,
                published_year AS publication_year,
                NULL AS publication_date,
                crossref_type AS work_type,
                container_title,
                container_title AS source_name,
                publisher,

                'doi_exact' AS claim_confidence,
                'doi' AS match_method,
                CURRENT_TIMESTAMP::VARCHAR AS created_at
            FROM crossref_enrichment_flat
            WHERE doi_clean IN (
                SELECT UNNEST(?)
            )
        """), [HARVEST_RUN_ID, doi_cleans])

        # Provenance: DOI identifier claims for successful Crossref payloads.
        con.execute(q("""
            INSERT OR REPLACE INTO work_identifier_claims
            SELECT
                'idclaim_crossref_doi_' || SHA1(doi_clean) AS claim_id,
                work_id AS canonical_work_id,
                'crossref' AS source_system,
                ? AS harvest_run_id,
                'raw_crossref_work_' || SHA1(doi_clean) AS raw_record_id,

                'doi' AS identifier_type,
                doi AS identifier_value,
                doi_clean AS identifier_normalised,

                'native_record' AS claim_confidence,
                'crossref_doi_endpoint' AS match_method,
                CURRENT_TIMESTAMP::VARCHAR AS created_at
            FROM crossref_enrichment_flat
            WHERE doi_clean IN (
                SELECT UNNEST(?)
            )
        """), [HARVEST_RUN_ID, doi_cleans])

        # Provenance: ISBN claims for successful Crossref payloads where available.
        con.execute(q("""
            INSERT OR REPLACE INTO work_identifier_claims
            SELECT
                'idclaim_crossref_isbn_' || SHA1(doi_clean || COALESCE(isbn, '')) AS claim_id,
                work_id AS canonical_work_id,
                'crossref' AS source_system,
                ? AS harvest_run_id,
                'raw_crossref_work_' || SHA1(doi_clean) AS raw_record_id,

                'isbn' AS identifier_type,
                isbn AS identifier_value,
                isbn AS identifier_normalised,

                'source_supplied' AS claim_confidence,
                'crossref_isbn_field' AS match_method,
                CURRENT_TIMESTAMP::VARCHAR AS created_at
            FROM crossref_enrichment_flat
            WHERE doi_clean IN (
                SELECT UNNEST(?)
            )
              AND isbn IS NOT NULL
              AND isbn != ''
        """), [HARVEST_RUN_ID, doi_cleans])

        con.execute("COMMIT")

    except Exception:
        con.execute("ROLLBACK")
        raise


def print_summary(con):
    print()
    print("Crossref delta enrichment run complete.")
    print(f"Harvest run: {HARVEST_RUN_ID}")
    print()

    checks = [
        ("delta scope rows", "SELECT COUNT(*) FROM crossref_delta_enrichment_scope"),
        ("crossref raw rows total", "SELECT COUNT(*) FROM crossref_enrichment_raw"),
        ("crossref successful rows total", "SELECT COUNT(*) FROM crossref_enrichment_raw WHERE response_status = 200"),
        ("crossref flat rows total", "SELECT COUNT(*) FROM crossref_enrichment_flat"),
        ("crossref contributor rows total", "SELECT COUNT(*) FROM crossref_contributors"),
        ("raw_source_records crossref rows", "SELECT COUNT(*) FROM raw_source_records WHERE source_system = 'crossref' AND source_record_type = 'work'"),
        ("source_work_claims crossref rows", "SELECT COUNT(*) FROM source_work_claims WHERE source_system = 'crossref'"),
        ("work_identifier_claims crossref doi rows", "SELECT COUNT(*) FROM work_identifier_claims WHERE source_system = 'crossref' AND identifier_type = 'doi'"),
    ]

    for label, sql in checks:
        print(f"{label}: {con.execute(q(sql)).fetchone()[0]:,}")

    print()
    print("This run status breakdown:")
    for row in con.execute(q("""
        SELECT
            response_status,
            COUNT(*) AS n
        FROM crossref_enrichment_raw
        WHERE doi_clean IN (
            SELECT doi_clean FROM crossref_delta_enrichment_scope
        )
        GROUP BY response_status
        ORDER BY n DESC
    """)).fetchall():
        print(row)

    print()
    print("This run Crossref type breakdown:")
    for row in con.execute(q("""
        SELECT
            crossref_type,
            COUNT(*) AS n
        FROM crossref_enrichment_flat
        WHERE doi_clean IN (
            SELECT doi_clean FROM crossref_delta_enrichment_scope
        )
        GROUP BY crossref_type
        ORDER BY n DESC
        LIMIT 40
    """)).fetchall():
        print(row)

    print()
    print("Remaining delta scope not attempted:")
    for row in con.execute(q("""
        SELECT
            scope_reason,
            COUNT(*) AS n
        FROM crossref_delta_enrichment_scope s
        LEFT JOIN crossref_enrichment_raw r
            ON s.doi_clean = r.doi_clean
        WHERE r.doi_clean IS NULL
        GROUP BY scope_reason
        ORDER BY n DESC
    """)).fetchall():
        print(row)


def write_scope_export(con):
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = EXPORT_DIR / f"{HARVEST_RUN_ID}_scope_summary.csv"
    con.execute(q(f"""
        COPY (
            SELECT
                scope_reason,
                COUNT(*) AS dois
            FROM crossref_delta_enrichment_scope
            GROUP BY scope_reason
            ORDER BY dois DESC
        )
        TO '{path}'
        WITH (HEADER, DELIMITER ',')
    """))
    print(f"Scope summary export: {path}")


def create_indexes(con):
    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_crossref_delta_scope_doi ON crossref_delta_enrichment_scope(doi_clean)",
        "CREATE INDEX IF NOT EXISTS idx_crossref_raw_doi ON crossref_enrichment_raw(doi_clean)",
        "CREATE INDEX IF NOT EXISTS idx_crossref_flat_doi ON crossref_enrichment_flat(doi_clean)",
        "CREATE INDEX IF NOT EXISTS idx_crossref_contrib_doi ON crossref_contributors(doi_clean)",
        "CREATE INDEX IF NOT EXISTS idx_crossref_subjects_doi ON crossref_subjects(doi_clean)",
        "CREATE INDEX IF NOT EXISTS idx_raw_source_records_source_record ON raw_source_records(source_system, source_record_id)",
        "CREATE INDEX IF NOT EXISTS idx_source_work_claims_source_work ON source_work_claims(source_system, source_work_id)",
        "CREATE INDEX IF NOT EXISTS idx_work_identifier_claims_identifier ON work_identifier_claims(identifier_type, identifier_normalised)",
    ]

    for statement in index_statements:
        con.execute(statement)


def main():
    if not CONTACT_EMAIL:
        print("WARNING: CONTACT_EMAIL is empty. Set CROSSREF_EMAIL or OPENALEX_EMAIL if possible.")

    con = duckdb.connect(str(DUCKDB_FILE))
    init_tables(con)
    seed_harvest_run(con)
    build_delta_scope(con)
    create_indexes(con)
    write_scope_export(con)

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
            FROM crossref_delta_enrichment_scope
            WHERE doi_clean IS NOT NULL
              AND doi_clean != ''
            ORDER BY
                CASE
                    WHEN scope_reason = 'crossref_delta_core_scholarly' THEN 0
                    WHEN scope_reason = 'crossref_delta_book_based_scholarly' THEN 1
                    WHEN scope_reason = 'crossref_delta_repository_or_open_output' THEN 2
                    ELSE 3
                END,
                work_type,
                work_id
        """)).fetchall()
        if row[2] not in already_done
    ]

    if MAX_WORKS is not None:
        candidates = candidates[:MAX_WORKS]

    print(f"Starting Crossref delta harvest: {HARVEST_RUN_ID}")
    print(f"DuckDB: {DUCKDB_FILE}")
    print(f"CONTACT_EMAIL set: {bool(CONTACT_EMAIL)}")
    print(f"Crossref delta scope rows: {con.execute('SELECT COUNT(*) FROM crossref_delta_enrichment_scope').fetchone()[0]:,}")
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

        complete_harvest_run(con, "complete")

    except KeyboardInterrupt:
        print()
        print("Interrupted by user. Writing completed in-memory batch before exit...")
        if batch:
            write_batch(con, batch)
        complete_harvest_run(con, "partial", "Interrupted by user after writing completed in-memory batch.")
        print_summary(con)
        con.close()
        raise

    except Exception as e:
        complete_harvest_run(con, "failed", str(e))
        con.close()
        raise

    print_summary(con)
    con.close()


if __name__ == "__main__":
    main()
