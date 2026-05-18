from pathlib import Path
from datetime import datetime
import hashlib
import json

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_FILE = PROJECT_ROOT / "data" / "research_database.duckdb"


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


def create_tables(con):
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
        CREATE TABLE IF NOT EXISTS processing_runs (
            processing_run_id VARCHAR PRIMARY KEY,
            script_name VARCHAR,
            script_version VARCHAR,
            started_at VARCHAR,
            completed_at VARCHAR,
            status VARCHAR,
            input_tables_json VARCHAR,
            output_tables_json VARCHAR,
            parameters_json VARCHAR,
            notes VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS processing_outputs (
            processing_run_id VARCHAR,
            output_table_name VARCHAR,
            output_row_count BIGINT,
            output_description VARCHAR
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


def seed_historical_runs(con):
    completed_at = now_iso()

    runs = [
        (
            "hr_openalex_initial_topic_harvest",
            "openalex",
            "topic_harvest",
            "unknown_initial_openalex_harvest_scripts",
            "historical_backfill",
            None,
            completed_at,
            "historical_backfill",
            json.dumps({"backfilled": True}, ensure_ascii=False),
            "Historical OpenAlex work harvest inferred from raw_works and harvest_seen_work_topics.",
        ),
        (
            "hr_crossref_all_dois_2026_05",
            "crossref",
            "doi_api",
            "12_enrich_all_dois_from_crossref.py / 12b_enrich_all_dois_from_crossref_rate_limited.py",
            "historical_backfill",
            None,
            completed_at,
            "historical_backfill",
            json.dumps({"backfilled": True}, ensure_ascii=False),
            "Historical all-DOI Crossref enrichment inferred from crossref_enrichment_raw.",
        ),
        (
            "hr_orcid_profile_harvest",
            "orcid",
            "profile_api",
            "unknown_orcid_harvest_script",
            "historical_backfill",
            None,
            completed_at,
            "historical_backfill",
            json.dumps({"backfilled": True}, ensure_ascii=False),
            "Historical ORCID enrichment inferred from orcid_raw_profiles.",
        ),
        (
            "hr_ror_snapshot_ingest",
            "ror",
            "bulk_or_api_ingest",
            "unknown_ror_ingest_script",
            "historical_backfill",
            None,
            completed_at,
            "historical_backfill",
            json.dumps({"backfilled": True}, ensure_ascii=False),
            "Historical ROR ingest inferred from ror_raw_records and derived ROR tables.",
        ),
    ]

    con.executemany(
        """
        INSERT OR REPLACE INTO harvest_runs
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        runs,
    )

    sources = [
        (
            "hs_openalex_api",
            "openalex",
            "OpenAlex API",
            "https://api.openalex.org",
            "api",
            "openalex",
            "api",
            "Source-level placeholder for historical OpenAlex API harvests.",
        ),
        (
            "hs_crossref_api",
            "crossref",
            "Crossref REST API",
            "https://api.crossref.org",
            "api",
            "crossref",
            "api",
            "Source-level placeholder for Crossref DOI API harvests.",
        ),
        (
            "hs_orcid_api",
            "orcid",
            "ORCID Public API",
            "https://pub.orcid.org",
            "api",
            "orcid",
            "api",
            "Source-level placeholder for ORCID public profile harvests.",
        ),
        (
            "hs_ror_registry",
            "ror",
            "ROR Registry",
            "https://ror.org",
            "registry",
            "ror",
            "api_or_bulk",
            "Source-level placeholder for ROR registry ingest.",
        ),
    ]

    con.executemany(
        """
        INSERT OR REPLACE INTO harvest_sources
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        sources,
    )


def backfill_raw_source_records(con):
    print("Backfilling raw_source_records...")

    if table_exists(con, "raw_works"):
        con.execute(q("""
            INSERT OR REPLACE INTO raw_source_records
            SELECT
                'raw_openalex_work_' || SHA1(work_id) AS raw_record_id,
                'hr_openalex_initial_topic_harvest' AS harvest_run_id,
                'hs_openalex_api' AS harvest_source_id,
                'openalex' AS source_system,
                work_id AS source_record_id,
                'work' AS source_record_type,
                'raw_works' AS raw_table_name,
                work_id AS raw_table_key,
                harvested_at AS retrieved_at,
                200 AS response_status,
                SHA1(COALESCE(raw_json, '')) AS content_hash,
                NULL AS error_message
            FROM raw_works
        """))

    if table_exists(con, "crossref_enrichment_raw"):
        con.execute(q("""
            INSERT OR REPLACE INTO raw_source_records
            SELECT
                'raw_crossref_work_' || SHA1(doi_clean) AS raw_record_id,
                'hr_crossref_all_dois_2026_05' AS harvest_run_id,
                'hs_crossref_api' AS harvest_source_id,
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
        """))

    if table_exists(con, "orcid_raw_profiles"):
        con.execute(q("""
            INSERT OR REPLACE INTO raw_source_records
            SELECT
                'raw_orcid_profile_' || SHA1(orcid) AS raw_record_id,
                'hr_orcid_profile_harvest' AS harvest_run_id,
                'hs_orcid_api' AS harvest_source_id,
                'orcid' AS source_system,
                orcid AS source_record_id,
                'person' AS source_record_type,
                'orcid_raw_profiles' AS raw_table_name,
                orcid AS raw_table_key,
                retrieved_at,
                response_status,
                SHA1(COALESCE(raw_json, '')) AS content_hash,
                error_message
            FROM orcid_raw_profiles
        """))

    if table_exists(con, "ror_raw_records"):
        con.execute(q("""
            INSERT OR REPLACE INTO raw_source_records
            SELECT
                'raw_ror_record_' || SHA1(ror_id) AS raw_record_id,
                'hr_ror_snapshot_ingest' AS harvest_run_id,
                'hs_ror_registry' AS harvest_source_id,
                'ror' AS source_system,
                ror_id AS source_record_id,
                'institution' AS source_record_type,
                'ror_raw_records' AS raw_table_name,
                ror_id AS raw_table_key,
                NULL AS retrieved_at,
                200 AS response_status,
                SHA1(COALESCE(raw_json, '')) AS content_hash,
                NULL AS error_message
            FROM ror_raw_records
        """))


def backfill_source_work_claims(con):
    print("Backfilling source_work_claims...")

    if table_exists(con, "works_flat"):
        con.execute(q("""
            INSERT OR REPLACE INTO source_work_claims
            SELECT
                'claim_openalex_work_' || SHA1(w.work_id) AS claim_id,
                'openalex' AS source_system,
                'hr_openalex_initial_topic_harvest' AS harvest_run_id,
                'raw_openalex_work_' || SHA1(w.work_id) AS raw_record_id,

                w.work_id AS source_work_id,
                w.work_id AS canonical_work_id,

                w.doi,
                w.title,
                w.publication_year,
                w.publication_date,
                w.work_type,
                NULL AS container_title,
                NULL AS source_name,
                NULL AS publisher,

                'native_record' AS claim_confidence,
                'native_openalex_work_id' AS match_method,
                CURRENT_TIMESTAMP::VARCHAR AS created_at
            FROM works_flat w
        """))

    if table_exists(con, "work_output_tags"):
        con.execute(q("""
            INSERT OR REPLACE INTO source_work_claims
            SELECT
                'claim_current_output_layer_' || SHA1(work_id) AS claim_id,
                'derived_current_database' AS source_system,
                NULL AS harvest_run_id,
                NULL AS raw_record_id,

                work_id AS source_work_id,
                work_id AS canonical_work_id,

                doi,
                title,
                publication_year,
                publication_date,
                work_type,
                canonical_source_name AS container_title,
                source_name,
                publisher,

                'derived' AS claim_confidence,
                'current_work_output_tags' AS match_method,
                CURRENT_TIMESTAMP::VARCHAR AS created_at
            FROM work_output_tags
        """))

    if table_exists(con, "crossref_enrichment_flat"):
        con.execute(q("""
            INSERT OR REPLACE INTO source_work_claims
            SELECT
                'claim_crossref_work_' || SHA1(doi_clean) AS claim_id,
                'crossref' AS source_system,
                'hr_crossref_all_dois_2026_05' AS harvest_run_id,
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
        """))


def backfill_identifier_claims(con):
    print("Backfilling work_identifier_claims...")

    if table_exists(con, "works_flat"):
        con.execute(q("""
            INSERT OR REPLACE INTO work_identifier_claims
            SELECT
                'idclaim_openalex_' || SHA1(work_id) AS claim_id,
                work_id AS canonical_work_id,
                'openalex' AS source_system,
                'hr_openalex_initial_topic_harvest' AS harvest_run_id,
                'raw_openalex_work_' || SHA1(work_id) AS raw_record_id,

                'openalex_work_id' AS identifier_type,
                work_id AS identifier_value,
                work_id AS identifier_normalised,

                'native_record' AS claim_confidence,
                'native_openalex_work_id' AS match_method,
                CURRENT_TIMESTAMP::VARCHAR AS created_at
            FROM works_flat
        """))

        con.execute(q("""
            INSERT OR REPLACE INTO work_identifier_claims
            SELECT
                'idclaim_openalex_doi_' || SHA1(work_id || COALESCE(doi, '')) AS claim_id,
                work_id AS canonical_work_id,
                'openalex' AS source_system,
                'hr_openalex_initial_topic_harvest' AS harvest_run_id,
                'raw_openalex_work_' || SHA1(work_id) AS raw_record_id,

                'doi' AS identifier_type,
                doi AS identifier_value,
                LOWER(REGEXP_REPLACE(doi, '^https?://(dx\\.)?doi\\.org/', '', 'i')) AS identifier_normalised,

                'source_supplied' AS claim_confidence,
                'openalex_doi_field' AS match_method,
                CURRENT_TIMESTAMP::VARCHAR AS created_at
            FROM works_flat
            WHERE doi IS NOT NULL
              AND doi != ''
        """))

    if table_exists(con, "crossref_enrichment_flat"):
        con.execute(q("""
            INSERT OR REPLACE INTO work_identifier_claims
            SELECT
                'idclaim_crossref_doi_' || SHA1(doi_clean) AS claim_id,
                work_id AS canonical_work_id,
                'crossref' AS source_system,
                'hr_crossref_all_dois_2026_05' AS harvest_run_id,
                'raw_crossref_work_' || SHA1(doi_clean) AS raw_record_id,

                'doi' AS identifier_type,
                doi AS identifier_value,
                doi_clean AS identifier_normalised,

                'native_record' AS claim_confidence,
                'crossref_doi_endpoint' AS match_method,
                CURRENT_TIMESTAMP::VARCHAR AS created_at
            FROM crossref_enrichment_flat
        """))

        con.execute(q("""
            INSERT OR REPLACE INTO work_identifier_claims
            SELECT
                'idclaim_crossref_isbn_' || SHA1(doi_clean || COALESCE(isbn, '')) AS claim_id,
                work_id AS canonical_work_id,
                'crossref' AS source_system,
                'hr_crossref_all_dois_2026_05' AS harvest_run_id,
                'raw_crossref_work_' || SHA1(doi_clean) AS raw_record_id,

                'isbn' AS identifier_type,
                isbn AS identifier_value,
                isbn AS identifier_normalised,

                'source_supplied' AS claim_confidence,
                'crossref_isbn_field' AS match_method,
                CURRENT_TIMESTAMP::VARCHAR AS created_at
            FROM crossref_enrichment_flat
            WHERE isbn IS NOT NULL
              AND isbn != ''
        """))


def seed_processing_runs(con):
    completed_at = now_iso()

    rows = [
        (
            "pr_publication_output_layer",
            "10_build_publication_output_layer.py",
            "historical_backfill",
            None,
            completed_at,
            "historical_backfill",
            json.dumps(["works_flat", "work_sources"], ensure_ascii=False),
            json.dumps(["canonical_sources", "work_output_tags", "source_metrics", "output_type_metrics"], ensure_ascii=False),
            json.dumps({"backfilled": True}, ensure_ascii=False),
            "Builds publication output layer and source/output classifications.",
        ),
        (
            "pr_journal_registry_ingest",
            "13_ingest_journal_registry.py",
            "historical_backfill",
            None,
            completed_at,
            "historical_backfill",
            json.dumps(["data/curated/journals/*.csv", "work_output_tags"], ensure_ascii=False),
            json.dumps(["journal_registry", "journal_registry_identifiers", "journal_registry_current_coverage"], ensure_ascii=False),
            json.dumps({"backfilled": True}, ensure_ascii=False),
            "Ingests curated AU/NZ education journal registry and checks current coverage.",
        ),
        (
            "pr_provenance_architecture_backfill",
            "15_create_provenance_architecture.py",
            "v2_no_crossref_test_harvest",
            completed_at,
            completed_at,
            "complete",
            json.dumps(
                [
                    "raw_works",
                    "crossref_enrichment_raw",
                    "orcid_raw_profiles",
                    "ror_raw_records",
                    "works_flat",
                    "work_output_tags",
                    "crossref_enrichment_flat",
                ],
                ensure_ascii=False,
            ),
            json.dumps(
                [
                    "harvest_runs",
                    "harvest_sources",
                    "raw_source_records",
                    "processing_runs",
                    "processing_outputs",
                    "source_work_claims",
                    "work_identifier_claims",
                ],
                ensure_ascii=False,
            ),
            json.dumps({"backfilled": True}, ensure_ascii=False),
            "Creates initial provenance architecture and backfills ledgers from existing tables. Crossref test harvest excluded.",
        ),
    ]

    con.executemany(
        """
        INSERT OR REPLACE INTO processing_runs
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

    con.execute(q("""
        DELETE FROM processing_outputs
        WHERE processing_run_id IN (
            'pr_publication_output_layer',
            'pr_journal_registry_ingest',
            'pr_provenance_architecture_backfill'
        )
    """))

    outputs = [
        ("pr_publication_output_layer", "canonical_sources", count_rows(con, "canonical_sources"), "Canonicalized OpenAlex source layer."),
        ("pr_publication_output_layer", "work_output_tags", count_rows(con, "work_output_tags"), "Work-level output classifications."),
        ("pr_publication_output_layer", "source_metrics", count_rows(con, "source_metrics"), "Source-level metrics."),
        ("pr_publication_output_layer", "output_type_metrics", count_rows(con, "output_type_metrics"), "Output type metrics."),

        ("pr_journal_registry_ingest", "journal_registry", count_rows(con, "journal_registry"), "Curated journal registry."),
        ("pr_journal_registry_ingest", "journal_registry_identifiers", count_rows(con, "journal_registry_identifiers"), "Journal ISSN identifier rows."),
        ("pr_journal_registry_ingest", "journal_registry_current_coverage", count_rows(con, "journal_registry_current_coverage"), "Registry coverage in current DB."),

        ("pr_provenance_architecture_backfill", "harvest_runs", count_rows(con, "harvest_runs"), "Harvest run ledger."),
        ("pr_provenance_architecture_backfill", "harvest_sources", count_rows(con, "harvest_sources"), "Harvest source ledger."),
        ("pr_provenance_architecture_backfill", "raw_source_records", count_rows(con, "raw_source_records"), "Universal raw record ledger."),
        ("pr_provenance_architecture_backfill", "source_work_claims", count_rows(con, "source_work_claims"), "Work-level source claims."),
        ("pr_provenance_architecture_backfill", "work_identifier_claims", count_rows(con, "work_identifier_claims"), "Identifier-level source claims."),
    ]

    con.executemany(
        """
        INSERT INTO processing_outputs
        VALUES (?, ?, ?, ?)
        """,
        outputs,
    )


def create_indexes(con):
    print("Creating indexes...")

    index_statements = [
        "CREATE INDEX IF NOT EXISTS idx_raw_source_records_source ON raw_source_records(source_system)",
        "CREATE INDEX IF NOT EXISTS idx_raw_source_records_source_record ON raw_source_records(source_system, source_record_id)",
        "CREATE INDEX IF NOT EXISTS idx_raw_source_records_harvest_run ON raw_source_records(harvest_run_id)",

        "CREATE INDEX IF NOT EXISTS idx_source_work_claims_canonical_work ON source_work_claims(canonical_work_id)",
        "CREATE INDEX IF NOT EXISTS idx_source_work_claims_source_work ON source_work_claims(source_system, source_work_id)",
        "CREATE INDEX IF NOT EXISTS idx_source_work_claims_doi ON source_work_claims(doi)",

        "CREATE INDEX IF NOT EXISTS idx_work_identifier_claims_canonical_work ON work_identifier_claims(canonical_work_id)",
        "CREATE INDEX IF NOT EXISTS idx_work_identifier_claims_identifier ON work_identifier_claims(identifier_type, identifier_normalised)",
        "CREATE INDEX IF NOT EXISTS idx_work_identifier_claims_source ON work_identifier_claims(source_system)",
    ]

    for statement in index_statements:
        con.execute(statement)


def cleanup_removed_test_harvest(con):
    con.execute(q("""
        DELETE FROM raw_source_records
        WHERE harvest_run_id = 'hr_crossref_missing_source_test'
    """))

    con.execute(q("""
        DELETE FROM harvest_runs
        WHERE harvest_run_id = 'hr_crossref_missing_source_test'
    """))


def print_summary(con):
    print()
    print("Provenance architecture complete.")
    print()

    for table in [
        "harvest_runs",
        "harvest_sources",
        "raw_source_records",
        "processing_runs",
        "processing_outputs",
        "source_work_claims",
        "work_identifier_claims",
    ]:
        print(f"{table}: {count_rows(con, table):,}")

    print()
    print("Raw records by source system:")
    for row in con.execute(q("""
        SELECT
            source_system,
            source_record_type,
            COUNT(*) AS rows
        FROM raw_source_records
        GROUP BY source_system, source_record_type
        ORDER BY rows DESC
    """)).fetchall():
        print(row)

    print()
    print("Source work claims by source system:")
    for row in con.execute(q("""
        SELECT
            source_system,
            COUNT(*) AS rows
        FROM source_work_claims
        GROUP BY source_system
        ORDER BY rows DESC
    """)).fetchall():
        print(row)

    print()
    print("Identifier claims by type/source:")
    for row in con.execute(q("""
        SELECT
            identifier_type,
            source_system,
            COUNT(*) AS rows
        FROM work_identifier_claims
        GROUP BY identifier_type, source_system
        ORDER BY rows DESC
    """)).fetchall():
        print(row)


def main():
    con = duckdb.connect(str(DUCKDB_FILE))

    print("Creating provenance architecture tables...")
    create_tables(con)

    print("Seeding historical harvest runs and sources...")
    seed_historical_runs(con)

    print("Cleaning up removed test harvest references...")
    cleanup_removed_test_harvest(con)

    backfill_raw_source_records(con)
    backfill_source_work_claims(con)
    backfill_identifier_claims(con)

    print("Seeding historical processing runs...")
    seed_processing_runs(con)

    create_indexes(con)
    print_summary(con)

    con.close()


if __name__ == "__main__":
    main()