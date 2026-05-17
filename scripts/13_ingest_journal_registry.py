from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_FILE = PROJECT_ROOT / "data" / "research_database.duckdb"
REGISTRY_CSV = PROJECT_ROOT / "data" / "curated" / "journals" / "au_nz_education_journal_registry_seed.csv"


def q(sql: str) -> str:
    return sql.strip()


def main():
    con = duckdb.connect(str(DUCKDB_FILE))

    print(f"Loading journal registry seed: {REGISTRY_CSV}")

    con.execute("DROP TABLE IF EXISTS journal_registry_seed")

    con.execute(q(f"""
        CREATE TABLE journal_registry_seed AS
        SELECT *
        FROM read_csv_auto(
            '{REGISTRY_CSV}',
            header = true,
            all_varchar = true
        )
    """))

    con.execute("DROP TABLE IF EXISTS journal_registry")

    con.execute(q("""
        CREATE TABLE journal_registry AS
        SELECT
            registry_id,
            title,

            LOWER(
                REGEXP_REPLACE(
                    title,
                    '[^a-zA-Z0-9 ]',
                    '',
                    'g'
                )
            ) AS title_normalized,

            NULLIF(issn_print, '') AS issn_print,
            NULLIF(issn_online, '') AS issn_online,
            country,
            region,
            journal_type,
            priority_band,

            CASE
                WHEN LOWER(active) IN ('true', 'yes', '1') THEN TRUE
                WHEN LOWER(active) IN ('false', 'no', '0') THEN FALSE
                ELSE NULL
            END AS active,

            NULLIF(publisher, '') AS publisher,
            NULLIF(notes, '') AS notes

        FROM journal_registry_seed
    """))

    con.execute("DROP TABLE IF EXISTS journal_registry_identifiers")

    con.execute(q("""
        CREATE TABLE journal_registry_identifiers AS
        SELECT
            registry_id,
            title,
            'issn_print' AS identifier_type,
            issn_print AS identifier_value
        FROM journal_registry
        WHERE issn_print IS NOT NULL

        UNION ALL

        SELECT
            registry_id,
            title,
            'issn_online' AS identifier_type,
            issn_online AS identifier_value
        FROM journal_registry
        WHERE issn_online IS NOT NULL
    """))

    con.execute("DROP TABLE IF EXISTS journal_registry_current_coverage")

    con.execute(q("""
        CREATE TABLE journal_registry_current_coverage AS
        WITH registry_issns AS (
            SELECT
                registry_id,
                title,
                identifier_value AS issn
            FROM journal_registry_identifiers
            WHERE identifier_type IN ('issn_print', 'issn_online')
        ),

        matched AS (
            SELECT
                r.registry_id,
                r.title AS registry_title,
                wot.work_id,
                wot.publication_year,
                wot.canonical_source_id,
                wot.canonical_source_name,
                wot.issn_l,
                wot.issn,
                wot.doi,
                wot.derived_output_class,
                wot.broad_output_family
            FROM registry_issns r
            JOIN work_output_tags wot
                ON wot.issn_l = r.issn
                OR wot.issn LIKE '%' || r.issn || '%'
        )

        SELECT
            registry_id,
            registry_title,
            COUNT(DISTINCT work_id) AS works_in_current_db,
            COUNT(DISTINCT CASE WHEN publication_year >= 2015 THEN work_id END) AS works_2015_onward,
            MIN(publication_year) AS first_year_in_db,
            MAX(publication_year) AS last_year_in_db,
            COUNT(DISTINCT canonical_source_id) AS matched_canonical_sources,
            STRING_AGG(DISTINCT canonical_source_name, '; ') AS matched_source_names,
            STRING_AGG(DISTINCT issn_l, '; ') AS matched_issn_ls,
            COUNT(DISTINCT doi) AS works_with_doi,
            STRING_AGG(DISTINCT derived_output_class, '; ') AS output_classes_observed
        FROM matched
        GROUP BY
            registry_id,
            registry_title
    """))

    con.execute("DROP TABLE IF EXISTS journal_registry_missing_from_current_db")

    con.execute(q("""
        CREATE TABLE journal_registry_missing_from_current_db AS
        SELECT
            jr.*
        FROM journal_registry jr
        LEFT JOIN journal_registry_current_coverage c
            ON jr.registry_id = c.registry_id
        WHERE c.registry_id IS NULL
    """))

    print()
    print("Journal registry ingest complete.")
    print()

    checks = [
        ("journal_registry", "SELECT COUNT(*) FROM journal_registry"),
        ("journal_registry_identifiers", "SELECT COUNT(*) FROM journal_registry_identifiers"),
        ("journal_registry_current_coverage", "SELECT COUNT(*) FROM journal_registry_current_coverage"),
        ("journal_registry_missing_from_current_db", "SELECT COUNT(*) FROM journal_registry_missing_from_current_db"),
    ]

    for label, sql in checks:
        print(f"{label}: {con.execute(sql).fetchone()[0]:,}")

    print()
    print("Coverage summary:")
    for row in con.execute(q("""
        SELECT
            jr.priority_band,
            jr.journal_type,
            COUNT(*) AS registry_journals,
            COUNT(c.registry_id) AS journals_matched,
            SUM(COALESCE(c.works_in_current_db, 0)) AS works_in_current_db,
            SUM(COALESCE(c.works_2015_onward, 0)) AS works_2015_onward
        FROM journal_registry jr
        LEFT JOIN journal_registry_current_coverage c
            ON jr.registry_id = c.registry_id
        GROUP BY
            jr.priority_band,
            jr.journal_type
        ORDER BY
            jr.priority_band,
            works_in_current_db DESC
    """)).fetchall():
        print(row)

    print()
    print("Potential registry journals not currently matched:")
    for row in con.execute(q("""
        SELECT
            registry_id,
            title,
            issn_print,
            issn_online,
            country,
            journal_type,
            priority_band
        FROM journal_registry_missing_from_current_db
        ORDER BY priority_band, country, title
    """)).fetchall():
        print(row)

    con.close()


if __name__ == "__main__":
    main()