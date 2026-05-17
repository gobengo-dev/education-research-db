from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_FILE = PROJECT_ROOT / "data" / "research_database.duckdb"

DROP_EXISTING_TABLES = True


def q(sql: str) -> str:
    return sql.strip()


def main():
    con = duckdb.connect(str(DUCKDB_FILE))

    if DROP_EXISTING_TABLES:
        for table in [
            "canonical_sources",
            "work_output_tags",
            "source_metrics",
            "output_type_metrics",
        ]:
            con.execute(f"DROP TABLE IF EXISTS {table}")

    print("Building canonical_sources...")

    con.execute(q("""
        CREATE TABLE canonical_sources AS
        WITH source_rows AS (
            SELECT
                COALESCE(
                    source_id,
                    'ISSNL:' || issn_l,
                    'NAME:' || LOWER(
                        REGEXP_REPLACE(
                            COALESCE(source_name_clean, source_name),
                            '[^a-zA-Z0-9 ]',
                            '',
                            'g'
                        )
                    ),
                    'MISSING_SOURCE'
                ) AS canonical_source_key,

                source_id,
                COALESCE(source_name_clean, source_name) AS source_name,
                source_type,
                COALESCE(publisher_clean, publisher) AS publisher,
                issn_l,
                issn,
                is_oa,
                oa_status,
                landing_page_url,
                pdf_url,
                work_id
            FROM work_sources
        ),

        grouped AS (
            SELECT
                canonical_source_key,

                ANY_VALUE(source_id) AS representative_source_id,
                ANY_VALUE(source_name) AS representative_source_name,
                ANY_VALUE(source_type) AS representative_source_type,
                ANY_VALUE(publisher) AS representative_publisher,
                ANY_VALUE(issn_l) AS representative_issn_l,

                STRING_AGG(DISTINCT source_name, '; ') AS source_name_variants,
                STRING_AGG(DISTINCT source_type, '; ') AS source_type_variants,
                STRING_AGG(DISTINCT publisher, '; ') AS publisher_variants,
                STRING_AGG(DISTINCT issn_l, '; ') AS issn_l_variants,
                STRING_AGG(DISTINCT issn, '; ') AS issn_variants,

                COUNT(DISTINCT work_id) AS works_linked

            FROM source_rows
            GROUP BY canonical_source_key
        )

        SELECT
            ROW_NUMBER() OVER (ORDER BY canonical_source_key) AS canonical_source_number,
            'CS' || LPAD(CAST(ROW_NUMBER() OVER (ORDER BY canonical_source_key) AS VARCHAR), 8, '0') AS canonical_source_id,
            canonical_source_key,

            representative_source_id,
            representative_source_name,
            representative_source_type,
            representative_publisher,
            representative_issn_l,

            source_name_variants,
            source_type_variants,
            publisher_variants,
            issn_l_variants,
            issn_variants,

            works_linked,

            CASE
                WHEN representative_source_name IS NULL THEN 'missing_source'
                WHEN representative_source_type = 'journal' THEN 'scholarly_journal'
                WHEN representative_source_type = 'repository' THEN 'repository_or_preprint_platform'
                WHEN representative_source_type = 'ebook platform' THEN 'ebook_platform'
                WHEN representative_source_type = 'book series' THEN 'book_series'
                WHEN LOWER(representative_source_name) LIKE '%proceeding%' THEN 'conference_or_proceedings'
                WHEN LOWER(representative_source_name) LIKE '%conference%' THEN 'conference_or_proceedings'
                ELSE 'other_source'
            END AS derived_source_class,

            CASE
                WHEN representative_source_type = 'journal' THEN TRUE ELSE FALSE
            END AS is_journal_like,

            CASE
                WHEN representative_source_type = 'repository' THEN TRUE ELSE FALSE
            END AS is_repository_like,

            CASE
                WHEN representative_source_type IN ('ebook platform', 'book series') THEN TRUE ELSE FALSE
            END AS is_book_like,

            CASE
                WHEN representative_source_name IS NULL THEN TRUE ELSE FALSE
            END AS is_missing_source

        FROM grouped
    """))

    print("Building work_output_tags...")

    con.execute(q("""
        CREATE TABLE work_output_tags AS
        WITH clean_sources AS (
            SELECT
                work_id,
                source_id,
                COALESCE(source_name_clean, source_name) AS source_name,
                source_type,
                COALESCE(publisher_clean, publisher) AS publisher,
                issn_l,
                issn,
                is_oa,
                oa_status,
                landing_page_url,
                pdf_url,

                COALESCE(
                    source_id,
                    'ISSNL:' || issn_l,
                    'NAME:' || LOWER(
                        REGEXP_REPLACE(
                            COALESCE(source_name_clean, source_name),
                            '[^a-zA-Z0-9 ]',
                            '',
                            'g'
                        )
                    ),
                    'MISSING_SOURCE'
                ) AS canonical_source_key
            FROM work_sources
        )

        SELECT
            wf.work_id,
            wf.doi,
            COALESCE(wf.title_clean, wf.title) AS title,
            wf.title AS title_raw,
            wf.title_clean,
            wf.publication_year,
            wf.publication_date,
            wf.work_type,
            wf.type_crossref,
            wf.cited_by_count,
            wf.fwci,
            wf.citation_percentile,
            wf.is_in_top_1_percent,
            wf.is_in_top_10_percent,
            wf.primary_topic_id,
            wf.primary_topic_name,
            wf.language,

            ws.source_id,
            ws.source_name,
            ws.source_type,
            ws.publisher,
            ws.issn_l,
            ws.issn,
            ws.is_oa,
            ws.oa_status,
            ws.landing_page_url,
            ws.pdf_url,

            cs.canonical_source_id,
            cs.canonical_source_key,
            cs.representative_source_name AS canonical_source_name,
            cs.representative_source_type AS canonical_source_type,
            cs.derived_source_class,

            CASE
                WHEN ws.source_name IS NULL THEN 'missing_source'

                WHEN wf.work_type = 'preprint'
                    THEN 'preprint'

                WHEN wf.work_type = 'dataset'
                    THEN 'dataset'

                WHEN wf.work_type = 'dissertation'
                    THEN 'dissertation'

                WHEN wf.work_type = 'book'
                    THEN 'book'

                WHEN wf.work_type = 'book-chapter'
                    THEN 'book_chapter'

                WHEN wf.work_type = 'review'
                     AND ws.source_type = 'journal'
                    THEN 'journal_review_article'

                WHEN wf.work_type = 'article'
                     AND ws.source_type = 'journal'
                    THEN 'journal_article'

                WHEN wf.work_type = 'article'
                     AND ws.source_type = 'repository'
                    THEN 'repository_article_or_preprint_record'

                WHEN wf.work_type = 'review'
                     AND ws.source_type = 'repository'
                    THEN 'repository_review_or_preprint_record'

                WHEN wf.work_type IN ('editorial', 'letter')
                    THEN 'editorial_or_letter'

                WHEN wf.work_type = 'report'
                    THEN 'report'

                WHEN wf.work_type = 'peer-review'
                    THEN 'peer_review_record'

                WHEN ws.source_type = 'ebook platform'
                    THEN 'ebook_platform_record'

                WHEN ws.source_type = 'book series'
                    THEN 'book_series_record'

                WHEN LOWER(COALESCE(ws.source_name, '')) LIKE '%proceeding%'
                  OR LOWER(COALESCE(ws.source_name, '')) LIKE '%conference%'
                  OR LOWER(COALESCE(wf.type_crossref, '')) LIKE '%proceeding%'
                    THEN 'conference_output'

                ELSE 'other_output'
            END AS derived_output_class,

            CASE
                WHEN wf.work_type IN ('article', 'review')
                 AND ws.source_type = 'journal'
                THEN TRUE ELSE FALSE
            END AS is_journal_article_like,

            CASE
                WHEN wf.work_type = 'article'
                 AND ws.source_type = 'journal'
                THEN TRUE ELSE FALSE
            END AS is_standard_journal_article,

            CASE
                WHEN wf.work_type = 'review'
                 AND ws.source_type = 'journal'
                THEN TRUE ELSE FALSE
            END AS is_review_article,

            CASE
                WHEN ws.source_type = 'journal'
                 AND wf.work_type IN ('article', 'review')
                THEN TRUE ELSE FALSE
            END AS is_peer_review_likely,

            CASE
                WHEN ws.source_type = 'repository'
                  OR wf.work_type = 'preprint'
                THEN TRUE ELSE FALSE
            END AS is_repository_or_preprint_like,

            CASE
                WHEN wf.work_type IN ('book', 'book-chapter')
                  OR ws.source_type IN ('ebook platform', 'book series')
                THEN TRUE ELSE FALSE
            END AS is_book_or_chapter_like,

            CASE
                WHEN wf.work_type IN ('report', 'dissertation')
                  OR ws.source_type = 'repository'
                  OR ws.source_name IS NULL
                THEN TRUE ELSE FALSE
            END AS is_grey_literature_likely,

            CASE
                WHEN wf.work_type IN ('dataset')
                THEN TRUE ELSE FALSE
            END AS is_data_output,

            CASE
                WHEN ws.source_name IS NULL
                THEN TRUE ELSE FALSE
            END AS is_missing_source,

            CASE
                WHEN wf.work_type IN ('article', 'review')
                 AND ws.source_type = 'journal'
                THEN 'core_scholarly'

                WHEN wf.work_type IN ('book', 'book-chapter')
                  OR ws.source_type IN ('ebook platform', 'book series')
                THEN 'book_based_scholarly'

                WHEN ws.source_type = 'repository'
                  OR wf.work_type IN ('preprint', 'dataset')
                THEN 'repository_or_open_output'

                WHEN wf.work_type IN ('report', 'dissertation')
                THEN 'grey_literature'

                WHEN wf.work_type IN ('editorial', 'letter', 'peer-review', 'erratum', 'paratext', 'retraction')
                THEN 'scholarly_service_or_minor_output'

                WHEN ws.source_name IS NULL
                THEN 'source_missing'

                ELSE 'other'
            END AS broad_output_family

        FROM works_flat wf

        LEFT JOIN clean_sources ws
            ON wf.work_id = ws.work_id

        LEFT JOIN canonical_sources cs
            ON ws.canonical_source_key = cs.canonical_source_key
    """))

    print("Building source_metrics...")

    con.execute(q("""
        CREATE TABLE source_metrics AS
        SELECT
            canonical_source_id,
            canonical_source_key,
            canonical_source_name,
            canonical_source_type,
            derived_source_class,

            COUNT(DISTINCT work_id) AS works,
            SUM(cited_by_count) AS total_citations,
            AVG(cited_by_count) AS mean_citations,
            MEDIAN(cited_by_count) AS median_citations,
            AVG(fwci) AS mean_fwci,

            COUNT(DISTINCT publication_year) AS publication_years_observed,
            MIN(publication_year) AS first_year_observed,
            MAX(publication_year) AS last_year_observed,

            SUM(CASE WHEN is_peer_review_likely THEN 1 ELSE 0 END) AS peer_review_likely_outputs,
            SUM(CASE WHEN is_repository_or_preprint_like THEN 1 ELSE 0 END) AS repository_or_preprint_outputs,
            SUM(CASE WHEN is_book_or_chapter_like THEN 1 ELSE 0 END) AS book_or_chapter_outputs,
            SUM(CASE WHEN is_grey_literature_likely THEN 1 ELSE 0 END) AS grey_literature_outputs,

            STRING_AGG(DISTINCT derived_output_class, '; ') AS output_classes_observed,
            STRING_AGG(DISTINCT broad_output_family, '; ') AS broad_output_families_observed

        FROM work_output_tags
        GROUP BY
            canonical_source_id,
            canonical_source_key,
            canonical_source_name,
            canonical_source_type,
            derived_source_class
    """))

    print("Building output_type_metrics...")

    con.execute(q("""
        CREATE TABLE output_type_metrics AS
        SELECT
            broad_output_family,
            derived_output_class,
            work_type,
            type_crossref,
            source_type,

            COUNT(*) AS works,
            SUM(cited_by_count) AS total_citations,
            AVG(cited_by_count) AS mean_citations,
            MEDIAN(cited_by_count) AS median_citations,
            AVG(fwci) AS mean_fwci,

            COUNT(DISTINCT canonical_source_id) AS sources,
            COUNT(DISTINCT publication_year) AS publication_years_observed,
            MIN(publication_year) AS first_year_observed,
            MAX(publication_year) AS last_year_observed

        FROM work_output_tags
        GROUP BY
            broad_output_family,
            derived_output_class,
            work_type,
            type_crossref,
            source_type
    """))

    print("Creating indexes...")

    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_cs_id ON canonical_sources(canonical_source_id)",
        "CREATE INDEX IF NOT EXISTS idx_cs_key ON canonical_sources(canonical_source_key)",
        "CREATE INDEX IF NOT EXISTS idx_cs_type ON canonical_sources(derived_source_class)",

        "CREATE INDEX IF NOT EXISTS idx_wot_work ON work_output_tags(work_id)",
        "CREATE INDEX IF NOT EXISTS idx_wot_source ON work_output_tags(canonical_source_id)",
        "CREATE INDEX IF NOT EXISTS idx_wot_output ON work_output_tags(derived_output_class)",
        "CREATE INDEX IF NOT EXISTS idx_wot_family ON work_output_tags(broad_output_family)",
        "CREATE INDEX IF NOT EXISTS idx_wot_year ON work_output_tags(publication_year)",

        "CREATE INDEX IF NOT EXISTS idx_sm_source ON source_metrics(canonical_source_id)",
        "CREATE INDEX IF NOT EXISTS idx_otm_output ON output_type_metrics(derived_output_class)",
    ]:
        con.execute(sql)

    print()
    print("Publication output layer complete.")
    print()

    checks = [
        ("canonical_sources", "SELECT COUNT(*) FROM canonical_sources"),
        ("work_output_tags", "SELECT COUNT(*) FROM work_output_tags"),
        ("source_metrics", "SELECT COUNT(*) FROM source_metrics"),
        ("output_type_metrics", "SELECT COUNT(*) FROM output_type_metrics"),
    ]

    for label, sql in checks:
        count = con.execute(sql).fetchone()[0]
        print(f"{label}: {count:,}")

    print()
    print("Broad output families:")
    for row in con.execute(q("""
        SELECT
            broad_output_family,
            COUNT(*) AS works
        FROM work_output_tags
        GROUP BY broad_output_family
        ORDER BY works DESC
    """)).fetchall():
        print(row)

    print()
    print("Derived output classes:")
    for row in con.execute(q("""
        SELECT
            derived_output_class,
            COUNT(*) AS works
        FROM work_output_tags
        GROUP BY derived_output_class
        ORDER BY works DESC
    """)).fetchall():
        print(row)

    print()
    print("Top canonical sources:")
    for row in con.execute(q("""
        SELECT
            canonical_source_name,
            canonical_source_type,
            derived_source_class,
            works,
            peer_review_likely_outputs,
            repository_or_preprint_outputs,
            book_or_chapter_outputs
        FROM source_metrics
        ORDER BY works DESC
        LIMIT 40
    """)).fetchall():
        print(row)

    print()
    print("Australian Journal of Teacher Education source check:")
    for row in con.execute(q("""
        SELECT DISTINCT
            source_name,
            HEX(source_name) AS source_name_hex
        FROM work_output_tags
        WHERE LOWER(source_name) LIKE '%australian journal of teacher education%'
        LIMIT 5
    """)).fetchall():
        print(row)

    con.close()


if __name__ == "__main__":
    main()