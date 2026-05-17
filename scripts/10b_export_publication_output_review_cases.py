from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_FILE = PROJECT_ROOT / "data" / "research_database.duckdb"
EXPORT_DIR = PROJECT_ROOT / "data" / "exports" / "publication_output_review"

EXPORT_DIR.mkdir(parents=True, exist_ok=True)


def export_query(con, filename, sql):
    path = EXPORT_DIR / filename
    con.execute(f"""
        COPY (
            {sql}
        ) TO '{path}' (HEADER, DELIMITER ',')
    """)
    print(f"Exported: {path}")


def main():
    con = duckdb.connect(str(DUCKDB_FILE))

    export_query(
        con,
        "output_class_counts.csv",
        """
        SELECT
            broad_output_family,
            derived_output_class,
            work_type,
            source_type,
            COUNT(*) AS works,
            SUM(cited_by_count) AS total_citations,
            AVG(cited_by_count) AS mean_citations,
            MEDIAN(cited_by_count) AS median_citations,
            AVG(fwci) AS mean_fwci
        FROM work_output_tags
        GROUP BY
            broad_output_family,
            derived_output_class,
            work_type,
            source_type
        ORDER BY works DESC
        """
    )

    export_query(
        con,
        "top_sources_by_output_class.csv",
        """
        SELECT
            canonical_source_name,
            canonical_source_type,
            derived_source_class,
            broad_output_family,
            derived_output_class,
            COUNT(*) AS works,
            SUM(cited_by_count) AS total_citations,
            AVG(cited_by_count) AS mean_citations,
            MEDIAN(cited_by_count) AS median_citations,
            AVG(fwci) AS mean_fwci
        FROM work_output_tags
        GROUP BY
            canonical_source_name,
            canonical_source_type,
            derived_source_class,
            broad_output_family,
            derived_output_class
        ORDER BY works DESC
        """
    )

    export_query(
        con,
        "missing_source_works_sample.csv",
        """
        SELECT
            work_id,
            doi,
            title,
            publication_year,
            work_type,
            type_crossref,
            primary_topic_name,
            cited_by_count,
            fwci,
            derived_output_class,
            broad_output_family
        FROM work_output_tags
        WHERE is_missing_source = TRUE
        ORDER BY cited_by_count DESC NULLS LAST
        LIMIT 1000
        """
    )

    export_query(
        con,
        "repository_or_preprint_sources.csv",
        """
        SELECT
            canonical_source_name,
            canonical_source_type,
            derived_source_class,
            COUNT(*) AS works,
            SUM(cited_by_count) AS total_citations,
            AVG(cited_by_count) AS mean_citations,
            COUNT(DISTINCT publication_year) AS years_observed,
            MIN(publication_year) AS first_year,
            MAX(publication_year) AS last_year
        FROM work_output_tags
        WHERE is_repository_or_preprint_like = TRUE
        GROUP BY
            canonical_source_name,
            canonical_source_type,
            derived_source_class
        ORDER BY works DESC
        """
    )

    export_query(
        con,
        "book_and_chapter_sources.csv",
        """
        SELECT
            canonical_source_name,
            canonical_source_type,
            derived_source_class,
            COUNT(*) AS works,
            SUM(cited_by_count) AS total_citations,
            AVG(cited_by_count) AS mean_citations,
            COUNT(DISTINCT publication_year) AS years_observed,
            MIN(publication_year) AS first_year,
            MAX(publication_year) AS last_year
        FROM work_output_tags
        WHERE is_book_or_chapter_like = TRUE
        GROUP BY
            canonical_source_name,
            canonical_source_type,
            derived_source_class
        ORDER BY works DESC
        """
    )

    export_query(
        con,
        "weird_source_names_control_chars.csv",
        """
        SELECT DISTINCT
            ws.source_name AS original_source_name,
            ws.source_name_clean,
            HEX(ws.source_name) AS original_source_name_hex,
            HEX(ws.source_name_clean) AS source_name_clean_hex,
            ws.source_type,
            ws.publisher AS original_publisher,
            ws.publisher_clean,
            ws.issn_l,
            COUNT(*) OVER (PARTITION BY ws.source_name) AS works
        FROM work_sources ws
        WHERE ws.source_name IS NOT NULL
          AND REGEXP_MATCHES(ws.source_name, '[\\x00-\\x1F\\x7F-\\x9F]')
        ORDER BY works DESC
        """
    )

    export_query(
        con,
        "possible_source_name_variants.csv",
        """
        SELECT
            LOWER(REGEXP_REPLACE(source_name_clean, '[^a-zA-Z0-9 ]', '', 'g')) AS normalised_source_name,
            COUNT(DISTINCT source_name_clean) AS variants,
            STRING_AGG(DISTINCT source_name_clean, '; ') AS source_names_clean,
            STRING_AGG(DISTINCT source_name, '; ') AS source_names_original,
            STRING_AGG(DISTINCT source_type, '; ') AS source_types,
            COUNT(*) AS works
        FROM work_sources
        WHERE source_name_clean IS NOT NULL
        GROUP BY normalised_source_name
        HAVING COUNT(DISTINCT source_name_clean) > 1
        ORDER BY works DESC
        """
    )

    export_query(
        con,
        "researcher_output_comparison_top500.csv",
        """
        WITH researcher_outputs AS (
            SELECT
                rim.canonical_researcher_id,
                rim.canonical_name,

                COUNT(DISTINCT wot.work_id) AS harvested_outputs,

                COUNT(DISTINCT CASE
                    WHEN wot.is_peer_review_likely THEN wot.work_id END
                ) AS peer_review_likely_outputs,

                COUNT(DISTINCT CASE
                    WHEN wot.is_standard_journal_article THEN wot.work_id END
                ) AS standard_journal_articles,

                COUNT(DISTINCT CASE
                    WHEN wot.is_review_article THEN wot.work_id END
                ) AS review_articles,

                COUNT(DISTINCT CASE
                    WHEN wot.is_book_or_chapter_like THEN wot.work_id END
                ) AS book_or_chapter_outputs,

                COUNT(DISTINCT CASE
                    WHEN wot.is_repository_or_preprint_like THEN wot.work_id END
                ) AS repository_or_preprint_outputs,

                COUNT(DISTINCT CASE
                    WHEN wot.is_grey_literature_likely THEN wot.work_id END
                ) AS grey_literature_outputs,

                COUNT(DISTINCT CASE
                    WHEN wot.is_missing_source THEN wot.work_id END
                ) AS missing_source_outputs,

                SUM(wot.cited_by_count) AS citations_to_harvested_outputs,

                SUM(CASE
                    WHEN wot.is_peer_review_likely THEN wot.cited_by_count ELSE 0 END
                ) AS citations_to_peer_review_likely_outputs

            FROM researcher_identity_map rim
            JOIN researcher_works rw
                ON rim.openalex_author_id = rw.author_id
            JOIN work_output_tags wot
                ON rw.work_id = wot.work_id
            GROUP BY
                rim.canonical_researcher_id,
                rim.canonical_name
        )

        SELECT *
        FROM researcher_outputs
        ORDER BY harvested_outputs DESC
        LIMIT 500
        """
    )

    export_query(
        con,
        "repository_heavy_researchers_top500.csv",
        """
        WITH researcher_outputs AS (
            SELECT
                rim.canonical_researcher_id,
                rim.canonical_name,

                COUNT(DISTINCT wot.work_id) AS harvested_outputs,

                COUNT(DISTINCT CASE
                    WHEN wot.is_peer_review_likely THEN wot.work_id END
                ) AS peer_review_likely_outputs,

                COUNT(DISTINCT CASE
                    WHEN wot.is_repository_or_preprint_like THEN wot.work_id END
                ) AS repository_or_preprint_outputs

            FROM researcher_identity_map rim
            JOIN researcher_works rw
                ON rim.openalex_author_id = rw.author_id
            JOIN work_output_tags wot
                ON rw.work_id = wot.work_id
            GROUP BY
                rim.canonical_researcher_id,
                rim.canonical_name
        )

        SELECT
            *,
            CASE
                WHEN harvested_outputs > 0
                THEN CAST(repository_or_preprint_outputs AS DOUBLE) / harvested_outputs
                ELSE NULL
            END AS repository_share
        FROM researcher_outputs
        WHERE harvested_outputs >= 10
        ORDER BY repository_share DESC NULLS LAST, harvested_outputs DESC
        LIMIT 500
        """
    )

    con.close()

    print()
    print("Publication output review exports complete.")
    print(f"Folder: {EXPORT_DIR}")


if __name__ == "__main__":
    main()