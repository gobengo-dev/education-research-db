from pathlib import Path
from datetime import datetime
import csv
import json

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_FILE = PROJECT_ROOT / "data" / "research_database.duckdb"
EXPORT_DIR = PROJECT_ROOT / "data" / "exports" / "database_state_snapshot"

SNAPSHOT_TS = datetime.now().strftime("%Y%m%d_%H%M%S")
SNAPSHOT_DIR = EXPORT_DIR / SNAPSHOT_TS
REPORT_MD = SNAPSHOT_DIR / "database_state_snapshot.md"


def q(sql: str) -> str:
    return sql.strip()


def safe_name(name: str) -> str:
    return (
        name.lower()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("-", "_")
        .replace(":", "")
    )


def table_exists(con, table_name: str) -> bool:
    return con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()[0] > 0


def column_exists(con, table_name: str, column_name: str) -> bool:
    if not table_exists(con, table_name):
        return False

    return con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name = ?
          AND column_name = ?
        """,
        [table_name, column_name],
    ).fetchone()[0] > 0


def fetch_one(con, sql: str):
    try:
        return con.execute(q(sql)).fetchone()
    except Exception as e:
        return ("ERROR", str(e))


def fetch_all(con, sql: str):
    try:
        return con.execute(q(sql)).fetchall()
    except Exception as e:
        return [("ERROR", str(e))]


def export_query(con, filename: str, sql: str):
    path = SNAPSHOT_DIR / filename
    con.execute(q(f"""
        COPY (
            {sql}
        )
        TO '{path}'
        WITH (HEADER, DELIMITER ',')
    """))
    return path


def md_table(headers, rows, max_rows=50):
    rows = rows[:max_rows]

    out = []
    out.append("| " + " | ".join(headers) + " |")
    out.append("| " + " | ".join(["---"] * len(headers)) + " |")

    for row in rows:
        out.append("| " + " | ".join("" if x is None else str(x).replace("|", "\\|") for x in row) + " |")

    return "\n".join(out)


def write_section(lines, title):
    lines.append("")
    lines.append(f"## {title}")
    lines.append("")


def main():
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DUCKDB_FILE))

    lines = []
    lines.append("# Education Research Database State Snapshot")
    lines.append("")
    lines.append(f"Generated: {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"DuckDB file: `{DUCKDB_FILE}`")
    lines.append(f"Export folder: `{SNAPSHOT_DIR}`")
    lines.append("")

    # ------------------------------------------------------------------
    # Database inventory
    # ------------------------------------------------------------------
    write_section(lines, "1. Database inventory")

    tables = fetch_all(con, """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
    """)

    lines.append(f"Total tables/views listed: **{len(tables)}**")
    lines.append("")
    lines.append(md_table(["table_name"], tables, max_rows=300))

    export_query(con, "tables_inventory.csv", """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'main'
        ORDER BY table_name
    """)

    export_query(con, "columns_inventory.csv", """
        SELECT
            table_name,
            column_name,
            data_type,
            ordinal_position
        FROM information_schema.columns
        WHERE table_schema = 'main'
        ORDER BY table_name, ordinal_position
    """)

    # ------------------------------------------------------------------
    # Table row counts
    # ------------------------------------------------------------------
    write_section(lines, "2. Table row counts")

    row_counts = []
    for (table_name,) in tables:
        try:
            n = con.execute(f'SELECT COUNT(*) FROM "{table_name}"').fetchone()[0]
            row_counts.append((table_name, n))
        except Exception as e:
            row_counts.append((table_name, f"ERROR: {e}"))

    row_counts_sorted = sorted(
        row_counts,
        key=lambda x: x[1] if isinstance(x[1], int) else -1,
        reverse=True,
    )

    lines.append(md_table(["table_name", "rows"], row_counts_sorted, max_rows=200))

    row_count_path = SNAPSHOT_DIR / "table_row_counts.csv"
    with row_count_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["table_name", "rows"])
        writer.writerows(row_counts_sorted)

    # ------------------------------------------------------------------
    # Core works summary
    # ------------------------------------------------------------------
    write_section(lines, "3. Core works / output summary")

    if table_exists(con, "work_output_tags"):
        rows = fetch_all(con, """
            SELECT
                COUNT(*) AS works_total,
                COUNT(DISTINCT work_id) AS distinct_work_ids,
                COUNT(doi) AS works_with_doi,
                COUNT(*) - COUNT(doi) AS works_without_doi,
                COUNT(DISTINCT doi) FILTER (WHERE doi IS NOT NULL AND doi != '') AS distinct_dois,
                MIN(publication_year) AS earliest_year,
                MAX(publication_year) AS latest_year
            FROM work_output_tags
        """)
        lines.append(md_table(
            ["works_total", "distinct_work_ids", "works_with_doi", "works_without_doi", "distinct_dois", "earliest_year", "latest_year"],
            rows,
        ))

        export_query(con, "work_output_summary.csv", """
            SELECT
                COUNT(*) AS works_total,
                COUNT(DISTINCT work_id) AS distinct_work_ids,
                COUNT(doi) AS works_with_doi,
                COUNT(*) - COUNT(doi) AS works_without_doi,
                COUNT(DISTINCT doi) FILTER (WHERE doi IS NOT NULL AND doi != '') AS distinct_dois,
                MIN(publication_year) AS earliest_year,
                MAX(publication_year) AS latest_year
            FROM work_output_tags
        """)

        rows = fetch_all(con, """
            SELECT
                broad_output_family,
                COUNT(*) AS works
            FROM work_output_tags
            GROUP BY broad_output_family
            ORDER BY works DESC
        """)
        lines.append("")
        lines.append("### Broad output families")
        lines.append(md_table(["broad_output_family", "works"], rows, max_rows=100))
        export_query(con, "broad_output_families.csv", """
            SELECT
                broad_output_family,
                COUNT(*) AS works
            FROM work_output_tags
            GROUP BY broad_output_family
            ORDER BY works DESC
        """)

        rows = fetch_all(con, """
            SELECT
                derived_output_class,
                COUNT(*) AS works
            FROM work_output_tags
            GROUP BY derived_output_class
            ORDER BY works DESC
        """)
        lines.append("")
        lines.append("### Derived output classes")
        lines.append(md_table(["derived_output_class", "works"], rows, max_rows=100))
        export_query(con, "derived_output_classes.csv", """
            SELECT
                derived_output_class,
                COUNT(*) AS works
            FROM work_output_tags
            GROUP BY derived_output_class
            ORDER BY works DESC
        """)

        if column_exists(con, "work_output_tags", "publication_year"):
            export_query(con, "works_by_year.csv", """
                SELECT
                    publication_year,
                    COUNT(*) AS works,
                    COUNT(doi) AS works_with_doi,
                    COUNT(*) - COUNT(doi) AS works_without_doi
                FROM work_output_tags
                GROUP BY publication_year
                ORDER BY publication_year
            """)

            rows = fetch_all(con, """
                SELECT
                    publication_year,
                    COUNT(*) AS works
                FROM work_output_tags
                WHERE publication_year >= 2010
                GROUP BY publication_year
                ORDER BY publication_year
            """)
            lines.append("")
            lines.append("### Works by year, 2010 onward")
            lines.append(md_table(["publication_year", "works"], rows, max_rows=30))

    else:
        lines.append("`work_output_tags` not found.")

    # ------------------------------------------------------------------
    # Works flat and source metadata
    # ------------------------------------------------------------------
    write_section(lines, "4. OpenAlex/source layer summary")

    if table_exists(con, "works_flat"):
        rows = fetch_all(con, """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT work_id) AS distinct_works,
                COUNT(doi) AS with_doi,
                MIN(publication_year) AS earliest_year,
                MAX(publication_year) AS latest_year
            FROM works_flat
        """)
        lines.append("### works_flat")
        lines.append(md_table(["rows", "distinct_works", "with_doi", "earliest_year", "latest_year"], rows))
        export_query(con, "works_flat_summary.csv", """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT work_id) AS distinct_works,
                COUNT(doi) AS with_doi,
                MIN(publication_year) AS earliest_year,
                MAX(publication_year) AS latest_year
            FROM works_flat
        """)

        if column_exists(con, "works_flat", "work_type"):
            rows = fetch_all(con, """
                SELECT work_type, COUNT(*) AS works
                FROM works_flat
                GROUP BY work_type
                ORDER BY works DESC
            """)
            lines.append("")
            lines.append("### OpenAlex work_type")
            lines.append(md_table(["work_type", "works"], rows, max_rows=100))
            export_query(con, "openalex_work_types.csv", """
                SELECT work_type, COUNT(*) AS works
                FROM works_flat
                GROUP BY work_type
                ORDER BY works DESC
            """)

    if table_exists(con, "work_sources"):
        rows = fetch_all(con, """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT work_id) AS works_with_source_rows,
                COUNT(source_name) AS rows_with_source_name,
                COUNT(issn_l) AS rows_with_issn_l,
                COUNT(issn) AS rows_with_issn
            FROM work_sources
        """)
        lines.append("")
        lines.append("### work_sources")
        lines.append(md_table(["rows", "works_with_source_rows", "rows_with_source_name", "rows_with_issn_l", "rows_with_issn"], rows))
        export_query(con, "work_sources_summary.csv", """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT work_id) AS works_with_source_rows,
                COUNT(source_name) AS rows_with_source_name,
                COUNT(issn_l) AS rows_with_issn_l,
                COUNT(issn) AS rows_with_issn
            FROM work_sources
        """)

        export_query(con, "top_work_sources.csv", """
            SELECT
                source_name,
                source_type,
                publisher,
                issn_l,
                COUNT(*) AS works
            FROM work_sources
            GROUP BY source_name, source_type, publisher, issn_l
            ORDER BY works DESC
            LIMIT 250
        """)

    if table_exists(con, "canonical_sources"):
        export_query(con, "top_canonical_sources.csv", """
            SELECT *
            FROM canonical_sources
            ORDER BY works_linked DESC NULLS LAST
            LIMIT 250
        """)

    # ------------------------------------------------------------------
    # Crossref enrichment
    # ------------------------------------------------------------------
    write_section(lines, "5. Crossref enrichment summary")

    if table_exists(con, "crossref_enrichment_raw"):
        rows = fetch_all(con, """
            SELECT
                COUNT(*) AS raw_rows,
                COUNT(*) FILTER (WHERE response_status = 200) AS successful,
                COUNT(*) FILTER (WHERE response_status != 200) AS non_successful,
                COUNT(raw_json) AS rows_with_raw_json
            FROM crossref_enrichment_raw
        """)
        lines.append(md_table(["raw_rows", "successful", "non_successful", "rows_with_raw_json"], rows))
        export_query(con, "crossref_raw_summary.csv", """
            SELECT
                COUNT(*) AS raw_rows,
                COUNT(*) FILTER (WHERE response_status = 200) AS successful,
                COUNT(*) FILTER (WHERE response_status != 200) AS non_successful,
                COUNT(raw_json) AS rows_with_raw_json
            FROM crossref_enrichment_raw
        """)

        rows = fetch_all(con, """
            SELECT
                response_status,
                COUNT(*) AS rows
            FROM crossref_enrichment_raw
            GROUP BY response_status
            ORDER BY rows DESC
        """)
        lines.append("")
        lines.append("### Crossref response status")
        lines.append(md_table(["response_status", "rows"], rows, max_rows=50))
        export_query(con, "crossref_response_status.csv", """
            SELECT
                response_status,
                COUNT(*) AS rows
            FROM crossref_enrichment_raw
            GROUP BY response_status
            ORDER BY rows DESC
        """)

    if table_exists(con, "crossref_enrichment_flat"):
        rows = fetch_all(con, """
            SELECT
                COUNT(*) AS rows,
                COUNT(container_title) AS with_container_title,
                COUNT(issn) AS with_issn,
                COUNT(isbn) AS with_isbn,
                COUNT(publisher) AS with_publisher,
                COUNT(reference_count) AS with_reference_count,
                COUNT(is_referenced_by_count) AS with_crossref_cited_by_count,
                SUM(author_count) AS total_authors_counted,
                SUM(editor_count) AS total_editors_counted
            FROM crossref_enrichment_flat
        """)
        lines.append("")
        lines.append("### crossref_enrichment_flat")
        lines.append(md_table(
            [
                "rows",
                "with_container_title",
                "with_issn",
                "with_isbn",
                "with_publisher",
                "with_reference_count",
                "with_crossref_cited_by_count",
                "total_authors_counted",
                "total_editors_counted",
            ],
            rows,
        ))
        export_query(con, "crossref_flat_summary.csv", """
            SELECT
                COUNT(*) AS rows,
                COUNT(container_title) AS with_container_title,
                COUNT(issn) AS with_issn,
                COUNT(isbn) AS with_isbn,
                COUNT(publisher) AS with_publisher,
                COUNT(reference_count) AS with_reference_count,
                COUNT(is_referenced_by_count) AS with_crossref_cited_by_count,
                SUM(author_count) AS total_authors_counted,
                SUM(editor_count) AS total_editors_counted
            FROM crossref_enrichment_flat
        """)

        rows = fetch_all(con, """
            SELECT
                crossref_type,
                COUNT(*) AS works
            FROM crossref_enrichment_flat
            GROUP BY crossref_type
            ORDER BY works DESC
        """)
        lines.append("")
        lines.append("### Crossref types")
        lines.append(md_table(["crossref_type", "works"], rows, max_rows=100))
        export_query(con, "crossref_types.csv", """
            SELECT
                crossref_type,
                COUNT(*) AS works
            FROM crossref_enrichment_flat
            GROUP BY crossref_type
            ORDER BY works DESC
        """)

        export_query(con, "top_crossref_containers.csv", """
            SELECT
                container_title,
                publisher,
                crossref_type,
                issn,
                COUNT(*) AS works,
                MIN(published_year) AS first_year,
                MAX(published_year) AS last_year
            FROM crossref_enrichment_flat
            WHERE container_title IS NOT NULL
            GROUP BY container_title, publisher, crossref_type, issn
            ORDER BY works DESC
            LIMIT 250
        """)

        export_query(con, "top_crossref_publishers.csv", """
            SELECT
                publisher,
                COUNT(*) AS works,
                COUNT(DISTINCT container_title) AS containers
            FROM crossref_enrichment_flat
            GROUP BY publisher
            ORDER BY works DESC
            LIMIT 250
        """)

        export_query(con, "crossref_sample_2000.csv", """
            SELECT
                work_id,
                doi,
                doi_clean,
                crossref_type,
                crossref_title,
                container_title,
                publisher,
                member,
                published_year,
                issn,
                isbn,
                volume,
                issue,
                page,
                article_number,
                reference_count,
                is_referenced_by_count,
                author_count,
                editor_count,
                raw_has_author,
                raw_has_editor
            FROM crossref_enrichment_flat
            USING SAMPLE 2000 ROWS
        """)

    # ------------------------------------------------------------------
    # Institution layer
    # ------------------------------------------------------------------
    write_section(lines, "6. Institution / ROR layer summary")

    for table_name in [
        "canonical_institutions",
        "institution_identity_map",
        "canonical_researcher_institutions",
        "canonical_institution_metrics",
        "canonical_institutions_enriched",
        "ror_raw_records",
    ]:
        if table_exists(con, table_name):
            n = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            lines.append(f"- `{table_name}`: **{n:,}** rows")

    if table_exists(con, "canonical_institution_metrics"):
        export_query(con, "top_canonical_institutions_by_researchers.csv", """
            SELECT *
            FROM canonical_institution_metrics
            ORDER BY canonical_researchers DESC NULLS LAST
            LIMIT 250
        """)

    if table_exists(con, "canonical_institutions_enriched"):
    	export_query(con, "top_au_nz_institutions_enriched.csv", """
        	SELECT *
        	FROM canonical_institutions_enriched
       		WHERE canonical_country_code IN ('AU', 'NZ')
           		OR primary_country_code IN ('AU', 'NZ')
        	ORDER BY canonical_researchers DESC NULLS LAST
        	LIMIT 250
    	""")

    if table_exists(con, "ror_raw_records"):
        rows = fetch_all(con, """
            SELECT
                status,
                COUNT(*) AS records
            FROM ror_raw_records
            GROUP BY status
            ORDER BY records DESC
        """)
        lines.append("")
        lines.append("### ROR status breakdown")
        lines.append(md_table(["status", "records"], rows, max_rows=20))
        export_query(con, "ror_status_breakdown.csv", """
            SELECT
                status,
                COUNT(*) AS records
            FROM ror_raw_records
            GROUP BY status
            ORDER BY records DESC
        """)

    # ------------------------------------------------------------------
    # Researcher/person layer
    # ------------------------------------------------------------------
    write_section(lines, "7. Researcher / author layer summary")

    likely_researcher_tables = [
        "researchers",
        "researchers_flat",
        "canonical_researchers",
        "researcher_works",
        "work_authorships",
        "author_works",
        "orcid_enrichment_raw",
        "orcid_enrichment_flat",
        "crossref_contributors",
    ]

    found_any = False
    for table_name in likely_researcher_tables:
        if table_exists(con, table_name):
            found_any = True
            n = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            lines.append(f"- `{table_name}`: **{n:,}** rows")

    if not found_any:
        lines.append("No expected researcher/person tables found by scripted name check.")

    # Generic exports for common found tables
    if table_exists(con, "canonical_researchers"):
        export_query(con, "canonical_researchers_sample.csv", """
            SELECT *
            FROM canonical_researchers
            LIMIT 250
        """)

    if table_exists(con, "orcid_enrichment_raw"):
        export_query(con, "orcid_enrichment_status.csv", """
            SELECT
                response_status,
                COUNT(*) AS rows
            FROM orcid_enrichment_raw
            GROUP BY response_status
            ORDER BY rows DESC
        """)

    # ------------------------------------------------------------------
    # Journal registry
    # ------------------------------------------------------------------
    write_section(lines, "8. Journal registry / coverage summary")

    if table_exists(con, "journal_registry"):
        rows = fetch_all(con, """
            SELECT
                COUNT(*) AS journals,
                COUNT(*) FILTER (WHERE active = TRUE) AS active_journals,
                COUNT(*) FILTER (WHERE priority_band = 'core') AS core_journals,
                COUNT(*) FILTER (WHERE priority_band = 'adjacent') AS adjacent_journals
            FROM journal_registry
        """)
        lines.append(md_table(["journals", "active_journals", "core_journals", "adjacent_journals"], rows))

        export_query(con, "journal_registry.csv", """
            SELECT *
            FROM journal_registry
            ORDER BY priority_band, country, journal_type, title
        """)

    if table_exists(con, "journal_registry_current_coverage"):
        rows = fetch_all(con, """
            SELECT
                COUNT(*) AS matched_registry_journals,
                SUM(works_in_current_db) AS works_in_current_db,
                SUM(works_2015_onward) AS works_2015_onward
            FROM journal_registry_current_coverage
        """)
        lines.append("")
        lines.append("### Journal registry current coverage")
        lines.append(md_table(["matched_registry_journals", "works_in_current_db", "works_2015_onward"], rows))

        export_query(con, "journal_registry_current_coverage.csv", """
            SELECT *
            FROM journal_registry_current_coverage
            ORDER BY works_in_current_db DESC
        """)

    if table_exists(con, "journal_registry_missing_from_current_db"):
        export_query(con, "journal_registry_missing_from_current_db.csv", """
            SELECT *
            FROM journal_registry_missing_from_current_db
            ORDER BY priority_band, country, title
        """)

    # ------------------------------------------------------------------
    # Missingness / unresolved
    # ------------------------------------------------------------------
    write_section(lines, "9. Missingness and unresolved work queues")

    if table_exists(con, "work_output_tags"):
        rows = fetch_all(con, """
            SELECT
                derived_output_class,
                COUNT(*) AS works_without_doi
            FROM work_output_tags
            WHERE doi IS NULL OR doi = ''
            GROUP BY derived_output_class
            ORDER BY works_without_doi DESC
        """)
        lines.append("### Works without DOI by derived output class")
        lines.append(md_table(["derived_output_class", "works_without_doi"], rows, max_rows=100))
        export_query(con, "works_without_doi_by_class.csv", """
            SELECT
                derived_output_class,
                COUNT(*) AS works_without_doi
            FROM work_output_tags
            WHERE doi IS NULL OR doi = ''
            GROUP BY derived_output_class
            ORDER BY works_without_doi DESC
        """)

        export_query(con, "works_without_doi_sample.csv", """
            SELECT
                work_id,
                title,
                publication_year,
                work_type,
                derived_output_class,
                broad_output_family,
                canonical_source_name,
                source_type
            FROM work_output_tags
            WHERE doi IS NULL OR doi = ''
            USING SAMPLE 1000 ROWS
        """)

    if table_exists(con, "crossref_enrichment_raw"):
        export_query(con, "crossref_non_successful_sample.csv", """
            SELECT
                work_id,
                doi,
                doi_clean,
                retrieved_at,
                response_status,
                error_message
            FROM crossref_enrichment_raw
            WHERE response_status != 200
            ORDER BY response_status, doi_clean
            LIMIT 1000
        """)

    # ------------------------------------------------------------------
    # Suggested next prompt payload
    # ------------------------------------------------------------------
    write_section(lines, "10. Files exported")

    files = sorted([p.name for p in SNAPSHOT_DIR.glob("*.csv")])
    lines.append("The following CSV files were exported:")
    lines.append("")
    for file in files:
        lines.append(f"- `{file}`")

    lines.append("")
    lines.append("## 11. Suggested use")
    lines.append("")
    lines.append("Upload this markdown report and selected CSV files back into ChatGPT to request a full descriptive account of the database state.")
    lines.append("Most useful files to upload with the report:")
    lines.append("")
    lines.append("- `table_row_counts.csv`")
    lines.append("- `work_output_summary.csv`")
    lines.append("- `derived_output_classes.csv`")
    lines.append("- `crossref_flat_summary.csv`")
    lines.append("- `crossref_types.csv`")
    lines.append("- `crossref_response_status.csv`")
    lines.append("- `journal_registry_current_coverage.csv`")
    lines.append("- `works_without_doi_by_class.csv`")
    lines.append("- `top_crossref_containers.csv`")
    lines.append("- `top_canonical_sources.csv` if present")
    lines.append("")

    REPORT_MD.write_text("\n".join(lines), encoding="utf-8")

    print()
    print("Database state snapshot complete.")
    print(f"Snapshot folder: {SNAPSHOT_DIR}")
    print(f"Markdown report: {REPORT_MD}")
    print()
    print("Most useful file to paste/upload first:")
    print(REPORT_MD)

    con.close()


if __name__ == "__main__":
    main()