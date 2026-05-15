from pathlib import Path
import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_FILE = PROJECT_ROOT / "data" / "research_database.duckdb"
EXPORT_DIR = PROJECT_ROOT / "data" / "exports" / "institution_review"

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
        "collapsed_institution_variants.csv",
        """
        SELECT
            canonical_institution_id,
            canonical_institution_name,
            canonical_country_code,
            canonical_ror,
            source_institution_appearance_variants,
            total_affiliation_rows,
            name_variants,
            normalised_name_variants,
            institution_type_variants
        FROM canonical_institutions
        WHERE source_institution_appearance_variants > 1
        ORDER BY total_affiliation_rows DESC
        """
    )

    export_query(
        con,
        "same_name_different_ror_review_cases.csv",
        """
        SELECT
            normalised_institution_name,
            country_code,
            canonical_entities,
            total_appearance_rows,
            canonical_names,
            rors
        FROM institution_name_review_cases
        ORDER BY total_appearance_rows DESC
        """
    )

    export_query(
        con,
        "top_au_nz_canonical_institutions.csv",
        """
        SELECT
            canonical_institution_id,
            canonical_institution_name,
            canonical_country_code,
            canonical_ror,
            canonical_institution_type,
            canonical_researchers,
            works_linked,
            source_institution_appearance_variants,
            name_variants
        FROM canonical_institution_metrics
        WHERE canonical_country_code IN ('AU', 'NZ')
        ORDER BY canonical_researchers DESC
        """
    )

    export_query(
        con,
        "au_nz_policy_relevant_institution_candidates.csv",
        """
        SELECT
            canonical_institution_id,
            canonical_institution_name,
            canonical_country_code,
            canonical_ror,
            canonical_institution_type,
            canonical_researchers,
            works_linked,
            name_variants
        FROM canonical_institution_metrics
        WHERE canonical_country_code IN ('AU', 'NZ')
          AND (
                LOWER(canonical_institution_name) LIKE '%education%'
             OR LOWER(canonical_institution_name) LIKE '%school%'
             OR LOWER(canonical_institution_name) LIKE '%department%'
             OR LOWER(canonical_institution_name) LIKE '%ministry%'
             OR LOWER(canonical_institution_name) LIKE '%curriculum%'
             OR LOWER(canonical_institution_name) LIKE '%assessment%'
             OR LOWER(canonical_institution_name) LIKE '%acer%'
             OR LOWER(canonical_institution_name) LIKE '%research council%'
             OR LOWER(canonical_institution_name) LIKE '%institute%'
          )
        ORDER BY canonical_researchers DESC
        """
    )

    export_query(
        con,
        "institution_identity_map_full.csv",
        """
        SELECT
            canonical_institution_id,
            canonical_institution_name,
            canonical_ror,
            canonical_country_code,
            openalex_institution_id,
            openalex_institution_name,
            ror,
            country_code,
            institution_type,
            normalised_institution_name,
            appearance_rows,
            distinct_openalex_authors,
            distinct_works,
            canonicalisation_basis
        FROM institution_identity_map
        ORDER BY canonical_country_code, canonical_institution_name, appearance_rows DESC
        """
    )

    con.close()

    print()
    print("Institution review exports complete.")
    print(f"Folder: {EXPORT_DIR}")


if __name__ == "__main__":
    main()