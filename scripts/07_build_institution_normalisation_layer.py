from pathlib import Path
import re
import unicodedata

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_FILE = PROJECT_ROOT / "data" / "research_database.duckdb"

DROP_EXISTING_TABLES = True


def q(sql: str) -> str:
    return sql.strip()


def normalise_name(name: str | None) -> str:
    if not name:
        return ""

    s = unicodedata.normalize("NFKC", name)
    s = s.lower().strip()
    s = s.replace("&", " and ")
    s = s.replace("’", "'").replace("`", "'")
    s = re.sub(r"[^\w\s']", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r"^the\s+", "", s)

    return s


def main():
    con = duckdb.connect(str(DUCKDB_FILE))

    if DROP_EXISTING_TABLES:
        for table in [
            "institution_identity_map",
            "canonical_institutions",
            "canonical_researcher_institutions",
            "canonical_institution_metrics",
            "institution_name_review_cases",
        ]:
            con.execute(f"DROP TABLE IF EXISTS {table}")

    print("Building institution identity map from researcher_institutions...")

    con.execute(q("""
        CREATE TABLE institution_identity_map AS
        WITH appearances AS (
            SELECT
                institution_id AS openalex_institution_id,
                institution_name AS openalex_institution_name,
                ror,
                country_code,
                institution_type,
                COUNT(*) AS appearance_rows,
                COUNT(DISTINCT author_id) AS distinct_openalex_authors,
                COUNT(DISTINCT work_id) AS distinct_works
            FROM researcher_institutions
            WHERE institution_id IS NOT NULL
               OR ror IS NOT NULL
               OR institution_name IS NOT NULL
            GROUP BY
                institution_id,
                institution_name,
                ror,
                country_code,
                institution_type
        ),

        grouped AS (
            SELECT
                *,
                CASE
                    WHEN ror IS NOT NULL AND ror != ''
                    THEN 'ROR:' || ror
                    ELSE 'OA:' || openalex_institution_id
                END AS institution_group_key
            FROM appearances
        ),

        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY institution_group_key
                    ORDER BY
                        appearance_rows DESC,
                        CASE WHEN ror IS NOT NULL AND ror != '' THEN 1 ELSE 0 END DESC,
                        LENGTH(openalex_institution_name) DESC,
                        openalex_institution_name
                ) AS rn
            FROM grouped
        ),

        canonical_choice AS (
            SELECT
                institution_group_key,
                openalex_institution_id AS canonical_openalex_institution_id,
                openalex_institution_name AS canonical_institution_name,
                ror AS canonical_ror,
                country_code AS canonical_country_code,
                institution_type AS canonical_institution_type
            FROM ranked
            WHERE rn = 1
        ),

        numbered_groups AS (
            SELECT
                institution_group_key,
                'CI' || LPAD(
                    CAST(ROW_NUMBER() OVER (ORDER BY institution_group_key) AS VARCHAR),
                    8,
                    '0'
                ) AS canonical_institution_id
            FROM (
                SELECT DISTINCT institution_group_key
                FROM grouped
            )
        )

        SELECT
            g.openalex_institution_id,
            g.openalex_institution_name,
            g.ror,
            g.country_code,
            g.institution_type,
            g.appearance_rows,
            g.distinct_openalex_authors,
            g.distinct_works,
            g.institution_group_key,

            ng.canonical_institution_id,
            cc.canonical_openalex_institution_id,
            cc.canonical_institution_name,
            cc.canonical_ror,
            cc.canonical_country_code,
            cc.canonical_institution_type,

            CASE
                WHEN g.ror IS NOT NULL AND g.ror != ''
                THEN 'ror'
                ELSE 'openalex_institution_id'
            END AS canonicalisation_basis

        FROM grouped g
        JOIN numbered_groups ng
            ON g.institution_group_key = ng.institution_group_key
        JOIN canonical_choice cc
            ON g.institution_group_key = cc.institution_group_key
    """))

    print("Adding normalised name column...")

    rows = con.execute(q("""
        SELECT
            openalex_institution_id,
            openalex_institution_name,
            ror,
            country_code,
            institution_type,
            appearance_rows,
            distinct_openalex_authors,
            distinct_works,
            institution_group_key,
            canonical_institution_id,
            canonical_openalex_institution_id,
            canonical_institution_name,
            canonical_ror,
            canonical_country_code,
            canonical_institution_type,
            canonicalisation_basis
        FROM institution_identity_map
    """)).fetchall()

    con.execute("DROP TABLE institution_identity_map")

    con.execute(q("""
        CREATE TABLE institution_identity_map (
            openalex_institution_id VARCHAR,
            openalex_institution_name VARCHAR,
            ror VARCHAR,
            country_code VARCHAR,
            institution_type VARCHAR,
            normalised_institution_name VARCHAR,
            appearance_rows BIGINT,
            distinct_openalex_authors BIGINT,
            distinct_works BIGINT,
            institution_group_key VARCHAR,

            canonical_institution_id VARCHAR,
            canonical_openalex_institution_id VARCHAR,
            canonical_institution_name VARCHAR,
            canonical_ror VARCHAR,
            canonical_country_code VARCHAR,
            canonical_institution_type VARCHAR,
            canonicalisation_basis VARCHAR
        )
    """))

    insert_rows = []
    for row in rows:
        (
            openalex_institution_id,
            openalex_institution_name,
            ror,
            country_code,
            institution_type,
            appearance_rows,
            distinct_openalex_authors,
            distinct_works,
            institution_group_key,
            canonical_institution_id,
            canonical_openalex_institution_id,
            canonical_institution_name,
            canonical_ror,
            canonical_country_code,
            canonical_institution_type,
            canonicalisation_basis,
        ) = row

        insert_rows.append((
            openalex_institution_id,
            openalex_institution_name,
            ror,
            country_code,
            institution_type,
            normalise_name(openalex_institution_name),
            appearance_rows,
            distinct_openalex_authors,
            distinct_works,
            institution_group_key,
            canonical_institution_id,
            canonical_openalex_institution_id,
            canonical_institution_name,
            canonical_ror,
            canonical_country_code,
            canonical_institution_type,
            canonicalisation_basis,
        ))

    con.executemany(
        "INSERT INTO institution_identity_map VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        insert_rows,
    )

    print("Building canonical_institutions...")

    con.execute(q("""
        CREATE TABLE canonical_institutions AS
        SELECT
            canonical_institution_id,
            ANY_VALUE(canonical_openalex_institution_id) AS canonical_openalex_institution_id,
            ANY_VALUE(canonical_institution_name) AS canonical_institution_name,
            ANY_VALUE(canonical_ror) AS canonical_ror,
            ANY_VALUE(canonical_country_code) AS canonical_country_code,
            ANY_VALUE(canonical_institution_type) AS canonical_institution_type,

            STRING_AGG(DISTINCT openalex_institution_name, '; ') AS name_variants,
            STRING_AGG(DISTINCT normalised_institution_name, '; ') AS normalised_name_variants,
            STRING_AGG(DISTINCT country_code, '; ') AS country_variants,
            STRING_AGG(DISTINCT institution_type, '; ') AS institution_type_variants,
            STRING_AGG(DISTINCT ror, '; ') AS ror_variants,

            COUNT(*) AS source_institution_appearance_variants,
            SUM(appearance_rows) AS total_affiliation_rows,
            SUM(distinct_openalex_authors) AS summed_distinct_openalex_author_links,
            SUM(distinct_works) AS summed_distinct_work_links,

            ANY_VALUE(canonicalisation_basis) AS canonicalisation_basis

        FROM institution_identity_map
        GROUP BY canonical_institution_id
    """))

    print("Building canonical_researcher_institutions...")

    con.execute(q("""
        CREATE TABLE canonical_researcher_institutions AS
        SELECT
            rim.canonical_researcher_id,
            rim.canonical_name,

            ri.author_id AS openalex_author_id,
            ri.author_name AS openalex_author_name,

            ri.institution_id AS openalex_institution_id,
            ri.institution_name AS openalex_institution_name,
            ri.ror,
            ri.country_code,
            ri.institution_type,
            ri.work_id,

            iim.normalised_institution_name,
            iim.canonical_institution_id,
            iim.canonical_institution_name,
            iim.canonical_ror,
            iim.canonical_country_code,
            iim.canonical_institution_type,
            iim.canonicalisation_basis

        FROM researcher_institutions ri
        JOIN researcher_identity_map rim
            ON ri.author_id = rim.openalex_author_id
        LEFT JOIN institution_identity_map iim
            ON ri.institution_id = iim.openalex_institution_id
           AND COALESCE(ri.ror, '') = COALESCE(iim.ror, '')
           AND COALESCE(ri.institution_name, '') = COALESCE(iim.openalex_institution_name, '')
    """))

    print("Building canonical_institution_metrics...")

    con.execute(q("""
        CREATE TABLE canonical_institution_metrics AS
        WITH counts AS (
            SELECT
                canonical_institution_id,
                COUNT(DISTINCT canonical_researcher_id) AS canonical_researchers,
                COUNT(DISTINCT openalex_author_id) AS openalex_author_ids,
                COUNT(DISTINCT work_id) AS works_linked,
                COUNT(*) AS researcher_institution_work_rows,

                COUNT(DISTINCT CASE
                    WHEN canonical_country_code IN ('AU', 'NZ')
                    THEN canonical_researcher_id END
                ) AS au_nz_canonical_researchers,

                COUNT(DISTINCT CASE
                    WHEN canonical_country_code IN ('AU', 'NZ')
                    THEN work_id END
                ) AS au_nz_works_linked

            FROM canonical_researcher_institutions
            GROUP BY canonical_institution_id
        )

        SELECT
            ci.*,
            COALESCE(c.canonical_researchers, 0) AS canonical_researchers,
            COALESCE(c.openalex_author_ids, 0) AS openalex_author_ids,
            COALESCE(c.works_linked, 0) AS works_linked,
            COALESCE(c.researcher_institution_work_rows, 0) AS researcher_institution_work_rows,
            COALESCE(c.au_nz_canonical_researchers, 0) AS au_nz_canonical_researchers,
            COALESCE(c.au_nz_works_linked, 0) AS au_nz_works_linked

        FROM canonical_institutions ci
        LEFT JOIN counts c
            ON ci.canonical_institution_id = c.canonical_institution_id
    """))

    print("Building institution_name_review_cases...")

    con.execute(q("""
        CREATE TABLE institution_name_review_cases AS
        SELECT
            normalised_institution_name,
            country_code,
            COUNT(DISTINCT canonical_institution_id) AS canonical_entities,
            STRING_AGG(DISTINCT canonical_institution_name, '; ') AS canonical_names,
            STRING_AGG(DISTINCT canonical_ror, '; ') AS rors,
            SUM(appearance_rows) AS total_appearance_rows
        FROM institution_identity_map
        WHERE normalised_institution_name IS NOT NULL
          AND normalised_institution_name != ''
        GROUP BY normalised_institution_name, country_code
        HAVING COUNT(DISTINCT canonical_institution_id) > 1
    """))

    print("Creating indexes...")

    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_iim_openalex ON institution_identity_map(openalex_institution_id)",
        "CREATE INDEX IF NOT EXISTS idx_iim_ror ON institution_identity_map(ror)",
        "CREATE INDEX IF NOT EXISTS idx_iim_canonical ON institution_identity_map(canonical_institution_id)",
        "CREATE INDEX IF NOT EXISTS idx_ci_id ON canonical_institutions(canonical_institution_id)",
        "CREATE INDEX IF NOT EXISTS idx_ci_ror ON canonical_institutions(canonical_ror)",
        "CREATE INDEX IF NOT EXISTS idx_cri_researcher ON canonical_researcher_institutions(canonical_researcher_id)",
        "CREATE INDEX IF NOT EXISTS idx_cri_inst ON canonical_researcher_institutions(canonical_institution_id)",
        "CREATE INDEX IF NOT EXISTS idx_cim_inst ON canonical_institution_metrics(canonical_institution_id)",
    ]:
        con.execute(sql)

    print()
    print("Institution normalisation layer complete.")
    print()

    checks = [
        ("researcher_institutions rows", "SELECT COUNT(*) FROM researcher_institutions"),
        ("institution_identity_map variants", "SELECT COUNT(*) FROM institution_identity_map"),
        ("canonical_institutions", "SELECT COUNT(*) FROM canonical_institutions"),
        ("canonical_researcher_institutions", "SELECT COUNT(*) FROM canonical_researcher_institutions"),
        ("canonical_institution_metrics", "SELECT COUNT(*) FROM canonical_institution_metrics"),
        (
            "canonical institutions with >1 name variant",
            """
            SELECT COUNT(*)
            FROM canonical_institutions
            WHERE source_institution_appearance_variants > 1
            """
        ),
    ]

    for label, sql in checks:
        count = con.execute(q(sql)).fetchone()[0]
        print(f"{label}: {count:,}")

    print()
    print("Top AU/NZ canonical institutions by linked researchers:")
    rows = con.execute(q("""
        SELECT
            canonical_institution_name,
            canonical_country_code,
            canonical_ror,
            source_institution_appearance_variants,
            canonical_researchers,
            works_linked,
            name_variants
        FROM canonical_institution_metrics
        WHERE canonical_country_code IN ('AU', 'NZ')
        ORDER BY canonical_researchers DESC
        LIMIT 30
    """)).fetchall()

    for row in rows:
        print(row)

    print()
    print("Sample collapsed institutions:")
    rows = con.execute(q("""
        SELECT
            canonical_institution_name,
            canonical_country_code,
            canonical_ror,
            source_institution_appearance_variants,
            name_variants,
            total_affiliation_rows
        FROM canonical_institutions
        WHERE source_institution_appearance_variants > 1
        ORDER BY total_affiliation_rows DESC
        LIMIT 30
    """)).fetchall()

    for row in rows:
        print(row)

    print()
    print("Potential remaining same-name/different-ROR review cases:")
    rows = con.execute(q("""
        SELECT
            normalised_institution_name,
            country_code,
            canonical_entities,
            canonical_names,
            rors,
            total_appearance_rows
        FROM institution_name_review_cases
        ORDER BY total_appearance_rows DESC
        LIMIT 30
    """)).fetchall()

    for row in rows:
        print(row)

    con.close()


if __name__ == "__main__":
    main()