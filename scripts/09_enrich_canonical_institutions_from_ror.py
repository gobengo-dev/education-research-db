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
            "canonical_institutions_enriched",
            "canonical_institution_ror_relationships",
            "canonical_institution_external_ids",
            "canonical_institution_websites",
        ]:
            con.execute(f"DROP TABLE IF EXISTS {table}")

    print("Building canonical_institutions_enriched...")

    con.execute(q("""
        CREATE TABLE canonical_institutions_enriched AS
        WITH ror_name_summary AS (
            SELECT
                ror_id,
                STRING_AGG(DISTINCT name, '; ') AS ror_all_names
            FROM ror_names
            GROUP BY ror_id
        ),

        ror_location_summary AS (
            SELECT
                ror_id,

                ANY_VALUE(city) AS primary_city,
                ANY_VALUE(region) AS primary_region,
                ANY_VALUE(country_code) AS primary_country_code,
                ANY_VALUE(country_name) AS primary_country_name,

                AVG(latitude) AS avg_latitude,
                AVG(longitude) AS avg_longitude,

                COUNT(*) AS location_count

            FROM ror_locations
            GROUP BY ror_id
        ),

        ror_link_summary AS (
            SELECT
                ror_id,
                STRING_AGG(DISTINCT url, '; ') AS all_links
            FROM ror_links
            GROUP BY ror_id
        ),

        ror_domain_summary AS (
            SELECT
                ror_id,
                STRING_AGG(DISTINCT domain, '; ') AS all_domains
            FROM ror_domains
            GROUP BY ror_id
        ),

        ror_relationship_summary AS (
            SELECT
                ror_id,

                COUNT(*) AS total_relationships,

                COUNT(CASE WHEN relationship_type = 'Parent' THEN 1 END)
                    AS parent_relationships,

                COUNT(CASE WHEN relationship_type = 'Child' THEN 1 END)
                    AS child_relationships,

                COUNT(CASE WHEN relationship_type = 'Related' THEN 1 END)
                    AS related_relationships,

                STRING_AGG(
                    DISTINCT related_ror_id || ' [' || relationship_type || ']',
                    '; '
                ) AS relationship_summary

            FROM ror_relationships
            GROUP BY ror_id
        ),

        ror_external_summary AS (
            SELECT
                ror_id,

                COUNT(*) AS external_id_count,

                STRING_AGG(
                    DISTINCT external_id_type || ':' || external_id_value,
                    '; '
                ) AS external_ids_summary

            FROM ror_external_ids
            GROUP BY ror_id
        )

        SELECT
            ci.canonical_institution_id,
            ci.canonical_openalex_institution_id,
            ci.canonical_institution_name,
            ci.canonical_ror,

            ci.canonical_country_code,
            ci.canonical_institution_type,

            ci.name_variants,
            ci.normalised_name_variants,
            ci.country_variants,
            ci.institution_type_variants,

            ci.source_institution_appearance_variants,
            ci.total_affiliation_rows,
            ci.summed_distinct_openalex_author_links,
            ci.summed_distinct_work_links,

            ci.canonicalisation_basis,

            cim.canonical_researchers,
            cim.openalex_author_ids,
            cim.works_linked,
            cim.researcher_institution_work_rows,
            cim.au_nz_canonical_researchers,
            cim.au_nz_works_linked,

            rr.name AS ror_primary_name,
            rr.status AS ror_status,
            rr.established AS ror_established_year,
            rr.types_json AS ror_types_json,

            rns.ror_all_names,

            rls.primary_city,
            rls.primary_region,
            rls.primary_country_code,
            rls.primary_country_name,
            rls.avg_latitude,
            rls.avg_longitude,
            rls.location_count,

            rlk.all_links,
            rds.all_domains,

            rrs.total_relationships,
            rrs.parent_relationships,
            rrs.child_relationships,
            rrs.related_relationships,
            rrs.relationship_summary,

            res.external_id_count,
            res.external_ids_summary,

            CASE
                WHEN rr.ror_id IS NOT NULL THEN TRUE
                ELSE FALSE
            END AS matched_to_ror,

            CASE
                WHEN rr.status = 'active' THEN TRUE
                ELSE FALSE
            END AS ror_active

        FROM canonical_institutions ci

        LEFT JOIN canonical_institution_metrics cim
            ON ci.canonical_institution_id = cim.canonical_institution_id

        LEFT JOIN ror_raw_records rr
            ON ci.canonical_ror = rr.ror_id

        LEFT JOIN ror_name_summary rns
            ON ci.canonical_ror = rns.ror_id

        LEFT JOIN ror_location_summary rls
            ON ci.canonical_ror = rls.ror_id

        LEFT JOIN ror_link_summary rlk
            ON ci.canonical_ror = rlk.ror_id

        LEFT JOIN ror_domain_summary rds
            ON ci.canonical_ror = rds.ror_id

        LEFT JOIN ror_relationship_summary rrs
            ON ci.canonical_ror = rrs.ror_id

        LEFT JOIN ror_external_summary res
            ON ci.canonical_ror = res.ror_id
    """))

    print("Building canonical_institution_ror_relationships...")

    con.execute(q("""
        CREATE TABLE canonical_institution_ror_relationships AS
        SELECT
            cie.canonical_institution_id,
            cie.canonical_institution_name,
            cie.canonical_ror,

            rr.related_ror_id,
            rr.related_label,
            rr.relationship_type,

            cie2.canonical_institution_id AS related_canonical_institution_id,
            cie2.canonical_institution_name AS related_canonical_institution_name,
            cie2.canonical_country_code AS related_country_code

        FROM canonical_institutions_enriched cie

        JOIN ror_relationships rr
            ON cie.canonical_ror = rr.ror_id

        LEFT JOIN canonical_institutions_enriched cie2
            ON rr.related_ror_id = cie2.canonical_ror
    """))

    print("Building canonical_institution_external_ids...")

    con.execute(q("""
        CREATE TABLE canonical_institution_external_ids AS
        SELECT
            cie.canonical_institution_id,
            cie.canonical_institution_name,
            cie.canonical_ror,

            rei.external_id_type,
            rei.external_id_value,
            rei.external_id_preferred

        FROM canonical_institutions_enriched cie

        JOIN ror_external_ids rei
            ON cie.canonical_ror = rei.ror_id
    """))

    print("Building canonical_institution_websites...")

    con.execute(q("""
        CREATE TABLE canonical_institution_websites AS
        SELECT
            cie.canonical_institution_id,
            cie.canonical_institution_name,
            cie.canonical_ror,

            rl.link_type,
            rl.url,

            rd.domain

        FROM canonical_institutions_enriched cie

        LEFT JOIN ror_links rl
            ON cie.canonical_ror = rl.ror_id

        LEFT JOIN ror_domains rd
            ON cie.canonical_ror = rd.ror_id
    """))

    print("Creating indexes...")

    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_cie_id ON canonical_institutions_enriched(canonical_institution_id)",
        "CREATE INDEX IF NOT EXISTS idx_cie_ror ON canonical_institutions_enriched(canonical_ror)",
        "CREATE INDEX IF NOT EXISTS idx_cie_country ON canonical_institutions_enriched(canonical_country_code)",
        "CREATE INDEX IF NOT EXISTS idx_cie_name ON canonical_institutions_enriched(canonical_institution_name)",

        "CREATE INDEX IF NOT EXISTS idx_cirr_inst ON canonical_institution_ror_relationships(canonical_institution_id)",
        "CREATE INDEX IF NOT EXISTS idx_cirr_related ON canonical_institution_ror_relationships(related_ror_id)",

        "CREATE INDEX IF NOT EXISTS idx_ciei_inst ON canonical_institution_external_ids(canonical_institution_id)",
        "CREATE INDEX IF NOT EXISTS idx_ciei_type ON canonical_institution_external_ids(external_id_type)",

        "CREATE INDEX IF NOT EXISTS idx_ciw_inst ON canonical_institution_websites(canonical_institution_id)",
    ]:
        con.execute(sql)

    print()
    print("ROR enrichment complete.")
    print()

    checks = [
        ("canonical_institutions_enriched",
         "SELECT COUNT(*) FROM canonical_institutions_enriched"),

        ("institutions matched to ROR",
         """
         SELECT COUNT(*)
         FROM canonical_institutions_enriched
         WHERE matched_to_ror = TRUE
         """),

        ("active ROR institutions",
         """
         SELECT COUNT(*)
         FROM canonical_institutions_enriched
         WHERE ror_active = TRUE
         """),

        ("canonical_institution_ror_relationships",
         "SELECT COUNT(*) FROM canonical_institution_ror_relationships"),

        ("canonical_institution_external_ids",
         "SELECT COUNT(*) FROM canonical_institution_external_ids"),

        ("canonical_institution_websites",
         "SELECT COUNT(*) FROM canonical_institution_websites"),
    ]

    for label, sql in checks:
        count = con.execute(q(sql)).fetchone()[0]
        print(f"{label}: {count:,}")

    print()
    print("Top AU/NZ institutions with relationship networks:")

    rows = con.execute(q("""
        SELECT
            canonical_institution_name,
            canonical_country_code,
            canonical_researchers,
            works_linked,
            total_relationships,
            parent_relationships,
            child_relationships,
            related_relationships
        FROM canonical_institutions_enriched
        WHERE canonical_country_code IN ('AU', 'NZ')
        ORDER BY total_relationships DESC NULLS LAST,
                 canonical_researchers DESC
        LIMIT 30
    """)).fetchall()

    for row in rows:
        print(row)

    print()
    print("Institutions with inactive/withdrawn ROR records:")

    rows = con.execute(q("""
        SELECT
            canonical_institution_name,
            canonical_country_code,
            canonical_ror,
            ror_status,
            works_linked
        FROM canonical_institutions_enriched
        WHERE ror_status IS NOT NULL
          AND ror_status != 'active'
        ORDER BY works_linked DESC
        LIMIT 30
    """)).fetchall()

    for row in rows:
        print(row)

    print()
    print("Top AU/NZ policy-relevant organisations:")

    rows = con.execute(q("""
        SELECT
            canonical_institution_name,
            canonical_institution_type,
            canonical_country_code,
            canonical_researchers,
            works_linked,
            all_domains
        FROM canonical_institutions_enriched
        WHERE canonical_country_code IN ('AU', 'NZ')
          AND (
                LOWER(canonical_institution_name) LIKE '%education%'
             OR LOWER(canonical_institution_name) LIKE '%curriculum%'
             OR LOWER(canonical_institution_name) LIKE '%assessment%'
             OR LOWER(canonical_institution_name) LIKE '%government%'
             OR LOWER(canonical_institution_name) LIKE '%department%'
             OR LOWER(canonical_institution_name) LIKE '%ministry%'
             OR LOWER(canonical_institution_name) LIKE '%institute%'
          )
        ORDER BY canonical_researchers DESC
        LIMIT 40
    """)).fetchall()

    for row in rows:
        print(row)

    con.close()


if __name__ == "__main__":
    main()