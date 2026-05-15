from pathlib import Path
import json

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_FILE = PROJECT_ROOT / "data" / "research_database.duckdb"
ROR_DIR = PROJECT_ROOT / "data" / "reference" / "ror"

ROR_JSON_FILE = ROR_DIR / "v2.7-2026-05-12-ror-data.json"

DROP_EXISTING_TABLES = True


def q(sql: str) -> str:
    return sql.strip()


def as_json(value):
    return json.dumps(value, ensure_ascii=False) if value is not None else None


def listify(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def main():
    if not ROR_JSON_FILE.exists():
        raise FileNotFoundError(f"Cannot find ROR JSON file: {ROR_JSON_FILE}")

    con = duckdb.connect(str(DUCKDB_FILE))

    if DROP_EXISTING_TABLES:
        for table in [
            "ror_raw_records",
            "ror_names",
            "ror_aliases",
            "ror_acronyms",
            "ror_external_ids",
            "ror_relationships",
            "ror_locations",
            "ror_links",
            "ror_domains",
        ]:
            con.execute(f"DROP TABLE IF EXISTS {table}")

    print(f"Loading ROR dump: {ROR_JSON_FILE}")

    with open(ROR_JSON_FILE, "r", encoding="utf-8") as f:
        records = json.load(f)

    print(f"ROR records loaded: {len(records):,}")

    con.execute(q("""
        CREATE TABLE ror_raw_records (
            ror_id VARCHAR PRIMARY KEY,
            name VARCHAR,
            status VARCHAR,
            established INTEGER,
            types_json VARCHAR,
            raw_json VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE ror_names (
            ror_id VARCHAR,
            name VARCHAR,
            language VARCHAR,
            types_json VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE ror_aliases (
            ror_id VARCHAR,
            alias VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE ror_acronyms (
            ror_id VARCHAR,
            acronym VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE ror_external_ids (
            ror_id VARCHAR,
            external_id_type VARCHAR,
            external_id_value VARCHAR,
            external_id_preferred BOOLEAN,
            all_external_id_values_json VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE ror_relationships (
            ror_id VARCHAR,
            related_ror_id VARCHAR,
            related_label VARCHAR,
            relationship_type VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE ror_locations (
            ror_id VARCHAR,
            geonames_id INTEGER,
            city VARCHAR,
            region VARCHAR,
            country_code VARCHAR,
            country_name VARCHAR,
            latitude DOUBLE,
            longitude DOUBLE
        )
    """))

    con.execute(q("""
        CREATE TABLE ror_links (
            ror_id VARCHAR,
            link_type VARCHAR,
            url VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE ror_domains (
            ror_id VARCHAR,
            domain VARCHAR
        )
    """))

    raw_rows = []
    name_rows = []
    alias_rows = []
    acronym_rows = []
    external_id_rows = []
    relationship_rows = []
    location_rows = []
    link_rows = []
    domain_rows = []

    for rec in records:
        ror_id = rec.get("id")
        if not ror_id:
            continue

        name = rec.get("name")
        status = rec.get("status")
        established = rec.get("established")
        types = rec.get("types")

        raw_rows.append((
            ror_id,
            name,
            status,
            established,
            as_json(types),
            as_json(rec),
        ))

        for item in listify(rec.get("names")):
            if not isinstance(item, dict):
                continue

            name_rows.append((
                ror_id,
                item.get("value"),
                item.get("lang"),
                as_json(item.get("types")),
            ))

        for alias in listify(rec.get("aliases")):
            if isinstance(alias, str):
                alias_rows.append((ror_id, alias))
            elif isinstance(alias, dict):
                value = alias.get("value")
                if value:
                    alias_rows.append((ror_id, value))

        for acronym in listify(rec.get("acronyms")):
            if isinstance(acronym, str):
                acronym_rows.append((ror_id, acronym))
            elif isinstance(acronym, dict):
                value = acronym.get("value")
                if value:
                    acronym_rows.append((ror_id, value))

        external_ids = rec.get("external_ids") or {}

        if isinstance(external_ids, dict):
            for ext_type, ext_value in external_ids.items():
                preferred = None
                values = []

                if isinstance(ext_value, dict):
                    preferred = ext_value.get("preferred")
                    values = listify(ext_value.get("all"))
                elif isinstance(ext_value, list):
                    values = ext_value
                elif isinstance(ext_value, str):
                    values = [ext_value]

                for val in values:
                    external_id_rows.append((
                        ror_id,
                        ext_type,
                        str(val) if val is not None else None,
                        bool(preferred) if preferred is not None else None,
                        as_json(values),
                    ))

        for rel in listify(rec.get("relationships")):
            if not isinstance(rel, dict):
                continue

            related = rel.get("id")
            label = rel.get("label")
            rel_type = rel.get("type")

            relationship_rows.append((
                ror_id,
                related,
                label,
                rel_type,
            ))

        for loc in listify(rec.get("locations")):
            if not isinstance(loc, dict):
                continue

            geonames = loc.get("geonames_details") or {}
            coordinates = loc.get("coordinates") or {}

            location_rows.append((
                ror_id,
                loc.get("geonames_id"),
                geonames.get("name"),
                geonames.get("region_name"),
                geonames.get("country_code"),
                geonames.get("country_name"),
                coordinates.get("lat"),
                coordinates.get("lng"),
            ))

        for link in listify(rec.get("links")):
            if isinstance(link, str):
                link_rows.append((ror_id, "website", link))
            elif isinstance(link, dict):
                link_rows.append((
                    ror_id,
                    link.get("type"),
                    link.get("value"),
                ))

        for domain in listify(rec.get("domains")):
            if domain:
                domain_rows.append((ror_id, str(domain)))

    print("Writing ROR tables...")

    con.executemany(
        "INSERT INTO ror_raw_records VALUES (?, ?, ?, ?, ?, ?)",
        raw_rows,
    )

    if name_rows:
        con.executemany("INSERT INTO ror_names VALUES (?, ?, ?, ?)", name_rows)

    if alias_rows:
        con.executemany("INSERT INTO ror_aliases VALUES (?, ?)", alias_rows)

    if acronym_rows:
        con.executemany("INSERT INTO ror_acronyms VALUES (?, ?)", acronym_rows)

    if external_id_rows:
        con.executemany(
            "INSERT INTO ror_external_ids VALUES (?, ?, ?, ?, ?)",
            external_id_rows,
        )

    if relationship_rows:
        con.executemany(
            "INSERT INTO ror_relationships VALUES (?, ?, ?, ?)",
            relationship_rows,
        )

    if location_rows:
        con.executemany(
            "INSERT INTO ror_locations VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            location_rows,
        )

    if link_rows:
        con.executemany("INSERT INTO ror_links VALUES (?, ?, ?)", link_rows)

    if domain_rows:
        con.executemany("INSERT INTO ror_domains VALUES (?, ?)", domain_rows)

    print("Creating indexes...")

    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_ror_raw_id ON ror_raw_records(ror_id)",
        "CREATE INDEX IF NOT EXISTS idx_ror_raw_status ON ror_raw_records(status)",
        "CREATE INDEX IF NOT EXISTS idx_ror_names_id ON ror_names(ror_id)",
        "CREATE INDEX IF NOT EXISTS idx_ror_names_name ON ror_names(name)",
        "CREATE INDEX IF NOT EXISTS idx_ror_aliases_id ON ror_aliases(ror_id)",
        "CREATE INDEX IF NOT EXISTS idx_ror_external_id ON ror_external_ids(ror_id)",
        "CREATE INDEX IF NOT EXISTS idx_ror_external_type ON ror_external_ids(external_id_type)",
        "CREATE INDEX IF NOT EXISTS idx_ror_relationships_id ON ror_relationships(ror_id)",
        "CREATE INDEX IF NOT EXISTS idx_ror_relationships_related ON ror_relationships(related_ror_id)",
        "CREATE INDEX IF NOT EXISTS idx_ror_locations_id ON ror_locations(ror_id)",
        "CREATE INDEX IF NOT EXISTS idx_ror_locations_country ON ror_locations(country_code)",
        "CREATE INDEX IF NOT EXISTS idx_ror_domains_id ON ror_domains(ror_id)",
    ]:
        con.execute(sql)

    print()
    print("ROR ingest complete.")
    print()

    checks = [
        ("ror_raw_records", "SELECT COUNT(*) FROM ror_raw_records"),
        ("ror_names", "SELECT COUNT(*) FROM ror_names"),
        ("ror_aliases", "SELECT COUNT(*) FROM ror_aliases"),
        ("ror_acronyms", "SELECT COUNT(*) FROM ror_acronyms"),
        ("ror_external_ids", "SELECT COUNT(*) FROM ror_external_ids"),
        ("ror_relationships", "SELECT COUNT(*) FROM ror_relationships"),
        ("ror_locations", "SELECT COUNT(*) FROM ror_locations"),
        ("ror_links", "SELECT COUNT(*) FROM ror_links"),
        ("ror_domains", "SELECT COUNT(*) FROM ror_domains"),
    ]

    for label, sql in checks:
        count = con.execute(sql).fetchone()[0]
        print(f"{label}: {count:,}")

    print()
    print("ROR record status breakdown:")
    for row in con.execute(q("""
        SELECT status, COUNT(*) AS n
        FROM ror_raw_records
        GROUP BY status
        ORDER BY n DESC
    """)).fetchall():
        print(row)

    print()
    print("Top countries in ROR dump:")
    for row in con.execute(q("""
        SELECT country_code, country_name, COUNT(*) AS n
        FROM ror_locations
        GROUP BY country_code, country_name
        ORDER BY n DESC
        LIMIT 20
    """)).fetchall():
        print(row)

    con.close()


if __name__ == "__main__":
    main()