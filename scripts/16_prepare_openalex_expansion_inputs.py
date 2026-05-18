from pathlib import Path
from datetime import datetime
import csv
import hashlib
import json
import re

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_FILE = PROJECT_ROOT / "data" / "research_database.duckdb"

TOPICS_CSV = PROJECT_ROOT / "data" / "curated" / "openalex" / "OpenAlex_topic_selection_run2.csv"
JOURNALS_CSV = PROJECT_ROOT / "data" / "curated" / "journals" / "AU-NZ_journals.csv"

MIN_PUBLICATION_YEAR = 2016
COUNTRY_CODES = ["AU", "NZ"]


def q(sql: str) -> str:
    return sql.strip()


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def clean(value):
    if value is None:
        return ""
    return str(value).strip()


def short_openalex_id(value: str, prefix: str = "") -> str:
    value = clean(value)
    if not value:
        return ""
    value = value.rstrip("/")
    if "/" in value:
        value = value.rsplit("/", 1)[-1]
    if prefix and value.isdigit():
        return f"{prefix}{value}"
    return value


def normalise_issn(value: str) -> str:
    value = clean(value).upper()
    if not value or value.lower() == "nan":
        return ""
    value = re.sub(r"[^0-9X]", "", value)
    if len(value) != 8:
        return ""
    return f"{value[:4]}-{value[4:]}"


def make_id(prefix: str, *parts: str) -> str:
    text = "|".join(clean(p) for p in parts)
    digest = hashlib.sha1(text.encode("utf-8")).hexdigest()[:16]
    return f"{prefix}_{digest}"


def create_support_tables(con):
    con.execute(q("""
        CREATE TABLE IF NOT EXISTS openalex_expansion_topics (
            topic_id VARCHAR PRIMARY KEY,
            topic_display_name VARCHAR,
            education_relevance_tier VARCHAR,
            include_decision VARCHAR,
            include_decision_run2 VARCHAR,
            subfield_id VARCHAR,
            subfield_name VARCHAR,
            field_id VARCHAR,
            field_name VARCHAR,
            domain_id VARCHAR,
            domain_name VARCHAR,
            keywords VARCHAR,
            summary VARCHAR,
            wikipedia_url VARCHAR,
            min_publication_year INTEGER,
            country_filter VARCHAR,
            loaded_at VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS openalex_journal_targets (
            journal_target_id VARCHAR PRIMARY KEY,
            canonical_title VARCHAR,
            alternate_historical_titles VARCHAR,
            family_id VARCHAR,
            predecessor_successor_links VARCHAR,
            print_issn VARCHAR,
            electronic_issn VARCHAR,
            country VARCHAR,
            publisher VARCHAR,
            society_or_association VARCHAR,
            active_status VARCHAR,
            oa_status VARCHAR,
            doi_usage_quality VARCHAR,
            indexing_presence VARCHAR,
            likely_metadata_quality VARCHAR,
            hosting_platform VARCHAR,
            harvest_difficulty VARCHAR,
            likely_covered_by_openalex VARCHAR,
            likely_underrepresented_in_global VARCHAR,
            hosted_via_ojs VARCHAR,
            harvestable_from_static_archives VARCHAR,
            relevance_classification VARCHAR,
            acquisition_priority VARCHAR,
            notes VARCHAR,
            min_publication_year INTEGER,
            loaded_at VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS openalex_journal_target_identifiers (
            journal_target_id VARCHAR,
            canonical_title VARCHAR,
            identifier_type VARCHAR,
            identifier_value VARCHAR,
            loaded_at VARCHAR,
            PRIMARY KEY (journal_target_id, identifier_type, identifier_value)
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS openalex_journal_source_resolutions (
            journal_target_id VARCHAR,
            canonical_title VARCHAR,
            issn VARCHAR,
            openalex_source_id VARCHAR,
            source_display_name VARCHAR,
            source_type VARCHAR,
            issn_l VARCHAR,
            issn_json VARCHAR,
            host_organization_name VARCHAR,
            works_count BIGINT,
            cited_by_count BIGINT,
            resolution_method VARCHAR,
            resolved_at VARCHAR,
            raw_json VARCHAR,
            PRIMARY KEY (journal_target_id, issn, openalex_source_id)
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS source_record_discovery_events (
            discovery_event_id VARCHAR PRIMARY KEY,
            harvest_run_id VARCHAR,
            harvest_source_id VARCHAR,
            source_system VARCHAR,
            source_record_id VARCHAR,
            source_record_type VARCHAR,
            discovery_context VARCHAR,
            discovery_key VARCHAR,
            discovered_at VARCHAR,
            already_present BOOLEAN
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS harvest_seen_work_openalex_sources (
            work_id VARCHAR,
            harvest_source_id VARCHAR,
            source_system VARCHAR,
            source_type VARCHAR,
            source_identifier VARCHAR,
            source_display_name VARCHAR,
            first_seen_at VARCHAR,
            latest_seen_at VARCHAR,
            seen_count BIGINT,
            PRIMARY KEY (work_id, harvest_source_id)
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS openalex_raw_harvest_records (
            raw_harvest_record_id VARCHAR PRIMARY KEY,
            harvest_run_id VARCHAR,
            harvest_source_id VARCHAR,
            work_id VARCHAR,
            retrieved_at VARCHAR,
            response_status INTEGER,
            already_present BOOLEAN,
            raw_json VARCHAR,
            content_hash VARCHAR
        )
    """))


def load_topics(con):
    if not TOPICS_CSV.exists():
        raise FileNotFoundError(f"Topic CSV not found: {TOPICS_CSV}")

    loaded_at = now_iso()
    selected = []

    with TOPICS_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            include_run2 = clean(row.get("include_decision_run2")).lower()
            if include_run2 not in {"y", "yes", "true", "1", "include", "included"}:
                continue

            topic_id = short_openalex_id(row.get("topic_id"), prefix="T")
            if not topic_id:
                continue

            selected.append((
                topic_id,
                clean(row.get("topic_name")),
                clean(row.get("education_relevance_tier")),
                clean(row.get("include_decision")),
                clean(row.get("include_decision_run2")),
                clean(row.get("subfield_id")),
                clean(row.get("subfield_name")),
                clean(row.get("field_id")),
                clean(row.get("field_name")),
                clean(row.get("domain_id")),
                clean(row.get("domain_name")),
                clean(row.get("keywords")),
                clean(row.get("summary")),
                clean(row.get("wikipedia_url")),
                MIN_PUBLICATION_YEAR,
                "|".join(COUNTRY_CODES),
                loaded_at,
            ))

    con.executemany(
        """
        INSERT OR REPLACE INTO openalex_expansion_topics
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        selected,
    )

    for topic in selected:
        topic_id, topic_name = topic[0], topic[1]
        harvest_source_id = f"hs_openalex_topic_run2_{topic_id}"
        con.execute(
            """
            INSERT OR REPLACE INTO harvest_sources
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                harvest_source_id,
                "openalex",
                f"OpenAlex topic run2: {topic_name}",
                "https://api.openalex.org/works",
                "topic_id",
                topic_id,
                "api",
                f"Run-2 OpenAlex topic expansion. Filter: topics.id:{topic_id}; AU/NZ author; from {MIN_PUBLICATION_YEAR}.",
            ],
        )

    return len(selected)


def load_journals(con):
    if not JOURNALS_CSV.exists():
        raise FileNotFoundError(f"Journal CSV not found: {JOURNALS_CSV}")

    loaded_at = now_iso()
    rows = []
    identifiers = []

    with JOURNALS_CSV.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            title = clean(row.get("canonical_title"))
            if not title:
                continue

            print_issn = normalise_issn(row.get("print_ISSN"))
            electronic_issn = normalise_issn(row.get("electronic_ISSN"))
            family_id = clean(row.get("family_id"))

            target_id_basis = family_id or title
            journal_target_id = make_id("oajournal", target_id_basis, print_issn, electronic_issn)

            record = (
                journal_target_id,
                title,
                clean(row.get("alternate_historical_titles")),
                family_id,
                clean(row.get("predecessor_successor_links")),
                print_issn or None,
                electronic_issn or None,
                clean(row.get("country")),
                clean(row.get("publisher")),
                clean(row.get("society_or_association")),
                clean(row.get("active_status")),
                clean(row.get("OA_status")),
                clean(row.get("DOI_usage_quality")),
                clean(row.get("indexing_presence")),
                clean(row.get("likely_metadata_quality")),
                clean(row.get("hosting_platform")),
                clean(row.get("harvest_difficulty")),
                clean(row.get("likely_covered_by_OpenAlex")),
                clean(row.get("likely_underrepresented_in_global")),
                clean(row.get("hosted_via_OJS")),
                clean(row.get("harvestable_from_static_archives")),
                clean(row.get("relevance_classification")),
                clean(row.get("acquisition_priority")),
                clean(row.get("notes")),
                MIN_PUBLICATION_YEAR,
                loaded_at,
            )
            rows.append(record)

            if print_issn:
                identifiers.append((journal_target_id, title, "print_issn", print_issn, loaded_at))
            if electronic_issn and electronic_issn != print_issn:
                identifiers.append((journal_target_id, title, "electronic_issn", electronic_issn, loaded_at))

            con.execute(
                """
                INSERT OR REPLACE INTO harvest_sources
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    f"hs_openalex_journal_{journal_target_id}",
                    "openalex",
                    f"OpenAlex journal target: {title}",
                    "https://api.openalex.org/works",
                    "journal_target_id",
                    journal_target_id,
                    "api",
                    f"OpenAlex journal/source expansion target from AU/NZ journals CSV. Harvest all OpenAlex works from {MIN_PUBLICATION_YEAR}.",
                ],
            )

    con.executemany(
        """
        INSERT OR REPLACE INTO openalex_journal_targets
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )

    if identifiers:
        con.executemany(
            """
            INSERT OR REPLACE INTO openalex_journal_target_identifiers
            VALUES (?, ?, ?, ?, ?)
            """,
            identifiers,
        )

    return len(rows), len(identifiers)


def main():
    con = duckdb.connect(str(DUCKDB_FILE))

    print("Creating OpenAlex expansion support tables...")
    create_support_tables(con)

    print("Loading run-2 topics...")
    topic_count = load_topics(con)

    print("Loading AU/NZ journal targets...")
    journal_count, identifier_count = load_journals(con)

    print()
    print("OpenAlex expansion inputs prepared.")
    print(f"Topics selected for run 2: {topic_count:,}")
    print(f"Journal targets loaded: {journal_count:,}")
    print(f"Journal identifiers loaded: {identifier_count:,}")
    print()
    print("Next script: 17_harvest_openalex_expansion.py")

    con.close()


if __name__ == "__main__":
    main()
