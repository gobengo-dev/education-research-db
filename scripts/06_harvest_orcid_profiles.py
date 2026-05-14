import os
import json
import time
import re
from datetime import datetime

import duckdb
import requests
from dotenv import load_dotenv

DUCKDB_FILE = "research_database.duckdb"

# Test first. Set to None for full run later.
MAX_ORCIDS_TO_HARVEST = None

REQUEST_SLEEP_SECONDS = 0.25
ORCID_API_BASE = "https://pub.orcid.org/v3.0"
ORCID_TOKEN_URL = "https://orcid.org/oauth/token"

DROP_EXISTING_ORCID_TABLES = False


def now_iso():
    return datetime.now().isoformat(timespec="seconds")


def q(sql):
    return sql.strip()


def clean_orcid(orcid):
    if not orcid:
        return ""

    orcid = str(orcid).strip()
    orcid = orcid.replace("https://orcid.org/", "")
    orcid = orcid.replace("http://orcid.org/", "")
    orcid = orcid.strip("/")

    # Keep only valid-looking ORCID IDs.
    if re.match(r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$", orcid):
        return orcid

    return ""


def get_access_token():
    load_dotenv()

    client_id = os.getenv("ORCID_CLIENT_ID")
    client_secret = os.getenv("ORCID_CLIENT_SECRET")

    if not client_id or not client_secret:
        raise RuntimeError("Missing ORCID_CLIENT_ID or ORCID_CLIENT_SECRET in .env")

    response = requests.post(
        ORCID_TOKEN_URL,
        data={
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
            "scope": "/read-public",
        },
        timeout=60,
    )

    response.raise_for_status()
    return response.json()["access_token"]


def init_tables(con):
    if DROP_EXISTING_ORCID_TABLES:
        for table in [
            "orcid_raw_profiles",
            "orcid_person",
            "orcid_keywords",
            "orcid_external_ids",
            "orcid_websites",
            "orcid_employment",
            "orcid_education",
        ]:
            con.execute(f"DROP TABLE IF EXISTS {table}")

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS orcid_raw_profiles (
            canonical_researcher_id VARCHAR,
            orcid VARCHAR PRIMARY KEY,
            retrieved_at VARCHAR,
            response_status INTEGER,
            raw_json VARCHAR,
            error_message VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS orcid_person (
            canonical_researcher_id VARCHAR,
            orcid VARCHAR PRIMARY KEY,
            given_names VARCHAR,
            family_name VARCHAR,
            credit_name VARCHAR,
            biography VARCHAR,
            last_modified_date BIGINT
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS orcid_keywords (
            orcid VARCHAR,
            keyword VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS orcid_external_ids (
            orcid VARCHAR,
            external_id_type VARCHAR,
            external_id_value VARCHAR,
            external_id_url VARCHAR,
            relationship VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS orcid_websites (
            orcid VARCHAR,
            website_name VARCHAR,
            website_url VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS orcid_employment (
            orcid VARCHAR,
            organization_name VARCHAR,
            city VARCHAR,
            region VARCHAR,
            country VARCHAR,
            role_title VARCHAR,
            department_name VARCHAR,
            start_year VARCHAR,
            end_year VARCHAR
        )
    """))

    con.execute(q("""
        CREATE TABLE IF NOT EXISTS orcid_education (
            orcid VARCHAR,
            organization_name VARCHAR,
            city VARCHAR,
            region VARCHAR,
            country VARCHAR,
            role_title VARCHAR,
            department_name VARCHAR,
            start_year VARCHAR,
            end_year VARCHAR
        )
    """))


def text_value(obj):
    if isinstance(obj, dict):
        return obj.get("value")
    return None


def date_year(date_obj):
    if not isinstance(date_obj, dict):
        return None

    year = date_obj.get("year")
    if isinstance(year, dict):
        return year.get("value")

    return None


def extract_person(con, canonical_researcher_id, orcid, record):
    person = record.get("person") or {}
    name = person.get("name") or {}

    given = text_value(name.get("given-names"))
    family = text_value(name.get("family-name"))
    credit = text_value(name.get("credit-name"))

    bio = person.get("biography") or {}
    biography = text_value(bio.get("content"))

    history = record.get("history") or {}
    last_mod = history.get("last-modified-date") or {}
    last_modified_date = last_mod.get("value") if isinstance(last_mod, dict) else None

    con.execute("DELETE FROM orcid_person WHERE orcid = ?", [orcid])

    con.execute(q("""
        INSERT INTO orcid_person VALUES (?, ?, ?, ?, ?, ?, ?)
    """), [
        canonical_researcher_id,
        orcid,
        given,
        family,
        credit,
        biography,
        last_modified_date,
    ])


def extract_keywords(con, orcid, record):
    con.execute("DELETE FROM orcid_keywords WHERE orcid = ?", [orcid])

    keywords = (
        record.get("person", {})
        .get("keywords", {})
        .get("keyword", [])
    )

    rows = []

    for kw in keywords:
        content = kw.get("content") if isinstance(kw, dict) else None
        if content:
            rows.append((orcid, content))

    if rows:
        con.executemany("INSERT INTO orcid_keywords VALUES (?, ?)", rows)


def extract_external_ids(con, orcid, record):
    con.execute("DELETE FROM orcid_external_ids WHERE orcid = ?", [orcid])

    ids = (
        record.get("person", {})
        .get("external-identifiers", {})
        .get("external-identifier", [])
    )

    rows = []

    for item in ids:
        if not isinstance(item, dict):
            continue

        url_obj = item.get("external-id-url")
        url = url_obj.get("value") if isinstance(url_obj, dict) else None

        rows.append((
            orcid,
            item.get("external-id-type"),
            item.get("external-id-value"),
            url,
            item.get("external-id-relationship"),
        ))

    if rows:
        con.executemany(
            "INSERT INTO orcid_external_ids VALUES (?, ?, ?, ?, ?)",
            rows,
        )


def extract_websites(con, orcid, record):
    con.execute("DELETE FROM orcid_websites WHERE orcid = ?", [orcid])

    websites = (
        record.get("person", {})
        .get("researcher-urls", {})
        .get("researcher-url", [])
    )

    rows = []

    for item in websites:
        if not isinstance(item, dict):
            continue

        url_obj = item.get("url")
        url = url_obj.get("value") if isinstance(url_obj, dict) else None

        rows.append((
            orcid,
            item.get("url-name"),
            url,
        ))

    if rows:
        con.executemany("INSERT INTO orcid_websites VALUES (?, ?, ?)", rows)


def affiliation_rows(orcid, summaries):
    rows = []

    for group in summaries or []:
        if not isinstance(group, dict):
            continue

        for summary_item in group.get("summaries", []):
            if not isinstance(summary_item, dict):
                continue

            item = (
                summary_item.get("employment-summary")
                or summary_item.get("education-summary")
                or {}
            )

            org = item.get("organization") or {}
            address = org.get("address") or {}

            rows.append((
                orcid,
                org.get("name"),
                address.get("city"),
                address.get("region"),
                address.get("country"),
                item.get("role-title"),
                item.get("department-name"),
                date_year(item.get("start-date")),
                date_year(item.get("end-date")),
            ))

    return rows


def extract_employment(con, orcid, record):
    con.execute("DELETE FROM orcid_employment WHERE orcid = ?", [orcid])

    summaries = (
        record.get("activities-summary", {})
        .get("employments", {})
        .get("affiliation-group", [])
    )

    rows = affiliation_rows(orcid, summaries)

    if rows:
        con.executemany(
            "INSERT INTO orcid_employment VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )


def extract_education(con, orcid, record):
    con.execute("DELETE FROM orcid_education WHERE orcid = ?", [orcid])

    summaries = (
        record.get("activities-summary", {})
        .get("educations", {})
        .get("affiliation-group", [])
    )

    rows = affiliation_rows(orcid, summaries)

    if rows:
        con.executemany(
            "INSERT INTO orcid_education VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            rows,
        )


def harvest_orcid(token, orcid):
    url = f"{ORCID_API_BASE}/{orcid}/record"

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

    response = requests.get(url, headers=headers, timeout=90)

    if response.status_code == 200:
        return response.status_code, response.json(), None

    return response.status_code, None, response.text[:2000]


def main():
    con = duckdb.connect(DUCKDB_FILE)
    init_tables(con)

    token = get_access_token()

    already_done = {
        clean_orcid(row[0])
        for row in con.execute("""
            SELECT orcid
            FROM orcid_raw_profiles
            WHERE response_status = 200
        """).fetchall()
    }

    already_done = {x for x in already_done if x}

    query = q("""
        SELECT
            canonical_researcher_id,
            canonical_orcid
        FROM canonical_researchers
        WHERE canonical_orcid IS NOT NULL
          AND canonical_orcid != ''
        ORDER BY canonical_researcher_id
    """)

    candidates = []

    for canonical_researcher_id, raw_orcid in con.execute(query).fetchall():
        orcid = clean_orcid(raw_orcid)
        if orcid and orcid not in already_done:
            candidates.append((canonical_researcher_id, orcid))

    if MAX_ORCIDS_TO_HARVEST is not None:
        candidates = candidates[:MAX_ORCIDS_TO_HARVEST]

    print(f"ORCID records already harvested successfully: {len(already_done):,}")
    print(f"ORCID records to harvest this run: {len(candidates):,}")

    for i, (canonical_researcher_id, orcid) in enumerate(candidates, start=1):
        print(f"[{i}/{len(candidates)}] {orcid} ({canonical_researcher_id})")

        try:
            status, record, error = harvest_orcid(token, orcid)

            con.execute("DELETE FROM orcid_raw_profiles WHERE orcid = ?", [orcid])

            con.execute(q("""
                INSERT INTO orcid_raw_profiles VALUES (?, ?, ?, ?, ?, ?)
            """), [
                canonical_researcher_id,
                orcid,
                now_iso(),
                status,
                json.dumps(record, ensure_ascii=False) if record else None,
                error,
            ])

            if status == 200 and record:
                extract_person(con, canonical_researcher_id, orcid, record)
                extract_keywords(con, orcid, record)
                extract_external_ids(con, orcid, record)
                extract_websites(con, orcid, record)
                extract_employment(con, orcid, record)
                extract_education(con, orcid, record)
            else:
                print(f"  Non-200 response: {status}")

        except Exception as e:
            con.execute("DELETE FROM orcid_raw_profiles WHERE orcid = ?", [orcid])
            con.execute(q("""
                INSERT INTO orcid_raw_profiles VALUES (?, ?, ?, ?, ?, ?)
            """), [
                canonical_researcher_id,
                orcid,
                now_iso(),
                -1,
                None,
                str(e),
            ])
            print(f"  ERROR: {e}")

        time.sleep(REQUEST_SLEEP_SECONDS)

    print()
    print("ORCID harvest run complete.")

    checks = [
        ("orcid_raw_profiles", "SELECT COUNT(*) FROM orcid_raw_profiles"),
        ("successful_orcid_profiles", "SELECT COUNT(*) FROM orcid_raw_profiles WHERE response_status = 200"),
        ("non_200_orcid_profiles", "SELECT COUNT(*) FROM orcid_raw_profiles WHERE response_status != 200"),
        ("orcid_person", "SELECT COUNT(*) FROM orcid_person"),
        ("orcid_keywords", "SELECT COUNT(*) FROM orcid_keywords"),
        ("orcid_external_ids", "SELECT COUNT(*) FROM orcid_external_ids"),
        ("orcid_websites", "SELECT COUNT(*) FROM orcid_websites"),
        ("orcid_employment", "SELECT COUNT(*) FROM orcid_employment"),
        ("orcid_education", "SELECT COUNT(*) FROM orcid_education"),
    ]

    for label, sql in checks:
        count = con.execute(sql).fetchone()[0]
        print(f"{label}: {count:,}")

    print()
    print("Response status breakdown:")
    rows = con.execute(q("""
        SELECT response_status, COUNT(*) AS n
        FROM orcid_raw_profiles
        GROUP BY response_status
        ORDER BY n DESC
    """)).fetchall()

    for row in rows:
        print(row)

    con.close()


if __name__ == "__main__":
    main()