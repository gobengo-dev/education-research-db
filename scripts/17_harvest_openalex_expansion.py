from pathlib import Path
from datetime import datetime
import hashlib
import json
import os
import time
import urllib.parse

import duckdb
import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_FILE = PROJECT_ROOT / "data" / "research_database.duckdb"
EXPORT_DIR = PROJECT_ROOT / "data" / "exports" / "openalex_expansion"

OPENALEX_EMAIL = os.environ.get("OPENALEX_EMAIL", "").strip()
OPENALEX_API_KEY = os.environ.get("OPENALEX_API_KEY", "").strip()

WORKS_URL = "https://api.openalex.org/works"
SOURCES_URL = "https://api.openalex.org/sources"

COUNTRY_CODES = ["AU", "NZ"]
MIN_PUBLICATION_YEAR = 2016

REQUEST_SLEEP_SECONDS = 0.15
MAX_RETRIES = 7
PER_PAGE = 100

# For testing, set these low. For full harvest, set both to None.
MAX_TOPICS = None
MAX_JOURNALS = None
MAX_PAGES_PER_SOURCE = None

HARVEST_TOPICS = True
HARVEST_JOURNALS = True

# For safety, existing OpenAlex works are not overwritten in raw_works/parsed tables by default.
# Rediscovery still gets recorded in provenance/discovery tables.
UPDATE_EXISTING_WORKS = False

# For journal harvests, this is the conservative field: works whose primary source is the journal.
# If later you want broader matching, consider locations.source.id, but expect more noise.
JOURNAL_SOURCE_FILTER_FIELD = "primary_location.source.id"

WORK_SELECT_FIELDS = [
    "id",
    "doi",
    "display_name",
    "title",
    "publication_year",
    "publication_date",
    "type",
    "type_crossref",
    "cited_by_count",
    "citation_normalized_percentile",
    "fwci",
    "authorships",
    "primary_topic",
    "topics",
    "keywords",
    "concepts",
    "primary_location",
    "locations",
    "best_oa_location",
    "open_access",
    "language",
    "abstract_inverted_index",
    "referenced_works",
    "related_works",
    "counts_by_year",
    "funders",
    "awards",
    "created_date",
    "updated_date",
]


def q(sql: str) -> str:
    return sql.strip()


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def clean(value):
    if value is None:
        return ""
    return str(value).strip()


def stable_hash(value: str) -> str:
    return hashlib.sha1(clean(value).encode("utf-8")).hexdigest()


def short_hash(value: str) -> str:
    return stable_hash(value)[:16]


def content_hash(value: str) -> str:
    return hashlib.sha1((value or "").encode("utf-8")).hexdigest()


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


def normalise_openalex_id(value: str) -> str:
    value = clean(value)
    if not value:
        return ""
    return value.rstrip("/")


def make_harvest_run_id() -> str:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"hr_openalex_expansion_{stamp}"


def add_common_params(params):
    params = dict(params)
    if OPENALEX_EMAIL:
        params["mailto"] = OPENALEX_EMAIL
    if OPENALEX_API_KEY:
        params["api_key"] = OPENALEX_API_KEY
    return params


def get_json_with_retries(url, params):
    params = add_common_params(params)

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, params=params, timeout=120)

            if response.status_code == 400:
                print()
                print("400 Bad Request")
                print(response.url)
                print(response.text[:1200])
                raise RuntimeError("400 Bad Request from OpenAlex")

            if response.status_code in (429, 500, 502, 503, 504):
                wait = min(300, 10 * (2 ** attempt))
                print(f"HTTP {response.status_code}; waiting {wait}s...")
                time.sleep(wait)
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            wait = min(300, 10 * (2 ** attempt))
            print(f"Request failed: {e}")
            print(f"Waiting {wait}s...")
            time.sleep(wait)

    raise RuntimeError(f"Failed after {MAX_RETRIES} retries: {url}")


def table_exists(con, table_name: str) -> bool:
    return con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchone()[0] > 0


def count_rows(con, table_name: str) -> int:
    if not table_exists(con, table_name):
        return 0
    return con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]


def ensure_required_tables(con):
    required = [
        "harvest_runs",
        "harvest_sources",
        "raw_source_records",
        "source_work_claims",
        "work_identifier_claims",
        "openalex_expansion_topics",
        "openalex_journal_targets",
        "openalex_journal_target_identifiers",
        "openalex_raw_harvest_records",
        "source_record_discovery_events",
        "harvest_seen_work_openalex_sources",
    ]

    missing = [t for t in required if not table_exists(con, t)]
    if missing:
        raise RuntimeError(
            "Missing required tables: "
            + ", ".join(missing)
            + "\nRun scripts/15_create_provenance_architecture.py and scripts/16_prepare_openalex_expansion_inputs.py first."
        )


def seed_harvest_run(con, harvest_run_id: str):
    started_at = now_iso()
    params = {
        "country_codes": COUNTRY_CODES,
        "min_publication_year": MIN_PUBLICATION_YEAR,
        "harvest_topics": HARVEST_TOPICS,
        "harvest_journals": HARVEST_JOURNALS,
        "max_topics": MAX_TOPICS,
        "max_journals": MAX_JOURNALS,
        "max_pages_per_source": MAX_PAGES_PER_SOURCE,
        "update_existing_works": UPDATE_EXISTING_WORKS,
        "journal_source_filter_field": JOURNAL_SOURCE_FILTER_FIELD,
        "per_page": PER_PAGE,
        "request_sleep_seconds": REQUEST_SLEEP_SECONDS,
    }

    con.execute(
        """
        INSERT OR REPLACE INTO harvest_runs
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            harvest_run_id,
            "openalex",
            "topic_and_journal_expansion",
            "17_harvest_openalex_expansion.py",
            "v1",
            started_at,
            None,
            "running",
            json.dumps(params, ensure_ascii=False),
            "Run-2 OpenAlex expansion: selected topics with AU/NZ author filter and AU/NZ journal-source harvests from 2016 onward.",
        ],
    )


def complete_harvest_run(con, harvest_run_id: str, status: str, notes: str):
    con.execute(
        """
        UPDATE harvest_runs
        SET completed_at = ?,
            status = ?,
            notes = ?
        WHERE harvest_run_id = ?
        """,
        [now_iso(), status, notes, harvest_run_id],
    )


def create_log_file(harvest_run_id: str):
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = EXPORT_DIR / f"{harvest_run_id}_log.csv"
    if not path.exists():
        path.write_text(
            "timestamp,harvest_run_id,harvest_source_id,source_kind,source_label,page,works_returned,status,message\n",
            encoding="utf-8",
        )
    return path


def append_log(log_path: Path, row):
    def esc(v):
        text = "" if v is None else str(v)
        text = text.replace('"', '""')
        return f'"{text}"'

    with log_path.open("a", encoding="utf-8") as f:
        f.write(",".join(esc(x) for x in row) + "\n")


def get_topics(con):
    rows = con.execute(q("""
        SELECT
            topic_id,
            topic_display_name,
            education_relevance_tier,
            include_decision_run2
        FROM openalex_expansion_topics
        ORDER BY topic_id
    """)).fetchall()

    if MAX_TOPICS is not None:
        rows = rows[:MAX_TOPICS]

    return [
        {
            "topic_id": row[0],
            "topic_display_name": row[1],
            "education_relevance_tier": row[2],
            "include_decision_run2": row[3],
            "harvest_source_id": f"hs_openalex_topic_run2_{row[0]}",
        }
        for row in rows
    ]


def get_journal_targets(con):
    rows = con.execute(q("""
        SELECT
            journal_target_id,
            canonical_title,
            print_issn,
            electronic_issn,
            relevance_classification,
            acquisition_priority,
            likely_covered_by_openalex,
            likely_underrepresented_in_global
        FROM openalex_journal_targets
        ORDER BY
            CASE acquisition_priority
                WHEN 'highest' THEN 1
                WHEN 'high' THEN 2
                WHEN 'medium' THEN 3
                WHEN 'low' THEN 4
                ELSE 5
            END,
            canonical_title
    """)).fetchall()

    if MAX_JOURNALS is not None:
        rows = rows[:MAX_JOURNALS]

    return [
        {
            "journal_target_id": row[0],
            "canonical_title": row[1],
            "print_issn": row[2],
            "electronic_issn": row[3],
            "relevance_classification": row[4],
            "acquisition_priority": row[5],
            "likely_covered_by_openalex": row[6],
            "likely_underrepresented_in_global": row[7],
            "harvest_source_id": f"hs_openalex_journal_{row[0]}",
        }
        for row in rows
    ]


def resolve_openalex_sources_for_journal(con, journal):
    identifiers = con.execute(
        """
        SELECT DISTINCT identifier_value
        FROM openalex_journal_target_identifiers
        WHERE journal_target_id = ?
          AND identifier_value IS NOT NULL
          AND identifier_value != ''
        ORDER BY identifier_value
        """,
        [journal["journal_target_id"]],
    ).fetchall()

    already = con.execute(
        """
        SELECT
            openalex_source_id,
            source_display_name
        FROM openalex_journal_source_resolutions
        WHERE journal_target_id = ?
        ORDER BY openalex_source_id
        """,
        [journal["journal_target_id"]],
    ).fetchall()

    if already:
        return [{"openalex_source_id": r[0], "source_display_name": r[1]} for r in already]

    seen_source_ids = set()
    resolved = []

    for (issn,) in identifiers:
        if not issn:
            continue

        # OpenAlex source lookup by ISSN. If this fails for a specific API/schema change,
        # the error is intentionally visible rather than silently guessing.
        params = {
            "filter": f"issn:{issn}",
            "per_page": 25,
        }

        try:
            data = get_json_with_retries(SOURCES_URL, params)
        except Exception as e:
            print(f"  source resolution failed for ISSN {issn}: {e}")
            continue

        for source in data.get("results") or []:
            source_id = normalise_openalex_id(source.get("id"))
            if not source_id or source_id in seen_source_ids:
                continue

            seen_source_ids.add(source_id)
            resolved.append({
                "openalex_source_id": source_id,
                "source_display_name": source.get("display_name"),
            })

            ids = source.get("ids") or {}
            issn_list = source.get("issn") or []

            con.execute(
                """
                INSERT OR REPLACE INTO openalex_journal_source_resolutions
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    journal["journal_target_id"],
                    journal["canonical_title"],
                    issn,
                    source_id,
                    source.get("display_name"),
                    source.get("type"),
                    source.get("issn_l"),
                    json.dumps(issn_list, ensure_ascii=False),
                    source.get("host_organization_name"),
                    source.get("works_count"),
                    source.get("cited_by_count"),
                    "openalex_sources_filter_issn",
                    now_iso(),
                    json.dumps(source, ensure_ascii=False),
                ],
            )

        time.sleep(REQUEST_SLEEP_SECONDS)

    # Fallback: title search if no ISSN results.
    if not resolved:
        params = {
            "search": journal["canonical_title"],
            "per_page": 10,
        }
        try:
            data = get_json_with_retries(SOURCES_URL, params)
        except Exception as e:
            print(f"  title fallback resolution failed: {e}")
            return []

        for source in data.get("results") or []:
            source_id = normalise_openalex_id(source.get("id"))
            if not source_id or source_id in seen_source_ids:
                continue

            title_a = clean(source.get("display_name")).lower()
            title_b = clean(journal["canonical_title"]).lower()

            if title_a != title_b:
                continue

            seen_source_ids.add(source_id)
            resolved.append({
                "openalex_source_id": source_id,
                "source_display_name": source.get("display_name"),
            })

            con.execute(
                """
                INSERT OR REPLACE INTO openalex_journal_source_resolutions
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    journal["journal_target_id"],
                    journal["canonical_title"],
                    "title_search",
                    source_id,
                    source.get("display_name"),
                    source.get("type"),
                    source.get("issn_l"),
                    json.dumps(source.get("issn") or [], ensure_ascii=False),
                    source.get("host_organization_name"),
                    source.get("works_count"),
                    source.get("cited_by_count"),
                    "openalex_sources_search_exact_title",
                    now_iso(),
                    json.dumps(source, ensure_ascii=False),
                ],
            )

        time.sleep(REQUEST_SLEEP_SECONDS)

    return resolved


def work_already_present(con, work_id: str) -> bool:
    return con.execute(
        "SELECT COUNT(*) FROM raw_works WHERE work_id = ?",
        [work_id],
    ).fetchone()[0] > 0


def title_clean(title: str) -> str:
    return clean(title)


def insert_or_update_work_tables(con, work, source_context, already_present: bool):
    work_id = work.get("id")
    if not work_id:
        return

    harvested_at = now_iso()
    raw_json = json.dumps(work, ensure_ascii=False)

    # Existing raw_works is intentionally one row per OpenAlex work.
    # Every retrieval is preserved separately in openalex_raw_harvest_records.
    if (not already_present) or UPDATE_EXISTING_WORKS:
        con.execute(
            """
            INSERT OR REPLACE INTO raw_works (
                work_id,
                raw_json,
                harvested_at,
                first_source_topic_id
            )
            VALUES (?, ?, ?, ?)
            """,
            [
                work_id,
                raw_json,
                harvested_at,
                source_context.get("source_identifier"),
            ],
        )

        insert_flattened_work(con, work)


def insert_flattened_work(con, work):
    work_id = work.get("id")
    primary_topic = work.get("primary_topic") or {}
    citation_pct = work.get("citation_normalized_percentile") or {}
    title = work.get("display_name") or work.get("title")

    con.execute(
        """
        INSERT OR REPLACE INTO works_flat (
            work_id,
            doi,
            title,
            publication_year,
            publication_date,
            work_type,
            type_crossref,
            cited_by_count,
            fwci,
            citation_percentile,
            is_in_top_1_percent,
            is_in_top_10_percent,
            primary_topic_id,
            primary_topic_name,
            language,
            updated_date,
            created_date,
            title_clean
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            work_id,
            work.get("doi"),
            title,
            work.get("publication_year"),
            work.get("publication_date"),
            work.get("type"),
            work.get("type_crossref"),
            work.get("cited_by_count"),
            work.get("fwci"),
            citation_pct.get("value") if isinstance(citation_pct, dict) else None,
            citation_pct.get("is_in_top_1_percent") if isinstance(citation_pct, dict) else None,
            citation_pct.get("is_in_top_10_percent") if isinstance(citation_pct, dict) else None,
            primary_topic.get("id"),
            primary_topic.get("display_name"),
            work.get("language"),
            work.get("updated_date"),
            work.get("created_date"),
            title_clean(title),
        ],
    )

    # Authorships
    con.execute("DELETE FROM authorships WHERE work_id = ?", [work_id])

    for idx, authorship in enumerate(work.get("authorships") or [], start=1):
        author = authorship.get("author") or {}
        institutions = authorship.get("institutions") or []

        countries = sorted({
            inst.get("country_code")
            for inst in institutions
            if inst.get("country_code")
        })

        is_au_nz = any(c in COUNTRY_CODES for c in countries)

        con.execute(
            """
            INSERT INTO authorships (
                work_id,
                author_order,
                author_position,
                author_id,
                author_name,
                author_orcid,
                is_corresponding,
                institutions_json,
                countries,
                is_au_nz_affiliated
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                work_id,
                idx,
                authorship.get("author_position"),
                author.get("id"),
                author.get("display_name"),
                author.get("orcid"),
                authorship.get("is_corresponding"),
                json.dumps(institutions, ensure_ascii=False),
                "; ".join(countries),
                is_au_nz,
            ],
        )

    # Topics
    con.execute("DELETE FROM work_topics WHERE work_id = ?", [work_id])

    for topic in work.get("topics") or []:
        subfield = topic.get("subfield") or {}
        field = topic.get("field") or {}
        domain = topic.get("domain") or {}

        con.execute(
            """
            INSERT INTO work_topics (
                work_id,
                topic_id,
                topic_name,
                subfield_id,
                subfield_name,
                field_id,
                field_name,
                domain_id,
                domain_name,
                topic_score
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                work_id,
                topic.get("id"),
                topic.get("display_name"),
                subfield.get("id"),
                subfield.get("display_name"),
                field.get("id"),
                field.get("display_name"),
                domain.get("id"),
                domain.get("display_name"),
                topic.get("score"),
            ],
        )

    # Keywords
    con.execute("DELETE FROM work_keywords WHERE work_id = ?", [work_id])

    for kw in work.get("keywords") or []:
        con.execute(
            """
            INSERT INTO work_keywords (
                work_id,
                keyword_id,
                keyword,
                score
            )
            VALUES (?, ?, ?, ?)
            """,
            [
                work_id,
                kw.get("id"),
                kw.get("display_name"),
                kw.get("score"),
            ],
        )

    # Source / journal
    primary_location = work.get("primary_location") or {}
    source = primary_location.get("source") or {}
    open_access = work.get("open_access") or {}

    con.execute(
        """
        INSERT OR REPLACE INTO work_sources (
            work_id,
            source_id,
            source_name,
            source_type,
            publisher,
            issn_l,
            issn,
            is_oa,
            oa_status,
            landing_page_url,
            pdf_url,
            source_name_clean,
            publisher_clean
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            work_id,
            source.get("id"),
            source.get("display_name"),
            source.get("type"),
            source.get("publisher"),
            source.get("issn_l"),
            "; ".join(source.get("issn") or []),
            open_access.get("is_oa") if isinstance(open_access, dict) else None,
            open_access.get("oa_status") if isinstance(open_access, dict) else None,
            primary_location.get("landing_page_url"),
            primary_location.get("pdf_url"),
            title_clean(source.get("display_name")),
            title_clean(source.get("publisher")),
        ],
    )

    # Funders
    con.execute("DELETE FROM work_funders WHERE work_id = ?", [work_id])

    for funder in work.get("funders") or []:
        con.execute(
            """
            INSERT INTO work_funders (
                work_id,
                funder_id,
                funder_name
            )
            VALUES (?, ?, ?)
            """,
            [
                work_id,
                funder.get("id"),
                funder.get("display_name"),
            ],
        )

    # Awards
    con.execute("DELETE FROM work_awards WHERE work_id = ?", [work_id])

    for award in work.get("awards") or []:
        funder = award.get("funder") or {}
        con.execute(
            """
            INSERT INTO work_awards (
                work_id,
                award_id,
                funder_id,
                funder_name
            )
            VALUES (?, ?, ?, ?)
            """,
            [
                work_id,
                award.get("id"),
                funder.get("id"),
                funder.get("display_name"),
            ],
        )


def insert_provenance_for_work(con, harvest_run_id, source_context, work, already_present):
    work_id = work.get("id")
    if not work_id:
        return

    retrieved_at = now_iso()
    raw_json = json.dumps(work, ensure_ascii=False)
    raw_hash = content_hash(raw_json)

    harvest_source_id = source_context["harvest_source_id"]
    source_identifier = source_context.get("source_identifier", "")
    source_display_name = source_context.get("source_display_name", "")
    source_type = source_context.get("source_type", "")
    discovery_context = source_context.get("discovery_context", "")

    raw_harvest_record_id = f"oahr_{short_hash(harvest_run_id + '|' + harvest_source_id + '|' + work_id)}"
    raw_record_id = f"raw_openalex_retrieval_{short_hash(harvest_run_id + '|' + harvest_source_id + '|' + work_id)}"
    discovery_event_id = f"disc_{short_hash(harvest_run_id + '|' + harvest_source_id + '|' + work_id)}"

    con.execute(
        """
        INSERT OR REPLACE INTO openalex_raw_harvest_records
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            raw_harvest_record_id,
            harvest_run_id,
            harvest_source_id,
            work_id,
            retrieved_at,
            200,
            already_present,
            raw_json,
            raw_hash,
        ],
    )

    con.execute(
        """
        INSERT OR REPLACE INTO raw_source_records
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            raw_record_id,
            harvest_run_id,
            harvest_source_id,
            "openalex",
            work_id,
            "work",
            "openalex_raw_harvest_records",
            raw_harvest_record_id,
            retrieved_at,
            200,
            raw_hash,
            None,
        ],
    )

    con.execute(
        """
        INSERT OR REPLACE INTO source_record_discovery_events
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            discovery_event_id,
            harvest_run_id,
            harvest_source_id,
            "openalex",
            work_id,
            "work",
            discovery_context,
            source_identifier,
            retrieved_at,
            already_present,
        ],
    )

    con.execute(
        """
        INSERT OR REPLACE INTO harvest_seen_work_openalex_sources
        VALUES (
            ?, ?, ?, ?, ?, ?, 
            COALESCE(
                (SELECT first_seen_at
                 FROM harvest_seen_work_openalex_sources
                 WHERE work_id = ? AND harvest_source_id = ?),
                ?
            ),
            ?,
            COALESCE(
                (SELECT seen_count
                 FROM harvest_seen_work_openalex_sources
                 WHERE work_id = ? AND harvest_source_id = ?),
                0
            ) + 1
        )
        """,
        [
            work_id,
            harvest_source_id,
            "openalex",
            source_type,
            source_identifier,
            source_display_name,
            work_id,
            harvest_source_id,
            retrieved_at,
            retrieved_at,
            work_id,
            harvest_source_id,
        ],
    )

    primary_location = work.get("primary_location") or {}
    source = primary_location.get("source") or {}
    title = work.get("display_name") or work.get("title")

    claim_id = f"claim_openalex_retrieval_{short_hash(harvest_run_id + '|' + harvest_source_id + '|' + work_id)}"
    con.execute(
        """
        INSERT OR REPLACE INTO source_work_claims (
            claim_id,
            source_system,
            harvest_run_id,
            raw_record_id,
            source_work_id,
            canonical_work_id,
            doi,
            title,
            publication_year,
            publication_date,
            work_type,
            container_title,
            source_name,
            publisher,
            claim_confidence,
            match_method,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            claim_id,
            "openalex",
            harvest_run_id,
            raw_record_id,
            work_id,
            work_id,
            work.get("doi"),
            title,
            work.get("publication_year"),
            work.get("publication_date"),
            work.get("type"),
            source.get("display_name"),
            source.get("display_name"),
            source.get("publisher"),
            "native_record",
            discovery_context,
            retrieved_at,
        ],
    )

    id_claim_id = f"idclaim_openalex_retrieval_{short_hash(harvest_run_id + '|' + harvest_source_id + '|' + work_id)}"
    con.execute(
        """
        INSERT OR REPLACE INTO work_identifier_claims
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            id_claim_id,
            work_id,
            "openalex",
            harvest_run_id,
            raw_record_id,
            "openalex_work_id",
            work_id,
            work_id,
            "native_record",
            discovery_context,
            retrieved_at,
        ],
    )

    doi = clean(work.get("doi"))
    if doi:
        doi_norm = doi.lower()
        doi_norm = doi_norm.replace("https://doi.org/", "").replace("http://doi.org/", "")
        doi_norm = doi_norm.replace("https://dx.doi.org/", "").replace("http://dx.doi.org/", "")
        doi_claim_id = f"idclaim_openalex_doi_retrieval_{short_hash(harvest_run_id + '|' + harvest_source_id + '|' + work_id + '|' + doi_norm)}"
        con.execute(
            """
            INSERT OR REPLACE INTO work_identifier_claims
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                doi_claim_id,
                work_id,
                "openalex",
                harvest_run_id,
                raw_record_id,
                "doi",
                doi,
                doi_norm,
                "source_supplied",
                discovery_context,
                retrieved_at,
            ],
        )

    # Existing legacy topic-discovery table, for compatibility with earlier scripts.
    if source_type == "topic":
        con.execute(
            """
            INSERT OR IGNORE INTO harvest_seen_work_topics
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                work_id,
                source_identifier,
                source_display_name,
                source_context.get("education_relevance_tier"),
                retrieved_at,
            ],
        )


def process_work(con, harvest_run_id, source_context, work):
    work_id = work.get("id")
    if not work_id:
        return False, False

    already_present = work_already_present(con, work_id)

    insert_provenance_for_work(con, harvest_run_id, source_context, work, already_present)
    insert_or_update_work_tables(con, work, source_context, already_present)

    return True, not already_present


def harvest_works_for_filter(con, harvest_run_id, log_path, source_context, filter_string):
    cursor = "*"
    page = 0
    total_seen = 0
    total_new = 0

    while True:
        page += 1

        if MAX_PAGES_PER_SOURCE is not None and page > MAX_PAGES_PER_SOURCE:
            print(f"  page cap reached: {MAX_PAGES_PER_SOURCE}")
            break

        params = {
            "filter": filter_string,
            "per_page": PER_PAGE,
            "cursor": cursor,
            "select": ",".join(WORK_SELECT_FIELDS),
        }

        try:
            data = get_json_with_retries(WORKS_URL, params)
        except Exception as e:
            print(f"  ERROR: {e}")
            append_log(log_path, [
                now_iso(),
                harvest_run_id,
                source_context["harvest_source_id"],
                source_context["source_type"],
                source_context["source_display_name"],
                page,
                0,
                "ERROR",
                str(e),
            ])
            break

        works = data.get("results") or []
        print(f"  page {page}: {len(works)} works")

        append_log(log_path, [
            now_iso(),
            harvest_run_id,
            source_context["harvest_source_id"],
            source_context["source_type"],
            source_context["source_display_name"],
            page,
            len(works),
            "OK",
            "",
        ])

        if not works:
            break

        for work in works:
            inserted, is_new = process_work(con, harvest_run_id, source_context, work)
            if inserted:
                total_seen += 1
            if is_new:
                total_new += 1

        next_cursor = data.get("meta", {}).get("next_cursor")
        if not next_cursor:
            break

        cursor = next_cursor
        time.sleep(REQUEST_SLEEP_SECONDS)

    return total_seen, total_new


def harvest_topics(con, harvest_run_id, log_path):
    topics = get_topics(con)
    print(f"Topic targets: {len(topics):,}")

    total_seen = 0
    total_new = 0

    for idx, topic in enumerate(topics, start=1):
        print()
        print(f"[topic {idx}/{len(topics)}] {topic['topic_display_name']} ({topic['topic_id']})")

        filter_string = (
            f"topics.id:{topic['topic_id']},"
            f"authorships.institutions.country_code:{'|'.join(COUNTRY_CODES)},"
            f"from_publication_date:{MIN_PUBLICATION_YEAR}-01-01"
        )

        source_context = {
            "harvest_source_id": topic["harvest_source_id"],
            "source_type": "topic",
            "source_identifier": topic["topic_id"],
            "source_display_name": topic["topic_display_name"],
            "education_relevance_tier": topic["education_relevance_tier"],
            "discovery_context": "openalex_topic_run2_au_nz_author",
        }

        seen, new = harvest_works_for_filter(con, harvest_run_id, log_path, source_context, filter_string)
        total_seen += seen
        total_new += new

    return total_seen, total_new


def harvest_journals(con, harvest_run_id, log_path):
    journals = get_journal_targets(con)
    print(f"Journal targets: {len(journals):,}")

    total_seen = 0
    total_new = 0
    unresolved = 0

    for idx, journal in enumerate(journals, start=1):
        print()
        print(f"[journal {idx}/{len(journals)}] {journal['canonical_title']}")

        sources = resolve_openalex_sources_for_journal(con, journal)

        if not sources:
            unresolved += 1
            print("  no OpenAlex source resolved")
            append_log(log_path, [
                now_iso(),
                harvest_run_id,
                journal["harvest_source_id"],
                "journal",
                journal["canonical_title"],
                0,
                0,
                "NO_SOURCE_RESOLVED",
                "",
            ])
            continue

        print(f"  resolved OpenAlex sources: {len(sources)}")

        for source in sources:
            source_id = source["openalex_source_id"]
            source_label = source.get("source_display_name") or source_id
            print(f"  source: {source_label} ({source_id})")

            filter_string = (
                f"{JOURNAL_SOURCE_FILTER_FIELD}:{source_id},"
                f"from_publication_date:{MIN_PUBLICATION_YEAR}-01-01"
            )

            source_context = {
                "harvest_source_id": journal["harvest_source_id"],
                "source_type": "journal",
                "source_identifier": source_id,
                "source_display_name": journal["canonical_title"],
                "discovery_context": "openalex_journal_registry_source_harvest",
            }

            seen, new = harvest_works_for_filter(con, harvest_run_id, log_path, source_context, filter_string)
            total_seen += seen
            total_new += new

    return total_seen, total_new, unresolved


def print_summary(con, harvest_run_id, topic_seen, topic_new, journal_seen, journal_new, unresolved_journals):
    print()
    print("OpenAlex expansion harvest complete.")
    print(f"Harvest run: {harvest_run_id}")
    print()
    print(f"Topic works seen this run: {topic_seen:,}")
    print(f"Topic new works inserted into raw_works: {topic_new:,}")
    print(f"Journal works seen this run: {journal_seen:,}")
    print(f"Journal new works inserted into raw_works: {journal_new:,}")
    print(f"Unresolved journal targets: {unresolved_journals:,}")
    print()
    print("Current key table counts:")
    for table in [
        "raw_works",
        "works_flat",
        "work_sources",
        "authorships",
        "raw_source_records",
        "source_record_discovery_events",
        "openalex_raw_harvest_records",
        "source_work_claims",
        "work_identifier_claims",
    ]:
        print(f"{table}: {count_rows(con, table):,}")

    print()
    print("Discovery events for this run:")
    for row in con.execute(
        """
        SELECT
            discovery_context,
            already_present,
            COUNT(*) AS rows
        FROM source_record_discovery_events
        WHERE harvest_run_id = ?
        GROUP BY discovery_context, already_present
        ORDER BY discovery_context, already_present
        """,
        [harvest_run_id],
    ).fetchall():
        print(row)


def main():
    harvest_run_id = make_harvest_run_id()
    log_path = create_log_file(harvest_run_id)

    con = duckdb.connect(str(DUCKDB_FILE))
    ensure_required_tables(con)

    print(f"Starting OpenAlex expansion harvest: {harvest_run_id}")
    print(f"DuckDB: {DUCKDB_FILE}")
    print(f"Log: {log_path}")
    print(f"UPDATE_EXISTING_WORKS: {UPDATE_EXISTING_WORKS}")
    print(f"MAX_PAGES_PER_SOURCE: {MAX_PAGES_PER_SOURCE}")
    print()

    seed_harvest_run(con, harvest_run_id)

    topic_seen = topic_new = 0
    journal_seen = journal_new = unresolved_journals = 0

    try:
        if HARVEST_TOPICS:
            topic_seen, topic_new = harvest_topics(con, harvest_run_id, log_path)

        if HARVEST_JOURNALS:
            journal_seen, journal_new, unresolved_journals = harvest_journals(con, harvest_run_id, log_path)

        notes = (
            f"Completed. topic_seen={topic_seen}; topic_new={topic_new}; "
            f"journal_seen={journal_seen}; journal_new={journal_new}; "
            f"unresolved_journals={unresolved_journals}."
        )
        complete_harvest_run(con, harvest_run_id, "complete", notes)

    except KeyboardInterrupt:
        notes = "Interrupted by user. Partial records retained with provenance."
        complete_harvest_run(con, harvest_run_id, "partial_interrupted", notes)
        print()
        print(notes)
        raise

    except Exception as e:
        notes = f"Failed with error: {e}"
        complete_harvest_run(con, harvest_run_id, "failed", notes)
        raise

    finally:
        print_summary(con, harvest_run_id, topic_seen, topic_new, journal_seen, journal_new, unresolved_journals)
        con.close()


if __name__ == "__main__":
    main()
