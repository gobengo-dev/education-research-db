import json
import itertools
from collections import defaultdict

import duckdb

DUCKDB_FILE = "research_database.duckdb"

# Set True while testing if you want to rebuild the derived layer repeatedly.
DROP_EXISTING_DERIVED_TABLES = True


def q(s: str) -> str:
    return s.strip()


def main():
    con = duckdb.connect(DUCKDB_FILE)

    if DROP_EXISTING_DERIVED_TABLES:
        for table in [
            "researchers_raw",
            "researcher_works",
            "researcher_topics",
            "researcher_institutions",
            "institutions_raw",
            "coauthor_edges",
        ]:
            con.execute(f"DROP TABLE IF EXISTS {table}")

    print("Building researcher_works...")

    con.execute(q("""
        CREATE TABLE researcher_works AS
        SELECT
            a.author_id,
            a.author_name,
            a.author_orcid,
            a.work_id,
            w.title,
            w.publication_year,
            w.publication_date,
            w.work_type,
            w.cited_by_count,
            w.fwci,
            w.citation_percentile,
            a.author_order,
            a.author_position,
            a.is_corresponding,
            a.countries,
            a.is_au_nz_affiliated
        FROM authorships a
        JOIN works_flat w
            ON a.work_id = w.work_id
        WHERE a.author_id IS NOT NULL
    """))

    print("Building researchers_raw...")

    con.execute(q("""
        CREATE TABLE researchers_raw AS
        SELECT
            author_id,
            any_value(author_name) AS author_name,
            any_value(author_orcid) FILTER (WHERE author_orcid IS NOT NULL AND author_orcid != '') AS orcid,
            COUNT(DISTINCT work_id) AS works_in_corpus,
            SUM(COALESCE(cited_by_count, 0)) AS citations_in_corpus,
            MIN(publication_year) AS first_year_in_corpus,
            MAX(publication_year) AS latest_year_in_corpus,
            COUNT(DISTINCT publication_year) AS active_years_in_corpus,
            SUM(CASE WHEN is_au_nz_affiliated THEN 1 ELSE 0 END) AS au_nz_affiliated_authorships,
            COUNT(*) AS total_authorship_rows,
            CASE
                WHEN SUM(CASE WHEN is_au_nz_affiliated THEN 1 ELSE 0 END) > 0
                THEN TRUE
                ELSE FALSE
            END AS has_au_nz_affiliation_in_corpus
        FROM researcher_works
        GROUP BY author_id
    """))

    print("Building researcher_topics...")

    con.execute(q("""
        CREATE TABLE researcher_topics AS
        SELECT
            a.author_id,
            a.author_name,
            wt.topic_id,
            wt.topic_name,
            wt.subfield_name,
            wt.field_name,
            wt.domain_name,
            ct.education_relevance_tier,
            ct.topic_family,
            ct.school_phase,
            COUNT(DISTINCT a.work_id) AS works_with_topic,
            SUM(COALESCE(w.cited_by_count, 0)) AS citations_with_topic,
            AVG(wt.topic_score) AS avg_topic_score,
            MIN(w.publication_year) AS first_year,
            MAX(w.publication_year) AS latest_year
        FROM authorships a
        JOIN works_flat w
            ON a.work_id = w.work_id
        JOIN work_topics wt
            ON a.work_id = wt.work_id
        LEFT JOIN curated_topics ct
            ON replace(wt.topic_id, 'https://openalex.org/', '') = ct.topic_id
        WHERE a.author_id IS NOT NULL
        GROUP BY
            a.author_id,
            a.author_name,
            wt.topic_id,
            wt.topic_name,
            wt.subfield_name,
            wt.field_name,
            wt.domain_name,
            ct.education_relevance_tier,
            ct.topic_family,
            ct.school_phase
    """))

    print("Building institutions_raw and researcher_institutions...")

    # These are easier to build in Python because institutions_json is nested.
    rows = con.execute(q("""
        SELECT
            work_id,
            author_id,
            author_name,
            institutions_json
        FROM authorships
        WHERE author_id IS NOT NULL
          AND institutions_json IS NOT NULL
          AND institutions_json != ''
    """)).fetchall()

    institution_rows = []
    researcher_institution_rows = []

    seen_institutions = set()
    seen_researcher_institutions = set()

    for work_id, author_id, author_name, institutions_json in rows:
        try:
            institutions = json.loads(institutions_json)
        except Exception:
            continue

        if not isinstance(institutions, list):
            continue

        for inst in institutions:
            if not isinstance(inst, dict):
                continue

            institution_id = inst.get("id")
            institution_name = inst.get("display_name")
            ror = inst.get("ror")
            country_code = inst.get("country_code")
            institution_type = inst.get("type")
            lineage = inst.get("lineage") or []

            if not institution_id:
                continue

            if institution_id not in seen_institutions:
                institution_rows.append((
                    institution_id,
                    institution_name,
                    ror,
                    country_code,
                    institution_type,
                    json.dumps(lineage, ensure_ascii=False),
                ))
                seen_institutions.add(institution_id)

            key = (author_id, institution_id, work_id)
            if key not in seen_researcher_institutions:
                researcher_institution_rows.append((
                    author_id,
                    author_name,
                    institution_id,
                    institution_name,
                    ror,
                    country_code,
                    institution_type,
                    work_id,
                ))
                seen_researcher_institutions.add(key)

    con.execute(q("""
        CREATE TABLE institutions_raw (
            institution_id VARCHAR,
            institution_name VARCHAR,
            ror VARCHAR,
            country_code VARCHAR,
            institution_type VARCHAR,
            lineage_json VARCHAR
        )
    """))

    con.executemany(
        "INSERT INTO institutions_raw VALUES (?, ?, ?, ?, ?, ?)",
        institution_rows,
    )

    con.execute(q("""
        CREATE TABLE researcher_institutions (
            author_id VARCHAR,
            author_name VARCHAR,
            institution_id VARCHAR,
            institution_name VARCHAR,
            ror VARCHAR,
            country_code VARCHAR,
            institution_type VARCHAR,
            work_id VARCHAR
        )
    """))

    con.executemany(
        "INSERT INTO researcher_institutions VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        researcher_institution_rows,
    )

    print("Building coauthor_edges...")

    # Build coauthor edges from authorships.
    # For each work, connect every pair of authors.
    author_rows = con.execute(q("""
        SELECT
            work_id,
            author_id,
            author_name
        FROM authorships
        WHERE author_id IS NOT NULL
        ORDER BY work_id, author_order
    """)).fetchall()

    by_work = defaultdict(list)

    for work_id, author_id, author_name in author_rows:
        by_work[work_id].append((author_id, author_name))

    edge_stats = {}

    for work_id, authors in by_work.items():
        unique_authors = []
        seen = set()

        for author_id, author_name in authors:
            if author_id not in seen:
                unique_authors.append((author_id, author_name))
                seen.add(author_id)

        # Avoid pathological huge consortium papers making millions of edges.
        if len(unique_authors) > 100:
            continue

        for (a_id, a_name), (b_id, b_name) in itertools.combinations(unique_authors, 2):
            if a_id == b_id:
                continue

            if a_id < b_id:
                left_id, left_name, right_id, right_name = a_id, a_name, b_id, b_name
            else:
                left_id, left_name, right_id, right_name = b_id, b_name, a_id, a_name

            key = (left_id, right_id)

            if key not in edge_stats:
                edge_stats[key] = {
                    "author_id_a": left_id,
                    "author_name_a": left_name,
                    "author_id_b": right_id,
                    "author_name_b": right_name,
                    "shared_work_count": 0,
                    "shared_work_ids": [],
                }

            edge_stats[key]["shared_work_count"] += 1
            if len(edge_stats[key]["shared_work_ids"]) < 50:
                edge_stats[key]["shared_work_ids"].append(work_id)

    coauthor_rows = [
        (
            e["author_id_a"],
            e["author_name_a"],
            e["author_id_b"],
            e["author_name_b"],
            e["shared_work_count"],
            "; ".join(e["shared_work_ids"]),
        )
        for e in edge_stats.values()
    ]

    con.execute(q("""
        CREATE TABLE coauthor_edges (
            author_id_a VARCHAR,
            author_name_a VARCHAR,
            author_id_b VARCHAR,
            author_name_b VARCHAR,
            shared_work_count INTEGER,
            sample_shared_work_ids VARCHAR
        )
    """))

    con.executemany(
        "INSERT INTO coauthor_edges VALUES (?, ?, ?, ?, ?, ?)",
        coauthor_rows,
    )

    print("Creating indexes...")

    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_researchers_raw_author_id ON researchers_raw(author_id)",
        "CREATE INDEX IF NOT EXISTS idx_researcher_works_author_id ON researcher_works(author_id)",
        "CREATE INDEX IF NOT EXISTS idx_researcher_works_work_id ON researcher_works(work_id)",
        "CREATE INDEX IF NOT EXISTS idx_researcher_topics_author_id ON researcher_topics(author_id)",
        "CREATE INDEX IF NOT EXISTS idx_researcher_topics_topic_id ON researcher_topics(topic_id)",
        "CREATE INDEX IF NOT EXISTS idx_researcher_institutions_author_id ON researcher_institutions(author_id)",
        "CREATE INDEX IF NOT EXISTS idx_researcher_institutions_institution_id ON researcher_institutions(institution_id)",
        "CREATE INDEX IF NOT EXISTS idx_coauthor_edges_a ON coauthor_edges(author_id_a)",
        "CREATE INDEX IF NOT EXISTS idx_coauthor_edges_b ON coauthor_edges(author_id_b)",
    ]:
        con.execute(sql)

    print()
    print("Derived layer complete.")
    print()

    checks = [
        ("researchers_raw", "SELECT COUNT(*) FROM researchers_raw"),
        ("researcher_works", "SELECT COUNT(*) FROM researcher_works"),
        ("researcher_topics", "SELECT COUNT(*) FROM researcher_topics"),
        ("institutions_raw", "SELECT COUNT(*) FROM institutions_raw"),
        ("researcher_institutions", "SELECT COUNT(*) FROM researcher_institutions"),
        ("coauthor_edges", "SELECT COUNT(*) FROM coauthor_edges"),
    ]

    for name, sql in checks:
        count = con.execute(sql).fetchone()[0]
        print(f"{name}: {count:,}")

    con.close()


if __name__ == "__main__":
    main()