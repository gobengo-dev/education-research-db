import re
import unicodedata
from collections import defaultdict

import duckdb

DUCKDB_FILE = "research_database.duckdb"
DROP_EXISTING_TABLES = True


def normalise_name(name):
    if not name:
        return ""

    name = unicodedata.normalize("NFC", name)
    name = name.lower()
    name = name.replace("‐", "-").replace("–", "-").replace("—", "-")
    name = name.replace("’", "'").replace("`", "'")
    name = re.sub(r"[^\w\s\-\']", " ", name)
    name = re.sub(r"\s+", " ", name).strip()

    return name


def surname_key(name):
    parts = normalise_name(name).split()
    if not parts:
        return ""
    return parts[-1]


def initials_key(name):
    parts = normalise_name(name).split()
    if not parts:
        return ""

    surname = parts[-1]
    given_parts = parts[:-1]

    initials = "".join(p[0] for p in given_parts if p)
    return f"{surname}|{initials}"


def simple_name_key(name):
    return normalise_name(name)


def q(sql):
    return sql.strip()


def main():
    con = duckdb.connect(DUCKDB_FILE)

    if DROP_EXISTING_TABLES:
        for table in [
            "identity_candidate_authors",
            "identity_candidate_pairs",
            "identity_candidate_groups",
        ]:
            con.execute(f"DROP TABLE IF EXISTS {table}")

    print("Loading candidate researchers...")

    rows = con.execute(q("""
        SELECT
            author_id,
            author_name,
            orcid,
            works_in_corpus,
            citations_in_corpus,
            main_au_nz_institution_ror,
            main_au_nz_institution_name,
            au_nz_institution_variants,
            distinct_core_topics,
            distinct_adjacent_topics,
            top_topics,
            coauthor_count
        FROM candidate_researchers
        WHERE author_name IS NOT NULL
          AND author_name != ''
    """)).fetchall()

    author_data = {}

    for row in rows:
        (
            author_id,
            author_name,
            orcid,
            works,
            citations,
            main_ror,
            main_inst,
            inst_variants,
            core_topics,
            adjacent_topics,
            top_topics,
            coauthor_count,
        ) = row

        author_data[author_id] = {
            "author_id": author_id,
            "author_name": author_name,
            "orcid": orcid or "",
            "works": works or 0,
            "citations": citations or 0,
            "main_ror": main_ror or "",
            "main_inst": main_inst or "",
            "inst_variants": inst_variants or "",
            "core_topics": core_topics or 0,
            "adjacent_topics": adjacent_topics or 0,
            "top_topics": top_topics or "",
            "coauthor_count": coauthor_count or 0,
            "simple_name_key": simple_name_key(author_name),
            "initials_key": initials_key(author_name),
            "surname_key": surname_key(author_name),
        }

    print(f"Loaded {len(author_data):,} candidate researchers.")

    print("Building duplicate blocking groups...")

    by_exact_name = defaultdict(list)
    by_initials = defaultdict(list)

    for author_id, data in author_data.items():
        if data["simple_name_key"]:
            by_exact_name[data["simple_name_key"]].append(author_id)

        if data["initials_key"]:
            by_initials[data["initials_key"]].append(author_id)

    candidate_pairs = {}

    def add_pair(a, b, basis, base_score):
        if a == b:
            return

        if a > b:
            a, b = b, a

        key = (a, b)

        if key not in candidate_pairs:
            candidate_pairs[key] = {
                "author_id_a": a,
                "author_id_b": b,
                "basis": set(),
                "score": 0,
            }

        candidate_pairs[key]["basis"].add(basis)
        candidate_pairs[key]["score"] += base_score

    # Exact same normalised name: possible duplicate, but not automatically safe.
    for key, ids in by_exact_name.items():
        if 1 < len(ids) <= 50:
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    add_pair(ids[i], ids[j], "same_normalised_name", 30)

    # Same surname + initials: weaker, only if group is not enormous.
    for key, ids in by_initials.items():
        if 1 < len(ids) <= 20:
            for i in range(len(ids)):
                for j in range(i + 1, len(ids)):
                    add_pair(ids[i], ids[j], "same_surname_initials", 10)

    print(f"Initial candidate pairs: {len(candidate_pairs):,}")

    print("Adding evidence: ORCID, institutions, topics, coauthors...")

    # Shared coauthor evidence
    coauthor_rows = con.execute(q("""
        SELECT
            author_id_a,
            author_id_b,
            shared_work_count
        FROM coauthor_edges
        WHERE shared_work_count >= 1
    """)).fetchall()

    coauthor_lookup = {}
    for a, b, shared in coauthor_rows:
        if a > b:
            a, b = b, a
        coauthor_lookup[(a, b)] = shared

    enriched_pairs = []

    for (a, b), pair in candidate_pairs.items():
        da = author_data[a]
        db = author_data[b]

        evidence = set(pair["basis"])
        score = pair["score"]

        same_orcid = da["orcid"] and db["orcid"] and da["orcid"] == db["orcid"]
        different_orcid = da["orcid"] and db["orcid"] and da["orcid"] != db["orcid"]

        if same_orcid:
            evidence.add("same_orcid")
            score += 100

        if different_orcid:
            evidence.add("different_orcid")
            score -= 100

        if da["main_ror"] and db["main_ror"] and da["main_ror"] == db["main_ror"]:
            evidence.add("same_main_ror")
            score += 30

        elif da["main_inst"] and db["main_inst"] and normalise_name(da["main_inst"]) == normalise_name(db["main_inst"]):
            evidence.add("same_main_institution_name")
            score += 20

        if da["top_topics"] and db["top_topics"]:
            # simple overlap by topic string chunks
            ta = set(x.strip().split(" (")[0] for x in da["top_topics"].split(";") if x.strip())
            tb = set(x.strip().split(" (")[0] for x in db["top_topics"].split(";") if x.strip())
            overlap = ta & tb

            if len(overlap) >= 3:
                evidence.add("topic_overlap_3plus")
                score += 20
            elif len(overlap) >= 1:
                evidence.add("topic_overlap")
                score += 8

        shared_works = coauthor_lookup.get((a, b), 0)

        if shared_works >= 3:
            evidence.add("shared_works_3plus")
            score += 60
        elif shared_works >= 1:
            evidence.add("shared_work")
            score += 30

        if da["works"] <= 1 or db["works"] <= 1:
            evidence.add("one_profile_tiny")
            score += 10

        if different_orcid:
            confidence = "reject_or_review"
        elif same_orcid:
            confidence = "very_high"
        elif score >= 90:
            confidence = "high"
        elif score >= 60:
            confidence = "medium"
        elif score >= 35:
            confidence = "low"
        else:
            confidence = "very_low"

        if confidence in {"very_high", "high", "medium", "low", "reject_or_review"}:
            enriched_pairs.append((
                a,
                da["author_name"],
                da["orcid"],
                da["works"],
                da["citations"],
                da["main_inst"],
                b,
                db["author_name"],
                db["orcid"],
                db["works"],
                db["citations"],
                db["main_inst"],
                score,
                confidence,
                "; ".join(sorted(evidence)),
                shared_works,
            ))

    print(f"Retained candidate pairs: {len(enriched_pairs):,}")

    con.execute(q("""
        CREATE TABLE identity_candidate_pairs (
            author_id_a VARCHAR,
            author_name_a VARCHAR,
            orcid_a VARCHAR,
            works_a INTEGER,
            citations_a HUGEINT,
            main_institution_a VARCHAR,

            author_id_b VARCHAR,
            author_name_b VARCHAR,
            orcid_b VARCHAR,
            works_b INTEGER,
            citations_b HUGEINT,
            main_institution_b VARCHAR,

            evidence_score INTEGER,
            confidence VARCHAR,
            evidence_basis VARCHAR,
            shared_work_count INTEGER
        )
    """))

    con.executemany(
        "INSERT INTO identity_candidate_pairs VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        enriched_pairs,
    )

    print("Building connected candidate groups...")

    # Build groups from medium-or-higher evidence only.
    graph = defaultdict(set)

    for row in enriched_pairs:
        a = row[0]
        b = row[6]
        confidence = row[13]

        if confidence in {"very_high", "high", "medium"}:
            graph[a].add(b)
            graph[b].add(a)

    visited = set()
    groups = []

    group_num = 0

    for node in graph:
        if node in visited:
            continue

        group_num += 1
        stack = [node]
        members = []

        while stack:
            current = stack.pop()
            if current in visited:
                continue

            visited.add(current)
            members.append(current)

            for neighbour in graph[current]:
                if neighbour not in visited:
                    stack.append(neighbour)

        if len(members) > 1:
            group_id = f"ICG{group_num:07d}"
            for member in members:
                data = author_data[member]
                groups.append((
                    group_id,
                    member,
                    data["author_name"],
                    data["orcid"],
                    data["works"],
                    data["citations"],
                    data["main_inst"],
                    len(members),
                ))

    con.execute(q("""
        CREATE TABLE identity_candidate_groups (
            candidate_group_id VARCHAR,
            author_id VARCHAR,
            author_name VARCHAR,
            orcid VARCHAR,
            works_in_corpus INTEGER,
            citations_in_corpus HUGEINT,
            main_institution VARCHAR,
            group_size INTEGER
        )
    """))

    con.executemany(
        "INSERT INTO identity_candidate_groups VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
        groups,
    )

    print("Building identity_candidate_authors...")

    con.execute(q("""
        CREATE TABLE identity_candidate_authors AS
        SELECT
            cr.*,
            icg.candidate_group_id,
            icg.group_size,
            CASE
                WHEN icg.candidate_group_id IS NOT NULL THEN TRUE
                ELSE FALSE
            END AS possible_identity_duplicate
        FROM candidate_researchers cr
        LEFT JOIN identity_candidate_groups icg
            ON cr.author_id = icg.author_id
    """))

    print("Creating indexes...")

    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_identity_pairs_a ON identity_candidate_pairs(author_id_a)",
        "CREATE INDEX IF NOT EXISTS idx_identity_pairs_b ON identity_candidate_pairs(author_id_b)",
        "CREATE INDEX IF NOT EXISTS idx_identity_pairs_conf ON identity_candidate_pairs(confidence)",
        "CREATE INDEX IF NOT EXISTS idx_identity_groups_group ON identity_candidate_groups(candidate_group_id)",
        "CREATE INDEX IF NOT EXISTS idx_identity_groups_author ON identity_candidate_groups(author_id)",
        "CREATE INDEX IF NOT EXISTS idx_identity_authors_author ON identity_candidate_authors(author_id)",
        "CREATE INDEX IF NOT EXISTS idx_identity_authors_group ON identity_candidate_authors(candidate_group_id)",
    ]:
        con.execute(sql)

    print()
    print("Identity candidate layer complete.")
    print()

    for label, sql in [
        ("identity_candidate_pairs", "SELECT COUNT(*) FROM identity_candidate_pairs"),
        ("identity_candidate_groups", "SELECT COUNT(DISTINCT candidate_group_id) FROM identity_candidate_groups"),
        ("authors_in_candidate_groups", "SELECT COUNT(*) FROM identity_candidate_groups"),
        ("possible_duplicate_authors", "SELECT COUNT(*) FROM identity_candidate_authors WHERE possible_identity_duplicate = TRUE"),
    ]:
        count = con.execute(sql).fetchone()[0]
        print(f"{label}: {count:,}")

    print()
    print("Confidence breakdown:")
    rows = con.execute(q("""
        SELECT confidence, COUNT(*) AS pairs
        FROM identity_candidate_pairs
        GROUP BY confidence
        ORDER BY pairs DESC
    """)).fetchall()

    for row in rows:
        print(row)

    print()
    print("Sample high-confidence pairs:")
    rows = con.execute(q("""
        SELECT
            author_name_a,
            works_a,
            main_institution_a,
            author_name_b,
            works_b,
            main_institution_b,
            confidence,
            evidence_score,
            evidence_basis
        FROM identity_candidate_pairs
        WHERE confidence IN ('very_high', 'high')
        ORDER BY evidence_score DESC
        LIMIT 20
    """)).fetchall()

    for row in rows:
        print(row)

    con.close()


if __name__ == "__main__":
    main()