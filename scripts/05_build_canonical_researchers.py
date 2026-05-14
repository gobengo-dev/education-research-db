import duckdb

DUCKDB_FILE = "research_database.duckdb"
DROP_EXISTING_TABLES = True

# Conservative for now.
# Only auto-merge candidates that were classified as very_high.
AUTO_MERGE_CONFIDENCES = ("very_high",)


def q(sql: str) -> str:
    return sql.strip()


def main():
    con = duckdb.connect(DUCKDB_FILE)

    if DROP_EXISTING_TABLES:
        for table in [
            "researcher_identity_map",
            "canonical_researchers",
            "canonical_researcher_metrics",
        ]:
            con.execute(f"DROP TABLE IF EXISTS {table}")

    print("Building conservative canonical identity map...")

    confidence_list = ", ".join(f"'{x}'" for x in AUTO_MERGE_CONFIDENCES)

    # Build graph groups from selected identity candidate pairs.
    pairs = con.execute(q(f"""
        SELECT author_id_a, author_id_b, confidence
        FROM identity_candidate_pairs
        WHERE confidence IN ({confidence_list})
    """)).fetchall()

    all_authors = con.execute(q("""
        SELECT
            author_id,
            author_name,
            orcid,
            works_in_corpus,
            citations_in_corpus,
            main_au_nz_institution_name,
            main_au_nz_institution_ror
        FROM candidate_researchers
    """)).fetchall()

    author_data = {
        row[0]: {
            "author_id": row[0],
            "author_name": row[1],
            "orcid": row[2],
            "works": row[3] or 0,
            "citations": row[4] or 0,
            "institution": row[5],
            "ror": row[6],
        }
        for row in all_authors
    }

    graph = {author_id: set() for author_id in author_data}

    for a, b, confidence in pairs:
        if a in graph and b in graph:
            graph[a].add(b)
            graph[b].add(a)

    visited = set()
    identity_map_rows = []
    canonical_rows = []

    canonical_counter = 0

    for author_id in sorted(author_data.keys()):
        if author_id in visited:
            continue

        stack = [author_id]
        group = []

        while stack:
            current = stack.pop()
            if current in visited:
                continue

            visited.add(current)
            group.append(current)

            for neighbour in graph.get(current, []):
                if neighbour not in visited:
                    stack.append(neighbour)

        # Pick canonical member:
        # 1. most works
        # 2. most citations
        # 3. has ORCID
        # 4. lexical author_id for deterministic tie-break
        canonical_author_id = sorted(
            group,
            key=lambda x: (
                author_data[x]["works"],
                author_data[x]["citations"],
                1 if author_data[x]["orcid"] else 0,
                x,
            ),
            reverse=True,
        )[0]

        canonical_counter += 1
        canonical_researcher_id = f"CR{canonical_counter:08d}"

        canonical = author_data[canonical_author_id]
        group_size = len(group)

        if group_size > 1:
            merge_status = "auto_merged_very_high"
            merge_confidence = "very_high"
        else:
            merge_status = "singleton"
            merge_confidence = "none"

        canonical_rows.append((
            canonical_researcher_id,
            canonical_author_id,
            canonical["author_name"],
            canonical["orcid"],
            canonical["institution"],
            canonical["ror"],
            group_size,
            merge_status,
            merge_confidence,
        ))

        for member_author_id in group:
            member = author_data[member_author_id]

            if member_author_id == canonical_author_id:
                is_canonical = True
                map_status = "canonical_self"
            elif group_size > 1:
                is_canonical = False
                map_status = "mapped_to_canonical"
            else:
                is_canonical = True
                map_status = "singleton_self"

            identity_map_rows.append((
                member_author_id,
                member["author_name"],
                member["orcid"],
                canonical_researcher_id,
                canonical_author_id,
                canonical["author_name"],
                is_canonical,
                group_size,
                map_status,
                merge_confidence,
            ))

    print(f"Canonical researchers: {len(canonical_rows):,}")
    print(f"Identity map rows: {len(identity_map_rows):,}")

    con.execute(q("""
        CREATE TABLE canonical_researchers (
            canonical_researcher_id VARCHAR,
            canonical_author_id VARCHAR,
            canonical_name VARCHAR,
            canonical_orcid VARCHAR,
            main_au_nz_institution_name VARCHAR,
            main_au_nz_institution_ror VARCHAR,
            source_author_count INTEGER,
            merge_status VARCHAR,
            merge_confidence VARCHAR
        )
    """))

    con.executemany(
        "INSERT INTO canonical_researchers VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        canonical_rows,
    )

    con.execute(q("""
        CREATE TABLE researcher_identity_map (
            openalex_author_id VARCHAR,
            openalex_author_name VARCHAR,
            openalex_orcid VARCHAR,
            canonical_researcher_id VARCHAR,
            canonical_author_id VARCHAR,
            canonical_name VARCHAR,
            is_canonical_author BOOLEAN,
            source_author_count INTEGER,
            map_status VARCHAR,
            merge_confidence VARCHAR
        )
    """))

    con.executemany(
        "INSERT INTO researcher_identity_map VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        identity_map_rows,
    )

    print("Building canonical_researcher_metrics...")

    con.execute(q("""
        CREATE TABLE canonical_researcher_metrics AS
        WITH mapped_works AS (
            SELECT DISTINCT
                rim.canonical_researcher_id,
                rim.canonical_name,
                rim.canonical_author_id,
                rim.canonical_author_id = rw.author_id AS is_canonical_author_work,
                rw.author_id AS source_author_id,
                rw.work_id,
                rw.title,
                rw.publication_year,
                rw.work_type,
                rw.cited_by_count,
                rw.fwci,
                rw.citation_percentile,
                rw.is_au_nz_affiliated
            FROM researcher_works rw
            JOIN researcher_identity_map rim
                ON rw.author_id = rim.openalex_author_id
        ),

        basic_metrics AS (
            SELECT
                canonical_researcher_id,
                ANY_VALUE(canonical_name) AS canonical_name,
                ANY_VALUE(canonical_author_id) AS canonical_author_id,
                COUNT(DISTINCT source_author_id) AS source_author_count,
                COUNT(DISTINCT work_id) AS works_in_corpus,
                SUM(COALESCE(cited_by_count, 0)) AS citations_in_corpus,
                MIN(publication_year) AS first_year_in_corpus,
                MAX(publication_year) AS latest_year_in_corpus,
                COUNT(DISTINCT publication_year) AS active_years_in_corpus,
                SUM(CASE WHEN is_au_nz_affiliated THEN 1 ELSE 0 END) AS au_nz_affiliated_authorships,

                SUM(CASE WHEN work_type = 'article' THEN 1 ELSE 0 END) AS article_count,
                SUM(CASE WHEN work_type = 'review' THEN 1 ELSE 0 END) AS review_count,
                SUM(CASE WHEN work_type = 'book' THEN 1 ELSE 0 END) AS book_count,
                SUM(CASE WHEN work_type = 'book-chapter' THEN 1 ELSE 0 END) AS book_chapter_count,
                SUM(CASE WHEN work_type = 'preprint' THEN 1 ELSE 0 END) AS preprint_count,
                SUM(CASE WHEN work_type IN ('dataset', 'erratum', 'retraction', 'peer-review') THEN 1 ELSE 0 END) AS low_relevance_type_count
            FROM mapped_works
            GROUP BY canonical_researcher_id
        ),

        topic_metrics AS (
            SELECT
                rim.canonical_researcher_id,

                COUNT(DISTINCT CASE
                    WHEN rt.education_relevance_tier = 'core'
                    THEN rt.topic_id END
                ) AS distinct_core_topics,

                COUNT(DISTINCT CASE
                    WHEN rt.education_relevance_tier = 'adjacent'
                    THEN rt.topic_id END
                ) AS distinct_adjacent_topics,

                SUM(CASE
                    WHEN rt.education_relevance_tier = 'core'
                    THEN rt.works_with_topic ELSE 0 END
                ) AS core_topic_work_links,

                SUM(CASE
                    WHEN rt.education_relevance_tier = 'adjacent'
                    THEN rt.works_with_topic ELSE 0 END
                ) AS adjacent_topic_work_links

            FROM researcher_topics rt
            JOIN researcher_identity_map rim
                ON rt.author_id = rim.openalex_author_id
            GROUP BY rim.canonical_researcher_id
        ),

        institution_metrics AS (
            SELECT
                rim.canonical_researcher_id,
                STRING_AGG(DISTINCT ri.institution_name, '; ') AS au_nz_institution_variants,
                STRING_AGG(DISTINCT ri.ror, '; ') AS au_nz_rors,
                STRING_AGG(DISTINCT ri.country_code, '; ') AS au_nz_countries
            FROM researcher_institutions ri
            JOIN researcher_identity_map rim
                ON ri.author_id = rim.openalex_author_id
            WHERE ri.country_code IN ('AU', 'NZ')
            GROUP BY rim.canonical_researcher_id
        ),

        main_inst_counts AS (
            SELECT
                rim.canonical_researcher_id,
                ri.institution_id,
                ri.institution_name,
                ri.ror,
                ri.country_code,
                COUNT(DISTINCT ri.work_id) AS works_at_institution
            FROM researcher_institutions ri
            JOIN researcher_identity_map rim
                ON ri.author_id = rim.openalex_author_id
            WHERE ri.country_code IN ('AU', 'NZ')
            GROUP BY
                rim.canonical_researcher_id,
                ri.institution_id,
                ri.institution_name,
                ri.ror,
                ri.country_code
        ),

        main_inst AS (
            SELECT *
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY canonical_researcher_id
                        ORDER BY works_at_institution DESC, institution_name
                    ) AS rn
                FROM main_inst_counts
            )
            WHERE rn = 1
        )

        SELECT
            bm.canonical_researcher_id,
            bm.canonical_name,
            bm.canonical_author_id,
            cr.canonical_orcid,

            bm.source_author_count,
            cr.merge_status,
            cr.merge_confidence,

            bm.works_in_corpus,
            bm.citations_in_corpus,
            bm.first_year_in_corpus,
            bm.latest_year_in_corpus,
            bm.active_years_in_corpus,
            bm.au_nz_affiliated_authorships,

            mi.institution_id AS main_au_nz_institution_id,
            mi.institution_name AS main_au_nz_institution_name,
            mi.ror AS main_au_nz_institution_ror,
            mi.country_code AS main_au_nz_country,

            im.au_nz_institution_variants,
            im.au_nz_rors,
            im.au_nz_countries,

            COALESCE(tm.distinct_core_topics, 0) AS distinct_core_topics,
            COALESCE(tm.distinct_adjacent_topics, 0) AS distinct_adjacent_topics,
            COALESCE(tm.core_topic_work_links, 0) AS core_topic_work_links,
            COALESCE(tm.adjacent_topic_work_links, 0) AS adjacent_topic_work_links,

            bm.article_count,
            bm.review_count,
            bm.book_count,
            bm.book_chapter_count,
            bm.preprint_count,
            bm.low_relevance_type_count,

            bm.article_count + bm.review_count + bm.book_count + bm.book_chapter_count AS formal_publication_count,

            CASE
                WHEN COALESCE(tm.core_topic_work_links, 0) > 0
                  AND COALESCE(tm.adjacent_topic_work_links, 0) > 0
                THEN 'core_and_adjacent'
                WHEN COALESCE(tm.core_topic_work_links, 0) > 0
                THEN 'core_only'
                WHEN COALESCE(tm.adjacent_topic_work_links, 0) > 0
                THEN 'adjacent_only'
                ELSE 'unclassified'
            END AS education_topic_profile

        FROM basic_metrics bm
        JOIN canonical_researchers cr
            ON bm.canonical_researcher_id = cr.canonical_researcher_id
        LEFT JOIN topic_metrics tm
            ON bm.canonical_researcher_id = tm.canonical_researcher_id
        LEFT JOIN institution_metrics im
            ON bm.canonical_researcher_id = im.canonical_researcher_id
        LEFT JOIN main_inst mi
            ON bm.canonical_researcher_id = mi.canonical_researcher_id
    """))

    print("Creating indexes...")

    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_rim_openalex ON researcher_identity_map(openalex_author_id)",
        "CREATE INDEX IF NOT EXISTS idx_rim_canonical ON researcher_identity_map(canonical_researcher_id)",
        "CREATE INDEX IF NOT EXISTS idx_cr_id ON canonical_researchers(canonical_researcher_id)",
        "CREATE INDEX IF NOT EXISTS idx_cr_author ON canonical_researchers(canonical_author_id)",
        "CREATE INDEX IF NOT EXISTS idx_crm_id ON canonical_researcher_metrics(canonical_researcher_id)",
        "CREATE INDEX IF NOT EXISTS idx_crm_name ON canonical_researcher_metrics(canonical_name)",
        "CREATE INDEX IF NOT EXISTS idx_crm_inst ON canonical_researcher_metrics(main_au_nz_institution_ror)",
    ]:
        con.execute(sql)

    print()
    print("Canonical researcher layer complete.")
    print()

    for label, sql in [
        ("canonical_researchers", "SELECT COUNT(*) FROM canonical_researchers"),
        ("researcher_identity_map", "SELECT COUNT(*) FROM researcher_identity_map"),
        ("auto-merged canonical researchers", "SELECT COUNT(*) FROM canonical_researchers WHERE source_author_count > 1"),
        ("source author IDs absorbed into merges", "SELECT COUNT(*) FROM researcher_identity_map WHERE source_author_count > 1"),
        ("canonical_researcher_metrics", "SELECT COUNT(*) FROM canonical_researcher_metrics"),
    ]:
        count = con.execute(sql).fetchone()[0]
        print(f"{label}: {count:,}")

    print()
    print("Top 20 canonical researchers by works_in_corpus:")
    rows = con.execute(q("""
        SELECT
            canonical_name,
            works_in_corpus,
            formal_publication_count,
            citations_in_corpus,
            source_author_count,
            merge_status,
            main_au_nz_institution_name
        FROM canonical_researcher_metrics
        ORDER BY works_in_corpus DESC
        LIMIT 20
    """)).fetchall()

    for row in rows:
        print(row)

    con.close()


if __name__ == "__main__":
    main()