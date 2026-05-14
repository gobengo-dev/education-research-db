import duckdb

DUCKDB_FILE = "research_database.duckdb"
DROP_EXISTING_TABLE = True


def q(sql: str) -> str:
    return sql.strip()


def main():
    con = duckdb.connect(DUCKDB_FILE)

    if DROP_EXISTING_TABLE:
        con.execute("DROP TABLE IF EXISTS candidate_researchers")

    print("Building candidate_researchers...")

    con.execute(q("""
        CREATE TABLE candidate_researchers AS
        WITH au_nz_researchers AS (
            SELECT *
            FROM researchers_raw
            WHERE has_au_nz_affiliation_in_corpus = TRUE
        ),

        topic_summary AS (
            SELECT
                rt.author_id,

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
                ) AS adjacent_topic_work_links,

                STRING_AGG(
                    DISTINCT CASE
                        WHEN rt.education_relevance_tier = 'core'
                        THEN rt.topic_name END,
                    '; '
                ) AS core_topics,

                STRING_AGG(
                    DISTINCT CASE
                        WHEN rt.education_relevance_tier = 'adjacent'
                        THEN rt.topic_name END,
                    '; '
                ) AS adjacent_topics

            FROM researcher_topics rt
            GROUP BY rt.author_id
        ),

        ranked_topics AS (
            SELECT
                author_id,
                topic_name,
                education_relevance_tier,
                works_with_topic,
                citations_with_topic,
                ROW_NUMBER() OVER (
                    PARTITION BY author_id
                    ORDER BY works_with_topic DESC, citations_with_topic DESC
                ) AS rn
            FROM researcher_topics
        ),

        top_topics AS (
            SELECT
                author_id,
                STRING_AGG(
                    topic_name || ' (' || COALESCE(education_relevance_tier, 'unclassified') || ': ' || works_with_topic || ')',
                    '; '
                    ORDER BY rn
                ) AS top_topics
            FROM ranked_topics
            WHERE rn <= 10
            GROUP BY author_id
        ),

        au_nz_institution_counts AS (
            SELECT
                author_id,
                institution_id,
                institution_name,
                ror,
                country_code,
                COUNT(DISTINCT work_id) AS works_at_institution
            FROM researcher_institutions
            WHERE country_code IN ('AU', 'NZ')
            GROUP BY
                author_id,
                institution_id,
                institution_name,
                ror,
                country_code
        ),

        main_institution AS (
            SELECT *
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY author_id
                        ORDER BY works_at_institution DESC, institution_name
                    ) AS rn
                FROM au_nz_institution_counts
            )
            WHERE rn = 1
        ),

        institution_summary AS (
            SELECT
                author_id,
                STRING_AGG(
                    DISTINCT institution_name,
                    '; '
                ) AS au_nz_institution_variants,

                STRING_AGG(
                    DISTINCT COALESCE(ror, ''),
                    '; '
                ) AS au_nz_rors,

                STRING_AGG(
                    DISTINCT country_code,
                    '; '
                ) AS au_nz_countries

            FROM au_nz_institution_counts
            GROUP BY author_id
        ),

        source_counts AS (
            SELECT
                rw.author_id,
                ws.source_name,
                COUNT(DISTINCT rw.work_id) AS works_in_source
            FROM researcher_works rw
            LEFT JOIN work_sources ws
                ON rw.work_id = ws.work_id
            GROUP BY rw.author_id, ws.source_name
        ),

        ranked_sources AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY author_id
                    ORDER BY works_in_source DESC, source_name
                ) AS rn
            FROM source_counts
            WHERE source_name IS NOT NULL
        ),

        top_sources AS (
            SELECT
                author_id,
                STRING_AGG(
                    source_name || ' (' || works_in_source || ')',
                    '; '
                    ORDER BY rn
                ) AS top_sources
            FROM ranked_sources
            WHERE rn <= 10
            GROUP BY author_id
        ),

        coauthor_summary AS (
            SELECT
                author_id,
                COUNT(DISTINCT coauthor_id) AS coauthor_count,
                COUNT(DISTINCT CASE
                    WHEN coauthor_has_au_nz_affiliation = FALSE
                    THEN coauthor_id END
                ) AS international_coauthor_count
            FROM (
                SELECT
                    ce.author_id_a AS author_id,
                    ce.author_id_b AS coauthor_id,
                    COALESCE(r2.has_au_nz_affiliation_in_corpus, FALSE) AS coauthor_has_au_nz_affiliation
                FROM coauthor_edges ce
                LEFT JOIN researchers_raw r2
                    ON ce.author_id_b = r2.author_id

                UNION ALL

                SELECT
                    ce.author_id_b AS author_id,
                    ce.author_id_a AS coauthor_id,
                    COALESCE(r1.has_au_nz_affiliation_in_corpus, FALSE) AS coauthor_has_au_nz_affiliation
                FROM coauthor_edges ce
                LEFT JOIN researchers_raw r1
                    ON ce.author_id_a = r1.author_id
            )
            GROUP BY author_id
        ),

        publication_type_summary AS (
            SELECT
                author_id,
                SUM(CASE WHEN work_type = 'article' THEN 1 ELSE 0 END) AS article_count,
                SUM(CASE WHEN work_type = 'book-chapter' THEN 1 ELSE 0 END) AS book_chapter_count,
                SUM(CASE WHEN work_type = 'book' THEN 1 ELSE 0 END) AS book_count,
                SUM(CASE WHEN work_type = 'review' THEN 1 ELSE 0 END) AS review_count,
                SUM(CASE WHEN work_type = 'preprint' THEN 1 ELSE 0 END) AS preprint_count,
                SUM(CASE WHEN work_type IN ('dataset', 'erratum', 'retraction', 'peer-review') THEN 1 ELSE 0 END) AS low_relevance_type_count
            FROM researcher_works
            GROUP BY author_id
        )

        SELECT
            r.author_id,
            r.author_name,
            r.orcid,

            r.works_in_corpus,
            r.citations_in_corpus,
            r.first_year_in_corpus,
            r.latest_year_in_corpus,
            r.active_years_in_corpus,

            r.au_nz_affiliated_authorships,
            r.total_authorship_rows,

            mi.institution_id AS main_au_nz_institution_id,
            mi.institution_name AS main_au_nz_institution_name,
            mi.ror AS main_au_nz_institution_ror,
            mi.country_code AS main_au_nz_country,

            ins.au_nz_institution_variants,
            ins.au_nz_rors,
            ins.au_nz_countries,

            COALESCE(ts.distinct_core_topics, 0) AS distinct_core_topics,
            COALESCE(ts.distinct_adjacent_topics, 0) AS distinct_adjacent_topics,
            COALESCE(ts.core_topic_work_links, 0) AS core_topic_work_links,
            COALESCE(ts.adjacent_topic_work_links, 0) AS adjacent_topic_work_links,
            ts.core_topics,
            ts.adjacent_topics,
            tt.top_topics,

            src.top_sources,

            COALESCE(cs.coauthor_count, 0) AS coauthor_count,
            COALESCE(cs.international_coauthor_count, 0) AS international_coauthor_count,

            COALESCE(pt.article_count, 0) AS article_count,
            COALESCE(pt.book_chapter_count, 0) AS book_chapter_count,
            COALESCE(pt.book_count, 0) AS book_count,
            COALESCE(pt.review_count, 0) AS review_count,
            COALESCE(pt.preprint_count, 0) AS preprint_count,
            COALESCE(pt.low_relevance_type_count, 0) AS low_relevance_type_count,

            CASE
                WHEN r.orcid IS NOT NULL AND r.orcid != '' THEN TRUE
                ELSE FALSE
            END AS has_orcid,

            CASE
                WHEN r.author_name LIKE '%. %'
                  OR regexp_matches(r.author_name, '^[A-Z]\\.? [A-Za-z]')
                THEN TRUE
                ELSE FALSE
            END AS possible_initials_name,

            CASE
                WHEN r.works_in_corpus >= 10 THEN 'substantial'
                WHEN r.works_in_corpus >= 3 THEN 'moderate'
                ELSE 'small'
            END AS corpus_presence_level,

            CASE
                WHEN COALESCE(ts.core_topic_work_links, 0) > 0
                  AND COALESCE(ts.adjacent_topic_work_links, 0) > 0
                THEN 'core_and_adjacent'
                WHEN COALESCE(ts.core_topic_work_links, 0) > 0
                THEN 'core_only'
                WHEN COALESCE(ts.adjacent_topic_work_links, 0) > 0
                THEN 'adjacent_only'
                ELSE 'unclassified'
            END AS education_topic_profile

        FROM au_nz_researchers r
        LEFT JOIN topic_summary ts
            ON r.author_id = ts.author_id
        LEFT JOIN top_topics tt
            ON r.author_id = tt.author_id
        LEFT JOIN main_institution mi
            ON r.author_id = mi.author_id
        LEFT JOIN institution_summary ins
            ON r.author_id = ins.author_id
        LEFT JOIN top_sources src
            ON r.author_id = src.author_id
        LEFT JOIN coauthor_summary cs
            ON r.author_id = cs.author_id
        LEFT JOIN publication_type_summary pt
            ON r.author_id = pt.author_id
    """))

    print("Creating indexes...")

    con.execute("CREATE INDEX IF NOT EXISTS idx_candidate_researchers_author_id ON candidate_researchers(author_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_candidate_researchers_name ON candidate_researchers(author_name)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_candidate_researchers_inst ON candidate_researchers(main_au_nz_institution_id)")
    con.execute("CREATE INDEX IF NOT EXISTS idx_candidate_researchers_ror ON candidate_researchers(main_au_nz_institution_ror)")

    print()
    print("candidate_researchers built.")
    count = con.execute("SELECT COUNT(*) FROM candidate_researchers").fetchone()[0]
    print(f"Rows: {count:,}")

    print()
    print("Top 20 by works_in_corpus:")
    rows = con.execute(q("""
        SELECT
            author_name,
            works_in_corpus,
            citations_in_corpus,
            main_au_nz_institution_name,
            education_topic_profile
        FROM candidate_researchers
        ORDER BY works_in_corpus DESC
        LIMIT 20
    """)).fetchall()

    for row in rows:
        print(row)

    con.close()


if __name__ == "__main__":
    main()