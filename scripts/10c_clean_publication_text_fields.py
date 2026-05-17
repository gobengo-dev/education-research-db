from pathlib import Path
import re
import unicodedata

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DUCKDB_FILE = PROJECT_ROOT / "data" / "research_database.duckdb"
EXPORT_DIR = PROJECT_ROOT / "data" / "exports" / "publication_text_cleaning"

EXPORT_DIR.mkdir(parents=True, exist_ok=True)


def clean_text(value):
    if value is None:
        return None

    text = str(value)

    text = unicodedata.normalize("NFKC", text)

    # Remove Unicode control characters, including U+0098 / U+009C.
    text = "".join(
        ch for ch in text
        if unicodedata.category(ch)[0] != "C"
    )

    # Normalise whitespace.
    text = re.sub(r"\s+", " ", text).strip()

    return text


def add_column_if_missing(con, table, column, column_type):
    cols = con.execute(f"DESCRIBE {table}").fetchall()
    existing = {row[0] for row in cols}

    if column not in existing:
        con.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def main():
    con = duckdb.connect(str(DUCKDB_FILE))

    print("Adding clean text columns if needed...")

    add_column_if_missing(con, "works_flat", "title_clean", "VARCHAR")
    add_column_if_missing(con, "work_sources", "source_name_clean", "VARCHAR")
    add_column_if_missing(con, "work_sources", "publisher_clean", "VARCHAR")

    print("Cleaning works_flat.title...")

    title_rows = con.execute("""
        SELECT work_id, title
        FROM works_flat
    """).fetchall()

    title_updates = []

    for work_id, title in title_rows:
        title_clean = clean_text(title)
        title_updates.append((title_clean, work_id))

    con.executemany(
        """
        UPDATE works_flat
        SET title_clean = ?
        WHERE work_id = ?
        """,
        title_updates,
    )

    print("Cleaning work_sources.source_name and publisher...")

    source_rows = con.execute("""
        SELECT work_id, source_name, publisher
        FROM work_sources
    """).fetchall()

    source_updates = []

    for work_id, source_name, publisher in source_rows:
        source_name_clean = clean_text(source_name)
        publisher_clean = clean_text(publisher)
        source_updates.append((source_name_clean, publisher_clean, work_id))

    con.executemany(
        """
        UPDATE work_sources
        SET source_name_clean = ?,
            publisher_clean = ?
        WHERE work_id = ?
        """,
        source_updates,
    )

    print("Creating review backup table...")

    con.execute("DROP TABLE IF EXISTS publication_text_cleaning_review")

    con.execute("""
        CREATE TABLE publication_text_cleaning_review AS
        SELECT
            wf.work_id,
            wf.title AS original_title,
            wf.title_clean,
            ws.source_name AS original_source_name,
            ws.source_name_clean,
            ws.publisher AS original_publisher,
            ws.publisher_clean,

            CASE
                WHEN wf.title IS DISTINCT FROM wf.title_clean THEN TRUE
                ELSE FALSE
            END AS title_changed,

            CASE
                WHEN ws.source_name IS DISTINCT FROM ws.source_name_clean THEN TRUE
                ELSE FALSE
            END AS source_name_changed,

            CASE
                WHEN ws.publisher IS DISTINCT FROM ws.publisher_clean THEN TRUE
                ELSE FALSE
            END AS publisher_changed

        FROM works_flat wf
        LEFT JOIN work_sources ws
            ON wf.work_id = ws.work_id
        WHERE
            wf.title IS DISTINCT FROM wf.title_clean
            OR ws.source_name IS DISTINCT FROM ws.source_name_clean
            OR ws.publisher IS DISTINCT FROM ws.publisher_clean
    """)

    print("Exporting review CSV...")

    review_path = EXPORT_DIR / "publication_text_cleaning_review.csv"

    con.execute(f"""
        COPY (
            SELECT *
            FROM publication_text_cleaning_review
            ORDER BY
                source_name_changed DESC,
                title_changed DESC,
                publisher_changed DESC,
                work_id
        ) TO '{review_path}' (HEADER, DELIMITER ',')
    """)

    print()
    print("Publication text cleaning complete.")
    print()

    checks = [
        (
            "works_flat rows",
            "SELECT COUNT(*) FROM works_flat",
        ),
        (
            "titles changed",
            """
            SELECT COUNT(*)
            FROM works_flat
            WHERE title IS DISTINCT FROM title_clean
            """,
        ),
        (
            "source names changed",
            """
            SELECT COUNT(*)
            FROM work_sources
            WHERE source_name IS DISTINCT FROM source_name_clean
            """,
        ),
        (
            "publishers changed",
            """
            SELECT COUNT(*)
            FROM work_sources
            WHERE publisher IS DISTINCT FROM publisher_clean
            """,
        ),
        (
            "review rows",
            "SELECT COUNT(*) FROM publication_text_cleaning_review",
        ),
    ]

    for label, sql in checks:
        count = con.execute(sql).fetchone()[0]
        print(f"{label}: {count:,}")

    print()
    print("Sample changed rows:")

    rows = con.execute("""
        SELECT
            original_title,
            title_clean,
            original_source_name,
            source_name_clean
        FROM publication_text_cleaning_review
        LIMIT 20
    """).fetchall()

    for row in rows:
        print(row)

    print()
    print(f"Review export: {review_path}")

    con.close()


if __name__ == "__main__":
    main()