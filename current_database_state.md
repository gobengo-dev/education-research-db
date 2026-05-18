# Current Database State

Snapshot date: 2026-05-18

---

# Database Overview

DuckDB database:

```text
data/research_database.duckdb
```

Current project scale:

| Entity | Approximate Rows |
|---|---|
| Works | 174k |
| Raw OpenAlex work records | 211k |
| Crossref records | 148k |
| Researchers | 237k |
| Canonical researchers | 110k |
| Institutions | 20k |
| Canonical sources | 14.8k |

---

# Core Layers

## OpenAlex-derived layers

Primary scholarly corpus source.

Main tables:

- raw_works
- works_flat
- work_sources
- work_topics
- authorships

## Crossref enrichment layers

DOI enrichment system.

Main tables:

- crossref_enrichment_scope
- crossref_enrichment_raw
- crossref_enrichment_flat
- crossref_contributors

## Researcher identity layers

Researcher normalisation and canonicalisation.

Main tables:

- researchers_raw
- candidate_researchers
- canonical_researchers
- researcher_identity_map

## Institutional layers

Institution normalisation and ROR enrichment.

Main tables:

- institutions_raw
- canonical_institutions
- canonical_institutions_enriched

## Publication classification layers

Output classification and source canonicalisation.

Main tables:

- canonical_sources
- source_metrics
- work_output_tags

---

# Provenance Architecture

Implemented in:

```text
scripts/15_create_provenance_architecture.py
```

Main provenance tables:

- harvest_runs
- harvest_sources
- raw_source_records
- source_work_claims
- work_identifier_claims
- source_record_discovery_events
- processing_runs
- processing_outputs

---

# Harvest History

## OpenAlex initial corpus

Script:

```text
01_harvest_openalex_works_to_duckdb.py
```

## OpenAlex expansion harvest

Script:

```text
17_harvest_openalex_expansion.py
```

Strategies:

- AU/NZ author topic harvesting
- Journal-level harvesting

## Crossref enrichment

Scripts:

```text
11_enrich_books_and_missing_sources_from_crossref.py
12*_enrich_all_dois_from_crossref*.py
18_harvest_crossref_delta_after_openalex_expansion.py
```

## ORCID enrichment

Script:

```text
06_harvest_orcid_profiles.py
```

## ROR ingestion

Scripts:

```text
08_ingest_ror_dump.py
09_enrich_canonical_institutions_from_ror.py
```

---

# Known Issues

## Source canonicalisation

Some source duplication still exists due to:

- encoding issues
- ISSN inconsistencies
- source title variants

## Missing-source outputs

A substantial number of outputs still lack reliable source mappings.

## Journal registry incompleteness

Australasian journal registry still incomplete.

## Topic-selection limitations

Some educationally relevant topics were omitted in early harvesting rounds.

---

# Current Active Work

1. Australasian journal expansion
2. Crossref delta harvesting
3. DataCite integration planning
4. Provenance auditing
5. Journal-level harvesting design
6. Source reconciliation

---

# Important Exports

State snapshot exports:

```text
data/exports/database_state_snapshot/
```

Publication output review exports:

```text
data/exports/publication_output_review/
```

Institution review exports:

```text
data/exports/institution_review/
```