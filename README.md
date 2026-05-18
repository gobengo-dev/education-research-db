# Australasian Education Research Database

A large-scale research infrastructure project for building a near-comprehensive, provenance-aware database of Australasian educational research outputs and associated scholarly entities.

The project integrates metadata and enrichment from multiple scholarly systems, including:

- OpenAlex
- Crossref
- ORCID
- ROR
- Future planned integrations:
  - DataCite
  - Institutional repositories
  - Journal-level harvesting
  - Grey literature collections

The database is designed to support:

- Educational research mapping
- Researcher identity resolution
- Institutional analysis
- Journal ecosystem analysis
- Policy and funding analysis
- Topic modelling and ontology work
- Bibliometric and scientometric analysis
- Longitudinal corpus construction
- Future downstream NLP and AI workflows

---

# Current Scale (May 2026)

## Core scholarly entities

- ~175k works
- ~211k raw OpenAlex work records
- ~148k Crossref records
- ~237k researchers
- ~20k institutions
- ~14.8k canonical publication sources

## Provenance architecture

The database now includes provenance-aware infrastructure for:

- harvest runs
- source systems
- raw harvested records
- identifier claims
- work-source claims
- discovery events
- processing lineage

---

# Repository Structure

```text
data/
    curated/
    exports/
    reference/
    research_database.duckdb

docs/
    architecture/
    provenance/
    harvesting/

scripts/

```

---

# Major Systems

## OpenAlex harvesting

Primary scholarly discovery layer.

Used for:

- work harvesting
- topic harvesting
- source harvesting
- authorships
- citation metadata
- topic tagging
- OA metadata

## Crossref enrichment

Used for:

- DOI enrichment
- contributor metadata
- ISBN/ISSN enrichment
- publisher metadata
- citation counts
- licensing metadata

## ORCID enrichment

Used for:

- researcher enrichment
- employment history
- education history
- external identifiers
- websites
- keywords

## ROR enrichment

Used for:

- institutional normalisation
- institutional hierarchy
- external identifiers
- geospatial metadata
- institutional relationship mapping

---

# Design Principles

## Provenance first

Every harvested record should be traceable to:

- source system
- harvest run
- harvest method
- ingestion script
- discovery context
- retrieval timestamp

## Raw preservation

Raw source records are preserved before flattening or transformation.

## Rebuildability

Derived layers should be reproducible from raw layers.

## Incremental expansion

Future harvesting should append provenance-aware records rather than overwrite historical state.

## Auditability

The system is intended to support future audit, validation, and reproducibility workflows.

---

# Important Tables

## Core work tables

- raw_works
- works_flat
- work_sources
- work_topics
- work_output_tags

## Researcher layers

- authorships
- researchers_raw
- candidate_researchers
- canonical_researchers
- researcher_identity_map

## Institutional layers

- institutions_raw
- institution_identity_map
- canonical_institutions
- canonical_institutions_enriched

## Provenance layers

- harvest_runs
- harvest_sources
- raw_source_records
- source_work_claims
- work_identifier_claims
- source_record_discovery_events
- processing_runs
- processing_outputs

---

# Current Priorities

1. Expand Australasian journal coverage
2. Harvest missing DOIs
3. Add DataCite enrichment
4. Improve source canonicalisation
5. Reduce missing-source outputs
6. Audit provenance integrity
7. Expand journal registry coverage
8. Prepare journal-level harvesting workflows

---

# Important Notes

This repository is under active development.

Schemas, scripts, and harvesting protocols are evolving rapidly.

Avoid treating derived tables as authoritative without checking:

- provenance
- harvest dates
- rebuild status
- processing version

---

# License

Internal research infrastructure project.