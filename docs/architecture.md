# Research Database Architecture

## Overview

This project builds a large-scale education research database using:
- OpenAlex
- ORCID
- DuckDB
- Python harvesting pipelines

Primary goals:
1. Identify education researchers in Australia and New Zealand
2. Harvest publication metadata
3. Map collaboration networks
4. Analyse topics, influence, and trends
5. Support future updating and enrichment workflows

---

## Core Components

### Topic Curation
Curated OpenAlex topics classified into:
- Core
- Adjacent
- Excluded

### Harvest Pipeline
Python scripts query the OpenAlex API and ingest works into DuckDB.

### Database Layer
DuckDB stores:
- works
- authorships
- institutions
- topics
- concepts
- citations
- collaborations

### Enrichment
Future enrichment sources:
- ORCID
- Crossref
- Scopus
- Google Scholar
- Institutional repositories

### Analysis
Potential outputs:
- Researcher rankings
- Collaboration maps
- Topic clustering
- Institutional benchmarking
- Citation trajectories
