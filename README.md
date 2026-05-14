# AU/NZ Education Research Database

A scholarly infrastructure project for constructing a large-scale research intelligence database focused on Australian and New Zealand education-related research.

## Current components

- OpenAlex publication harvest
- Canonical researcher layer
- ORCID enrichment layer
- Collaboration network construction
- Topic curation framework
- DuckDB analytical warehouse

## Scale

- ~153k works
- ~110k canonical researchers
- ~66k ORCID-enriched researchers
- ~2.5M collaboration edges

## Pipeline

1. Harvest OpenAlex works
2. Build researcher layer
3. Detect duplicate researchers
4. Build canonical identities
5. Harvest ORCID enrichment
6. Profile and normalise database
7. Build policy-tagging layer

## Technology

- Python
- DuckDB
- OpenAlex API
- ORCID Public API

## Status

Active development.