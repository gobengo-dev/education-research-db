# Harvesting Protocol

## OpenAlex Harvest Strategy

### Inclusion Rules
Harvest all works where:
- at least one author has an AU or NZ institution
- publication date is within the target range
- work belongs to curated core or adjacent topics

---

## API Design Decisions

### Cursor Pagination
Use OpenAlex cursor pagination for scalability.

### Rate Limiting
Respect OpenAlex rate limits and polite pool recommendations.

### Deduplication
Deduplicate works by:
- OpenAlex work ID
- DOI

### Storage
Raw JSON should be preserved where possible.

---

## Future-Proofing

### Stable IDs
Always store:
- OpenAlex IDs
- ORCID IDs
- ROR IDs
- DOI

### Incremental Updating
Design future harvests around:
- updated_date
- created_date
- append-only workflows

### Reproducibility
Record:
- harvest date
- script version
- topic version
- API parameters

---

## Planned Enrichment

### ORCID
Potential additions:
- employment history
- education history
- external identifiers

### Citation Networks
Potential future citation-edge harvesting.

### Collaboration Networks
Potential future co-authorship graph generation.
