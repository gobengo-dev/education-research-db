# Database Schema

## Current Tables

### works_flat
Publication-level metadata.

Suggested fields:
- work_id
- doi
- title
- publication_year
- publication_date
- cited_by_count
- fwci
- type
- language
- abstract
- primary_topic
- open_access_status

---

### authorships
Author-publication relationships.

Suggested fields:
- work_id
- author_id
- author_name
- author_position
- institution_id
- institution_name
- country_code
- is_corresponding

---

### topics
Topic associations.

Suggested fields:
- work_id
- topic_id
- topic_name
- topic_level
- topic_score

---

### institutions
Institution metadata.

Suggested fields:
- institution_id
- institution_name
- country_code
- institution_type
- ror

---

### concepts
Concept-level tagging.

Suggested fields:
- work_id
- concept_id
- concept_name
- score

---

## Future Tables

### orcid_profiles
ORCID enrichment data.

### citation_edges
Citation network relationships.

### collaboration_edges
Author-author collaboration network.

### grants
Grant metadata and funders.

### datasets
Research dataset links.
