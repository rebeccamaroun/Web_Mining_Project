# Data Directory

## Structure

```
data/
├── raw/
│   └── edtech_corpus.jsonl        # Crawled articles (8 documents)
├── processed/
│   └── entities_relations.jsonl   # NER output (entities, relations, verb triples)
├── samples/
│   └── (sample queries and outputs)
└── README.md
```

## Raw Data

`edtech_corpus.jsonl` contains 8 crawled articles in JSONL format. Each line is a JSON object with:
- `url`: Source URL
- `word_count`: Number of words
- `text`: Extracted main text content
- `title`: Article title (when available)
- `date`: Publication date (when available)

### Sources

| # | Source | Topic | Words |
|---|--------|-------|-------|
| 1 | AERA | Education technology research | ~5,000 |
| 2 | PMC (9247945) | E-learning in Iraq/Kurdistan | ~6,400 |
| 3 | PMC (8455229) | AI ethics in K-12 education | ~7,100 |
| 4 | Purdue University | Technology in education | ~735 |
| 5 | Stanford HAI | AI + Education Summit | ~1,600 |
| 6 | PMC (9069679) | AML immunotherapy | ~5,300 |
| 7 | PMC (10020843) | Burn infections | ~5,100 |
| 8 | PMC (8893233) | Bone mechanical stress | ~12,100 |

Documents 1-5 are on-domain (Education & AI). Documents 6-8 are off-domain (medical/biology) and were included from the initial crawl.

## Processed Data

`entities_relations.jsonl` contains NER extraction results per document:
- `entities`: List of `{text, label, start, end}` objects
- `relations`: Co-occurrence relation triples
- `verb_triples`: Dependency-parsed verb-based triples
- `url`: Source document URL

### Entity Statistics

| Type | Count |
|------|-------|
| PERSON | 1,427 |
| ORG | 348 |
| GPE | 65 |
| WORK_OF_ART | 30 |
| EVENT | 7 |
| FAC | 1 |
| **Total** | **1,878** |

## Reproduction

To regenerate the data:

```bash
# Crawl articles
python src/phase1_crawler.py

# Run NER
python src/phase2_ner.py
```