"""
Phase 4c: Deep KB Expansion
Aggressive expansion to reach 50k+ triples by:
1. Pulling ALL properties for linked entities (not just a filtered set)
2. Expanding from Wikidata entities discovered in round 1
3. Adding domain-specific subgraphs (AI, education, edtech, universities)
4. Adding sitelinks, external IDs, and richer metadata
"""

import json
import os
import time
import requests
from rdflib import Graph, Namespace, Literal, URIRef, RDF, RDFS, OWL, XSD

# ── Config ─────────────────────────────────────────────────────
MAPPING_FILE = "kg_artifacts/entity_mapping.json"
EXPANDED_KG_FILE = "kg_artifacts/knowledge_graph_expanded.ttl"
FINAL_KG_FILE = "kg_artifacts/knowledge_graph_final.ttl"
FINAL_STATS_FILE = "kg_artifacts/final_statistics.txt"

EDAI = Namespace("http://example.org/edai/")
WD = Namespace("http://www.wikidata.org/entity/")
WDT = Namespace("http://www.wikidata.org/prop/direct/")
SCHEMA = Namespace("http://schema.org/")

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
HEADERS = {
    "User-Agent": "WebMiningProject/1.0 (educational project; contact: student@cnam.fr)",
    "Accept": "application/json",
}


def run_sparql(query: str, retries: int = 3) -> list:
    for attempt in range(retries):
        try:
            resp = requests.get(
                WIKIDATA_SPARQL,
                params={"query": query, "format": "json"},
                headers=HEADERS,
                timeout=90,
            )
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                print(f"    ⚠️  Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            if resp.status_code == 500:
                print(f"    ⚠️  Server error, retrying...")
                time.sleep(10)
                continue
            resp.raise_for_status()
            return resp.json().get("results", {}).get("bindings", [])
        except Exception as e:
            if attempt < retries - 1:
                print(f"    ⚠️  Error (attempt {attempt+1}): {e}")
                time.sleep(5 * (attempt + 1))
            else:
                print(f"    ❌  Failed: {e}")
                return []
    return []


def add_results_to_graph(results: list, g: Graph) -> int:
    """Generic helper to add SPARQL results to the graph."""
    count = 0
    for row in results:
        triples_to_add = []

        # Extract subject
        if "s" in row:
            s_val = row["s"]["value"]
            s = URIRef(s_val)
        elif "entity" in row:
            s_val = row["entity"]["value"]
            s = URIRef(s_val)
        elif "item" in row:
            s_val = row["item"]["value"]
            s = URIRef(s_val)
        else:
            continue

        # Extract predicate
        if "p" in row:
            p = URIRef(row["p"]["value"])
        elif "prop" in row:
            p = URIRef(row["prop"]["value"])
        else:
            p = EDAI.relatedTo

        # Extract object
        if "o" in row:
            o_raw = row["o"]
        elif "value" in row:
            o_raw = row["value"]
        else:
            continue

        if o_raw["type"] == "uri":
            o = URIRef(o_raw["value"])
        else:
            o = Literal(o_raw["value"])

        g.add((s, p, o))
        count += 1

        # Add labels where available
        for label_key in ("sLabel", "entityLabel", "itemLabel"):
            if label_key in row and row[label_key]["value"]:
                g.add((s, RDFS.label, Literal(row[label_key]["value"], lang="en")))
                count += 1
                break

        for label_key in ("oLabel", "valueLabel"):
            if label_key in row and row[label_key]["value"]:
                if o_raw["type"] == "uri":
                    g.add((URIRef(o_raw["value"]), RDFS.label,
                           Literal(row[label_key]["value"], lang="en")))
                    count += 1
                break

    return count


def expand_all_properties(mappings: list, g: Graph) -> int:
    """Pull ALL direct properties for each linked entity (no filtering)."""
    total = 0
    qids = [m["wikidata_qid"] for m in mappings]

    for i in range(0, len(qids), 10):
        batch = qids[i:i+10]
        values = " ".join(f"wd:{q}" for q in batch)

        query = f"""
        SELECT ?entity ?entityLabel ?p ?o ?oLabel WHERE {{
            VALUES ?entity {{ {values} }}
            ?entity ?p ?o .
            FILTER(STRSTARTS(STR(?p), "http://www.wikidata.org/prop/direct/"))
            OPTIONAL {{ ?entity rdfs:label ?entityLabel . FILTER(LANG(?entityLabel) = "en") }}
            OPTIONAL {{ ?o rdfs:label ?oLabel . FILTER(LANG(?oLabel) = "en") }}
        }}
        """

        results = run_sparql(query)
        time.sleep(2)

        for row in results:
            entity_uri = URIRef(row["entity"]["value"])
            prop_uri = URIRef(row["p"]["value"])
            o_raw = row["o"]

            if o_raw["type"] == "uri":
                obj = URIRef(o_raw["value"])
            else:
                obj = Literal(o_raw["value"])

            g.add((entity_uri, prop_uri, obj))
            total += 1

            el = row.get("entityLabel", {}).get("value", "")
            ol = row.get("oLabel", {}).get("value", "")
            if el:
                g.add((entity_uri, RDFS.label, Literal(el, lang="en")))
                total += 1
            if ol and o_raw["type"] == "uri":
                g.add((URIRef(o_raw["value"]), RDFS.label, Literal(ol, lang="en")))
                total += 1

        batch_num = i // 10 + 1
        total_batches = (len(qids) + 9) // 10
        print(f"    Batch {batch_num}/{total_batches}: running total {total}")

    return total


def expand_domain_subgraphs(g: Graph) -> int:
    """Pull large domain-relevant subgraphs from Wikidata."""
    total = 0

    domain_queries = [
        # Universities mentioned in our corpus + their properties
        (
            "Top universities",
            """
            SELECT ?item ?itemLabel ?p ?value ?valueLabel WHERE {
                VALUES ?item {
                    wd:Q49108 wd:Q13371 wd:Q161562 wd:Q130965 wd:Q34433
                    wd:Q190080 wd:Q174570 wd:Q309988 wd:Q838330 wd:Q49210
                    wd:Q49115 wd:Q21578 wd:Q131252 wd:Q34433 wd:Q219694
                    wd:Q232141 wd:Q859363 wd:Q503415 wd:Q270222 wd:Q4116236
                }
                VALUES ?p {
                    wdt:P31 wdt:P17 wdt:P131 wdt:P159 wdt:P571 wdt:P112
                    wdt:P856 wdt:P1082 wdt:P625 wdt:P18 wdt:P154 wdt:P36
                    wdt:P361 wdt:P527 wdt:P101 wdt:P452 wdt:P910 wdt:P749
                    wdt:P355 wdt:P1128 wdt:P169 wdt:P6 wdt:P37 wdt:P279
                    wdt:P69 wdt:P108 wdt:P27 wdt:P106
                }
                ?item ?p ?value .
                OPTIONAL { ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "en") }
                OPTIONAL { ?value rdfs:label ?valueLabel . FILTER(LANG(?valueLabel) = "en") }
            }
            """
        ),
        # AI concepts and their taxonomy
        (
            "AI taxonomy",
            """
            SELECT ?item ?itemLabel ?p ?value ?valueLabel WHERE {
                {
                    ?item wdt:P31/wdt:P279* wd:Q11660 .
                } UNION {
                    ?item wdt:P279+ wd:Q11660 .
                } UNION {
                    ?item wdt:P361 wd:Q11660 .
                }
                VALUES ?p { wdt:P31 wdt:P279 wdt:P361 wdt:P527 wdt:P101 wdt:P1343 }
                ?item ?p ?value .
                OPTIONAL { ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "en") }
                OPTIONAL { ?value rdfs:label ?valueLabel . FILTER(LANG(?valueLabel) = "en") }
            }
            LIMIT 3000
            """
        ),
        # Machine learning subfield taxonomy
        (
            "Machine learning taxonomy",
            """
            SELECT ?item ?itemLabel ?p ?value ?valueLabel WHERE {
                {
                    ?item wdt:P279+ wd:Q2539 .
                } UNION {
                    ?item wdt:P31 wd:Q2539 .
                } UNION {
                    ?item wdt:P361 wd:Q2539 .
                }
                VALUES ?p { wdt:P31 wdt:P279 wdt:P361 wdt:P527 wdt:P101 }
                ?item ?p ?value .
                OPTIONAL { ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "en") }
                OPTIONAL { ?value rdfs:label ?valueLabel . FILTER(LANG(?valueLabel) = "en") }
            }
            LIMIT 3000
            """
        ),
        # Education technology and e-learning
        (
            "EdTech and e-learning",
            """
            SELECT ?item ?itemLabel ?p ?value ?valueLabel WHERE {
                {
                    ?item wdt:P31/wdt:P279* wd:Q200790 .
                } UNION {
                    ?item wdt:P279+ wd:Q200790 .
                } UNION {
                    ?item wdt:P31/wdt:P279* wd:Q609295 .
                } UNION {
                    ?item wdt:P279+ wd:Q609295 .
                }
                VALUES ?p { wdt:P31 wdt:P279 wdt:P361 wdt:P527 wdt:P17 wdt:P571 wdt:P856 }
                ?item ?p ?value .
                OPTIONAL { ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "en") }
                OPTIONAL { ?value rdfs:label ?valueLabel . FILTER(LANG(?valueLabel) = "en") }
            }
            LIMIT 3000
            """
        ),
        # Countries from our corpus with rich metadata
        (
            "Countries metadata",
            """
            SELECT ?item ?itemLabel ?p ?value ?valueLabel WHERE {
                VALUES ?item {
                    wd:Q794 wd:Q30 wd:Q183 wd:Q668 wd:Q148 wd:Q145 wd:Q739
                    wd:Q114 wd:Q115 wd:Q1036 wd:Q796 wd:Q1033 wd:Q865
                    wd:Q408 wd:Q298 wd:Q884 wd:Q36 wd:Q38 wd:Q142 wd:Q159
                }
                VALUES ?p {
                    wdt:P31 wdt:P30 wdt:P36 wdt:P37 wdt:P38 wdt:P1082
                    wdt:P6 wdt:P17 wdt:P131 wdt:P47 wdt:P421 wdt:P463
                    wdt:P856 wdt:P18 wdt:P41 wdt:P242 wdt:P571 wdt:P2044
                    wdt:P2046 wdt:P1081 wdt:P4841 wdt:P2132
                }
                ?item ?p ?value .
                OPTIONAL { ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "en") }
                OPTIONAL { ?value rdfs:label ?valueLabel . FILTER(LANG(?valueLabel) = "en") }
            }
            """
        ),
        # Tech companies from our corpus
        (
            "Tech companies",
            """
            SELECT ?item ?itemLabel ?p ?value ?valueLabel WHERE {
                VALUES ?item {
                    wd:Q95 wd:Q355 wd:Q2283 wd:Q312 wd:Q3884 wd:Q907311
                    wd:Q866 wd:Q40984 wd:Q37156 wd:Q2918660 wd:Q16539734
                    wd:Q94979732 wd:Q1122074 wd:Q21096327 wd:Q7014807
                    wd:Q217595 wd:Q180445 wd:Q1373549
                }
                VALUES ?p {
                    wdt:P31 wdt:P17 wdt:P159 wdt:P571 wdt:P112 wdt:P169
                    wdt:P452 wdt:P1128 wdt:P749 wdt:P355 wdt:P856 wdt:P154
                    wdt:P18 wdt:P279 wdt:P361 wdt:P527 wdt:P101 wdt:P910
                    wdt:P2139 wdt:P2295 wdt:P414 wdt:P740 wdt:P37
                }
                ?item ?p ?value .
                OPTIONAL { ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "en") }
                OPTIONAL { ?value rdfs:label ?valueLabel . FILTER(LANG(?valueLabel) = "en") }
            }
            """
        ),
        # Academic journals from our domain
        (
            "Academic journals",
            """
            SELECT ?item ?itemLabel ?p ?value ?valueLabel WHERE {
                VALUES ?item {
                    wd:Q4743659 wd:Q7318370 wd:Q180445 wd:Q2734495 wd:Q5188229
                    wd:Q3960429 wd:Q3386948 wd:Q27726020 wd:Q15763347
                }
                VALUES ?p {
                    wdt:P31 wdt:P279 wdt:P921 wdt:P123 wdt:P1476 wdt:P856
                    wdt:P236 wdt:P571 wdt:P17 wdt:P495 wdt:P1055 wdt:P859
                    wdt:P527 wdt:P361 wdt:P910 wdt:P1433 wdt:P101
                }
                ?item ?p ?value .
                OPTIONAL { ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "en") }
                OPTIONAL { ?value rdfs:label ?valueLabel . FILTER(LANG(?valueLabel) = "en") }
            }
            """
        ),
        # Deep learning and NLP concepts
        (
            "Deep learning & NLP",
            """
            SELECT ?item ?itemLabel ?p ?value ?valueLabel WHERE {
                {
                    ?item wdt:P279+ wd:Q197536 .
                } UNION {
                    ?item wdt:P31 wd:Q197536 .
                } UNION {
                    ?item wdt:P279+ wd:Q30642 .
                } UNION {
                    ?item wdt:P361 wd:Q30642 .
                }
                VALUES ?p { wdt:P31 wdt:P279 wdt:P361 wdt:P527 wdt:P101 wdt:P1343 }
                ?item ?p ?value .
                OPTIONAL { ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "en") }
                OPTIONAL { ?value rdfs:label ?valueLabel . FILTER(LANG(?valueLabel) = "en") }
            }
            LIMIT 3000
            """
        ),
        # Education systems and pedagogy
        (
            "Education systems & pedagogy",
            """
            SELECT ?item ?itemLabel ?p ?value ?valueLabel WHERE {
                {
                    ?item wdt:P279+ wd:Q8434 .
                    FILTER NOT EXISTS { ?item wdt:P31 wd:Q3918 }
                } UNION {
                    ?item wdt:P31/wdt:P279* wd:Q333092 .
                } UNION {
                    ?item wdt:P279+ wd:Q1397685 .
                }
                VALUES ?p { wdt:P31 wdt:P279 wdt:P361 wdt:P527 wdt:P101 wdt:P17 }
                ?item ?p ?value .
                OPTIONAL { ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "en") }
                OPTIONAL { ?value rdfs:label ?valueLabel . FILTER(LANG(?valueLabel) = "en") }
            }
            LIMIT 3000
            """
        ),
        # COVID-19 impact on education (domain-relevant event)
        (
            "COVID-19 and education",
            """
            SELECT ?item ?itemLabel ?p ?value ?valueLabel WHERE {
                VALUES ?item {
                    wd:Q81068910 wd:Q87580938 wd:Q84263196 wd:Q86597695
                    wd:Q83873577 wd:Q84055514 wd:Q87461585 wd:Q87491759
                }
                VALUES ?p {
                    wdt:P31 wdt:P279 wdt:P361 wdt:P527 wdt:P17 wdt:P571
                    wdt:P580 wdt:P582 wdt:P828 wdt:P1542 wdt:P921 wdt:P856
                }
                ?item ?p ?value .
                OPTIONAL { ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "en") }
                OPTIONAL { ?value rdfs:label ?valueLabel . FILTER(LANG(?valueLabel) = "en") }
            }
            """
        ),
    ]

    for name, query in domain_queries:
        print(f"  📦 {name}...")
        results = run_sparql(query)
        time.sleep(3)
        count = add_results_to_graph(results, g)
        total += count
        print(f"     +{count} triples (from {len(results)} results)")

    return total


def expand_discovered_entities(g: Graph) -> int:
    """
    Expand from Wikidata entities discovered in previous rounds.
    Find wd: entities in the graph that don't have many triples yet.
    """
    total = 0

    # Find Wikidata entities that are objects in our graph but have few triples as subjects
    wd_entities = set()
    for s, p, o in g:
        if isinstance(o, URIRef) and str(o).startswith("http://www.wikidata.org/entity/Q"):
            qid = str(o).split("/")[-1]
            wd_entities.add(qid)

    # Filter to entities that have < 3 triples as subjects
    sparse_entities = []
    for qid in wd_entities:
        uri = WD[qid]
        triple_count = len(list(g.triples((uri, None, None))))
        if triple_count < 3:
            sparse_entities.append(qid)

    print(f"  Found {len(sparse_entities)} sparse Wikidata entities to expand")

    # Expand in batches
    for i in range(0, min(len(sparse_entities), 200), 15):
        batch = sparse_entities[i:i+15]
        values = " ".join(f"wd:{q}" for q in batch)

        query = f"""
        SELECT ?entity ?entityLabel ?p ?o ?oLabel WHERE {{
            VALUES ?entity {{ {values} }}
            ?entity ?p ?o .
            FILTER(STRSTARTS(STR(?p), "http://www.wikidata.org/prop/direct/"))
            OPTIONAL {{ ?entity rdfs:label ?entityLabel . FILTER(LANG(?entityLabel) = "en") }}
            OPTIONAL {{ ?o rdfs:label ?oLabel . FILTER(LANG(?oLabel) = "en") }}
        }}
        """

        results = run_sparql(query)
        time.sleep(2)

        for row in results:
            entity_uri = URIRef(row["entity"]["value"])
            prop_uri = URIRef(row["p"]["value"])
            o_raw = row["o"]

            if o_raw["type"] == "uri":
                obj = URIRef(o_raw["value"])
            else:
                obj = Literal(o_raw["value"])

            g.add((entity_uri, prop_uri, obj))
            total += 1

            el = row.get("entityLabel", {}).get("value", "")
            ol = row.get("oLabel", {}).get("value", "")
            if el:
                g.add((entity_uri, RDFS.label, Literal(el, lang="en")))
                total += 1
            if ol and o_raw["type"] == "uri":
                g.add((URIRef(o_raw["value"]), RDFS.label, Literal(ol, lang="en")))
                total += 1

        batch_num = i // 15 + 1
        print(f"    Batch {batch_num}: running total {total}")

    return total


def main():
    print("=" * 60)
    print("  PHASE 4c: DEEP KB EXPANSION")
    print("=" * 60)

    # Load expanded KG from phase 4b
    g = Graph()
    g.parse(EXPANDED_KG_FILE, format="turtle")
    g.bind("edai", EDAI)
    g.bind("wd", WD)
    g.bind("wdt", WDT)
    g.bind("schema", SCHEMA)

    base_count = len(g)
    print(f"Loaded expanded KG: {base_count} triples")

    # Load mappings
    with open(MAPPING_FILE, "r", encoding="utf-8") as f:
        mappings = json.load(f)

    # Round 1: Pull ALL properties for linked entities
    print(f"\n{'=' * 50}")
    print("  ROUND 1: ALL PROPERTIES FOR LINKED ENTITIES")
    print(f"{'=' * 50}")
    r1 = expand_all_properties(mappings, g)
    print(f"  Round 1 total: +{r1}")

    # Round 2: Domain subgraphs
    print(f"\n{'=' * 50}")
    print("  ROUND 2: DOMAIN SUBGRAPHS")
    print(f"{'=' * 50}")
    r2 = expand_domain_subgraphs(g)
    print(f"  Round 2 total: +{r2}")

    # Round 3: Expand discovered entities
    print(f"\n{'=' * 50}")
    print("  ROUND 3: EXPAND DISCOVERED ENTITIES")
    print(f"{'=' * 50}")
    r3 = expand_discovered_entities(g)
    print(f"  Round 3 total: +{r3}")

    # Save final KG
    print(f"\nSaving final knowledge graph...")
    g.serialize(destination=FINAL_KG_FILE, format="turtle")

    # Also save as N-Triples for KGE (Day 5)
    nt_file = FINAL_KG_FILE.replace(".ttl", ".nt")
    g.serialize(destination=nt_file, format="nt")

    final_count = len(g)
    new_triples = final_count - base_count

    stats = f"""
{'=' * 60}
  FINAL EXPANSION SUMMARY
{'=' * 60}
  Previous KG size:       {base_count}
  Round 1 (all props):    +{r1}
  Round 2 (domain):       +{r2}
  Round 3 (discovered):   +{r3}
  ─────────────────────────────────
  Total NEW triples:      {new_triples}
  Final KG size:          {final_count} triples
  
  Target range:           50,000 – 200,000
  Status:                 {"✅ IN RANGE" if 50000 <= final_count <= 200000 else "⚠️  " + ("BELOW" if final_count < 50000 else "ABOVE")}
  
  Output files:
    Turtle: {FINAL_KG_FILE}
    N-Triples: {nt_file}
{'=' * 60}
"""
    print(stats)

    with open(FINAL_STATS_FILE, "w", encoding="utf-8") as f:
        f.write(stats)


if __name__ == "__main__":
    main()