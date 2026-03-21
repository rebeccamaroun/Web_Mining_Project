"""
Phase 4b: KB Expansion via Wikidata SPARQL
Uses the entity mappings from phase4a to pull additional triples
from Wikidata, reaching the 50k-200k target.

Also creates predicate alignment triples.
"""

import json
import os
import time
import requests
from rdflib import Graph, Namespace, Literal, URIRef, RDF, RDFS, OWL, XSD

# ── Configuration ──────────────────────────────────────────────
MAPPING_FILE = "kg_artifacts/entity_mapping.json"
BASE_KG_FILE = "kg_artifacts/knowledge_graph.ttl"
ALIGNMENT_FILE = "kg_artifacts/alignment.ttl"
EXPANDED_KG_FILE = "kg_artifacts/knowledge_graph_expanded.ttl"
PRED_ALIGNMENT_FILE = "kg_artifacts/predicate_alignment.ttl"
EXPANSION_STATS_FILE = "kg_artifacts/expansion_statistics.txt"

EDAI = Namespace("http://example.org/edai/")
WD = Namespace("http://www.wikidata.org/entity/")
WDT = Namespace("http://www.wikidata.org/prop/direct/")
WIKIBASE = Namespace("http://wikiba.se/ontology#")
SCHEMA = Namespace("http://schema.org/")

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
HEADERS = {
    "User-Agent": "WebMiningProject/1.0 (educational project; contact: student@cnam.fr)",
    "Accept": "application/json",
}

# Properties we want to pull from Wikidata (education & AI relevant)
EXPANSION_PROPERTIES = {
    "P31": "instance of",
    "P279": "subclass of",
    "P17": "country",
    "P131": "located in",
    "P159": "headquarters location",
    "P101": "field of work",
    "P106": "occupation",
    "P108": "employer",
    "P69": "educated at",
    "P27": "country of citizenship",
    "P361": "part of",
    "P527": "has part",
    "P571": "inception date",
    "P856": "official website",
    "P154": "logo image",
    "P18": "image",
    "P1566": "GeoNames ID",
    "P625": "coordinate location",
    "P36": "capital",
    "P30": "continent",
    "P37": "official language",
    "P38": "currency",
    "P1082": "population",
    "P2044": "elevation above sea level",
    "P6": "head of government",
    "P112": "founded by",
    "P169": "chief executive officer",
    "P452": "industry",
    "P1128": "employees",
    "P749": "parent organization",
    "P355": "subsidiary",
    "P910": "topic's main category",
    "P1343": "described by source",
}

# 2-hop properties: from linked entity → intermediate → new entity
TWO_HOP_PROPERTIES = {
    "P131": ["P17", "P30"],       # located in → country → continent
    "P108": ["P17", "P159"],      # employer → country, headquarters
    "P69": ["P17", "P159"],       # educated at → country, headquarters
    "P749": ["P17", "P159"],      # parent org → country, headquarters
}


def run_sparql(query: str, retries: int = 3) -> list:
    """Run a SPARQL query against the Wikidata endpoint."""
    for attempt in range(retries):
        try:
            resp = requests.get(
                WIKIDATA_SPARQL,
                params={"query": query, "format": "json"},
                headers=HEADERS,
                timeout=60,
            )
            if resp.status_code == 429:
                wait = 30 * (attempt + 1)
                print(f"    ⚠️  Rate limited, waiting {wait}s...")
                time.sleep(wait)
                continue
            resp.raise_for_status()
            return resp.json().get("results", {}).get("bindings", [])
        except Exception as e:
            if attempt < retries - 1:
                print(f"    ⚠️  Query error (attempt {attempt+1}): {e}")
                time.sleep(5 * (attempt + 1))
            else:
                print(f"    ❌  Query failed after {retries} attempts: {e}")
                return []
    return []


def create_predicate_alignment() -> Graph:
    """
    Step 2: Create predicate alignment between our ontology and Wikidata.
    Maps our predicates to Wikidata properties via owl:equivalentProperty.
    """
    g = Graph()
    g.bind("edai", EDAI)
    g.bind("wdt", WDT)
    g.bind("owl", OWL)

    alignments = {
        EDAI.affiliatedWith: WDT.P108,    # employer
        EDAI.locatedIn: WDT.P131,          # located in admin. territory
        EDAI.authorOf: WDT.P50,            # author (inverse)
        EDAI.publishedIn: WDT.P1433,       # published in
        EDAI.coAuthorWith: WDT.P50,        # both are authors
    }

    for local_pred, wd_pred in alignments.items():
        g.add((local_pred, OWL.equivalentProperty, wd_pred))

    print(f"  Created {len(alignments)} predicate alignments")
    return g


def expand_1hop(mappings: list, g: Graph) -> int:
    """
    Step 3a: 1-hop expansion — for each linked entity, pull
    direct properties from Wikidata.
    """
    total_triples = 0
    batch_size = 20  # Query multiple entities at once

    # Group QIDs into batches
    qids = [(m["wikidata_qid"], m["local_uri"], m["ner_type"]) for m in mappings]

    for i in range(0, len(qids), batch_size):
        batch = qids[i:i + batch_size]
        values = " ".join(f"wd:{qid}" for qid, _, _ in batch)
        props = " ".join(f"wdt:{p}" for p in EXPANSION_PROPERTIES.keys())

        query = f"""
        SELECT ?entity ?prop ?value ?valueLabel WHERE {{
            VALUES ?entity {{ {values} }}
            VALUES ?prop {{ {props} }}
            ?entity ?prop ?value .
            OPTIONAL {{
                ?value rdfs:label ?valueLabel .
                FILTER(LANG(?valueLabel) = "en")
            }}
        }}
        """

        results = run_sparql(query)
        time.sleep(1.5)  # Polite delay between batch queries

        if not results:
            continue

        # Build a QID → local URI lookup
        qid_to_local = {qid: uri for qid, uri, _ in batch}

        for row in results:
            entity_qid = row["entity"]["value"].split("/")[-1]
            prop_uri = URIRef(row["prop"]["value"])
            value = row["value"]["value"]
            value_label = row.get("valueLabel", {}).get("value", "")

            # Find the local URI
            local_uri = qid_to_local.get(entity_qid)
            if not local_uri:
                continue
            local_uri = URIRef(local_uri)

            # Add the triple
            if value.startswith("http://www.wikidata.org/entity/"):
                obj = URIRef(value)
                g.add((local_uri, prop_uri, obj))
                # Also add the label for the object
                if value_label:
                    g.add((obj, RDFS.label, Literal(value_label, lang="en")))
            else:
                # Literal value
                g.add((local_uri, prop_uri, Literal(value)))

            total_triples += 1

        batch_num = i // batch_size + 1
        total_batches = (len(qids) + batch_size - 1) // batch_size
        print(f"    Batch {batch_num}/{total_batches}: +{len(results)} triples (running total: {total_triples})")

    return total_triples


def expand_2hop(mappings: list, g: Graph) -> int:
    """
    Step 3b: 2-hop expansion — follow linked entities through
    intermediate nodes to discover more facts.
    """
    total_triples = 0

    # Focus on ORGs and GPEs (they have the richest 2-hop connections)
    org_gpe = [m for m in mappings if m["ner_type"] in ("ORG", "GPE")]
    qids = [m["wikidata_qid"] for m in org_gpe[:40]]

    if not qids:
        return 0

    values = " ".join(f"wd:{q}" for q in qids)

    for first_prop, second_props in TWO_HOP_PROPERTIES.items():
        for second_prop in second_props:
            query = f"""
            SELECT ?entity ?mid ?midLabel ?end ?endLabel WHERE {{
                VALUES ?entity {{ {values} }}
                ?entity wdt:{first_prop} ?mid .
                ?mid wdt:{second_prop} ?end .
                OPTIONAL {{ ?mid rdfs:label ?midLabel . FILTER(LANG(?midLabel) = "en") }}
                OPTIONAL {{ ?end rdfs:label ?endLabel . FILTER(LANG(?endLabel) = "en") }}
            }}
            LIMIT 500
            """

            results = run_sparql(query)
            time.sleep(2)

            for row in results:
                mid_uri = URIRef(row["mid"]["value"])
                end_uri = URIRef(row["end"]["value"])
                mid_label = row.get("midLabel", {}).get("value", "")
                end_label = row.get("endLabel", {}).get("value", "")

                # Add intermediate and end triples
                entity_uri = URIRef(row["entity"]["value"])
                g.add((entity_uri, WDT[first_prop], mid_uri))
                g.add((mid_uri, WDT[second_prop], end_uri))

                if mid_label:
                    g.add((mid_uri, RDFS.label, Literal(mid_label, lang="en")))
                if end_label:
                    g.add((end_uri, RDFS.label, Literal(end_label, lang="en")))

                total_triples += 2

            if results:
                print(f"    2-hop {first_prop}→{second_prop}: +{len(results)*2} triples")

    return total_triples


def expand_topic_triples(mappings: list, g: Graph) -> int:
    """
    Step 3c: Topic expansion — pull entities related to education and AI
    topics from Wikidata to enrich the domain context.
    """
    total_triples = 0

    # Education and AI topic QIDs
    topic_queries = [
        # AI and machine learning concepts
        """
        SELECT ?item ?itemLabel ?prop ?value ?valueLabel WHERE {
            VALUES ?item {
                wd:Q11660   wd:Q2539    wd:Q234953  wd:Q868148
                wd:Q4830453 wd:Q846662  wd:Q208456  wd:Q816264
            }
            VALUES ?prop { wdt:P31 wdt:P279 wdt:P361 wdt:P527 wdt:P1343 wdt:P101 }
            ?item ?prop ?value .
            OPTIONAL { ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "en") }
            OPTIONAL { ?value rdfs:label ?valueLabel . FILTER(LANG(?valueLabel) = "en") }
        }
        """,
        # Education-related entities
        """
        SELECT ?item ?itemLabel ?prop ?value ?valueLabel WHERE {
            VALUES ?item {
                wd:Q8434    wd:Q3966    wd:Q333092  wd:Q182250
                wd:Q1075990 wd:Q668937  wd:Q1397685 wd:Q747265
            }
            VALUES ?prop { wdt:P31 wdt:P279 wdt:P361 wdt:P527 wdt:P1343 wdt:P101 }
            ?item ?prop ?value .
            OPTIONAL { ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "en") }
            OPTIONAL { ?value rdfs:label ?valueLabel . FILTER(LANG(?valueLabel) = "en") }
        }
        """,
        # Online learning and ed-tech
        """
        SELECT ?item ?itemLabel ?prop ?value ?valueLabel WHERE {
            VALUES ?item {
                wd:Q200790  wd:Q609295  wd:Q1322466 wd:Q746083
                wd:Q1049956 wd:Q492264  wd:Q477674  wd:Q211755
            }
            VALUES ?prop { wdt:P31 wdt:P279 wdt:P361 wdt:P527 wdt:P101 wdt:P856 }
            ?item ?prop ?value .
            OPTIONAL { ?item rdfs:label ?itemLabel . FILTER(LANG(?itemLabel) = "en") }
            OPTIONAL { ?value rdfs:label ?valueLabel . FILTER(LANG(?valueLabel) = "en") }
        }
        """,
    ]

    for idx, query in enumerate(topic_queries):
        results = run_sparql(query)
        time.sleep(2)

        for row in results:
            item_uri = URIRef(row["item"]["value"])
            prop_uri = URIRef(row["prop"]["value"])
            value_uri = URIRef(row["value"]["value"]) if row["value"]["value"].startswith("http") else None

            item_label = row.get("itemLabel", {}).get("value", "")
            value_label = row.get("valueLabel", {}).get("value", "")

            if value_uri:
                g.add((item_uri, prop_uri, value_uri))
                if value_label:
                    g.add((value_uri, RDFS.label, Literal(value_label, lang="en")))
            else:
                g.add((item_uri, prop_uri, Literal(row["value"]["value"])))

            if item_label:
                g.add((item_uri, RDFS.label, Literal(item_label, lang="en")))

            total_triples += 1

        print(f"    Topic batch {idx+1}/3: +{len(results)} triples")

    return total_triples


def expand_entity_descriptions(mappings: list, g: Graph) -> int:
    """
    Step 3d: Pull descriptions and aliases for linked entities.
    """
    total_triples = 0
    qids = [m["wikidata_qid"] for m in mappings]

    for i in range(0, len(qids), 25):
        batch = qids[i:i+25]
        values = " ".join(f"wd:{q}" for q in batch)

        query = f"""
        SELECT ?entity ?desc ?alias WHERE {{
            VALUES ?entity {{ {values} }}
            OPTIONAL {{ ?entity schema:description ?desc . FILTER(LANG(?desc) = "en") }}
            OPTIONAL {{ ?entity skos:altLabel ?alias . FILTER(LANG(?alias) = "en") }}
        }}
        """

        results = run_sparql(query)
        time.sleep(1.5)

        for row in results:
            entity_uri = URIRef(row["entity"]["value"])
            if "desc" in row and row["desc"]["value"]:
                g.add((entity_uri, SCHEMA.description, Literal(row["desc"]["value"], lang="en")))
                total_triples += 1
            if "alias" in row and row["alias"]["value"]:
                g.add((entity_uri, RDFS.label, Literal(row["alias"]["value"], lang="en")))
                total_triples += 1

        print(f"    Descriptions batch {i//25 + 1}: +{len(results)} triples")

    return total_triples


def main():
    print("=" * 60)
    print("  EDUCATION & AI — PHASE 4b: KB EXPANSION")
    print("=" * 60)

    # Load mappings
    with open(MAPPING_FILE, "r", encoding="utf-8") as f:
        mappings = json.load(f)
    print(f"\nLoaded {len(mappings)} entity mappings")

    # Load base KG + alignment
    g = Graph()
    g.parse(BASE_KG_FILE, format="turtle")
    if os.path.exists(ALIGNMENT_FILE):
        g.parse(ALIGNMENT_FILE, format="turtle")
    base_count = len(g)
    print(f"Base KG loaded: {base_count} triples")

    # Bind namespaces
    g.bind("edai", EDAI)
    g.bind("wd", WD)
    g.bind("wdt", WDT)
    g.bind("owl", OWL)
    g.bind("schema", SCHEMA)

    # Step 2: Predicate alignment
    print(f"\n{'=' * 50}")
    print("  STEP 2: PREDICATE ALIGNMENT")
    print(f"{'=' * 50}")
    pred_g = create_predicate_alignment()
    g += pred_g
    pred_g.serialize(destination=PRED_ALIGNMENT_FILE, format="turtle")
    print(f"  Saved to {PRED_ALIGNMENT_FILE}")

    # Step 3a: 1-hop expansion
    print(f"\n{'=' * 50}")
    print("  STEP 3a: 1-HOP EXPANSION")
    print(f"{'=' * 50}")
    hop1_count = expand_1hop(mappings, g)
    print(f"  Total 1-hop triples: {hop1_count}")

    # Step 3b: 2-hop expansion
    print(f"\n{'=' * 50}")
    print("  STEP 3b: 2-HOP EXPANSION")
    print(f"{'=' * 50}")
    hop2_count = expand_2hop(mappings, g)
    print(f"  Total 2-hop triples: {hop2_count}")

    # Step 3c: Topic expansion
    print(f"\n{'=' * 50}")
    print("  STEP 3c: TOPIC EXPANSION (Education & AI)")
    print(f"{'=' * 50}")
    topic_count = expand_topic_triples(mappings, g)
    print(f"  Total topic triples: {topic_count}")

    # Step 3d: Entity descriptions
    print(f"\n{'=' * 50}")
    print("  STEP 3d: ENTITY DESCRIPTIONS")
    print(f"{'=' * 50}")
    desc_count = expand_entity_descriptions(mappings, g)
    print(f"  Total description triples: {desc_count}")

    # Save expanded KG
    print(f"\nSaving expanded knowledge graph...")
    g.serialize(destination=EXPANDED_KG_FILE, format="turtle")

    # Final statistics
    final_count = len(g)
    new_triples = final_count - base_count

    stats = f"""
{'=' * 60}
  EXPANSION SUMMARY
{'=' * 60}
  Base KG triples:        {base_count}
  1-hop expansion:        +{hop1_count}
  2-hop expansion:        +{hop2_count}
  Topic expansion:        +{topic_count}
  Description expansion:  +{desc_count}
  Predicate alignments:   +{len(pred_g)}
  ─────────────────────────────────
  Total NEW triples:      {new_triples}
  Final KG size:          {final_count} triples
  
  Target range:           50,000 – 200,000
  Status:                 {"✅ IN RANGE" if 50000 <= final_count <= 200000 else "⚠️  " + ("BELOW target" if final_count < 50000 else "ABOVE target")}
  
  Output file:            {EXPANDED_KG_FILE}
{'=' * 60}
"""
    print(stats)

    with open(EXPANSION_STATS_FILE, "w", encoding="utf-8") as f:
        f.write(stats)


if __name__ == "__main__":
    main()