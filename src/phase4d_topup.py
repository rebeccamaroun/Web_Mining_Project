"""
Phase 4d: Quick top-up to push past 50k triples.
Expands a few more discovered entities from the existing graph.
"""

import time
import requests
from rdflib import Graph, Namespace, Literal, URIRef, RDFS

FINAL_KG = "kg_artifacts/knowledge_graph_final.ttl"
WD = Namespace("http://www.wikidata.org/entity/")
WDT = Namespace("http://www.wikidata.org/prop/direct/")
EDAI = Namespace("http://example.org/edai/")

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
HEADERS = {
    "User-Agent": "WebMiningProject/1.0 (educational project; contact: student@cnam.fr)",
    "Accept": "application/json",
}


def run_sparql(query):
    try:
        resp = requests.get(WIKIDATA_SPARQL, params={"query": query, "format": "json"},
                           headers=HEADERS, timeout=90)
        resp.raise_for_status()
        return resp.json().get("results", {}).get("bindings", [])
    except Exception as e:
        print(f"  ⚠️ {e}")
        return []


def main():
    print("Loading KG...")
    g = Graph()
    g.parse(FINAL_KG, format="turtle")
    g.bind("wd", WD)
    g.bind("wdt", WDT)
    g.bind("edai", EDAI)
    before = len(g)
    print(f"Current size: {before} triples")

    # Find more sparse Wikidata entities to expand
    wd_entities = set()
    for s, p, o in g:
        if isinstance(o, URIRef) and str(o).startswith("http://www.wikidata.org/entity/Q"):
            qid = str(o).split("/")[-1]
            count = len(list(g.triples((WD[qid], None, None))))
            if count < 2:
                wd_entities.add(qid)

    sparse = list(wd_entities)
    print(f"Found {len(sparse)} sparse entities to expand")

    total = 0
    for i in range(0, min(len(sparse), 150), 15):
        batch = sparse[i:i+15]
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
            obj = URIRef(o_raw["value"]) if o_raw["type"] == "uri" else Literal(o_raw["value"])
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

        print(f"  Batch {i//15+1}: +{len(results)} results, running new: {total}")

        if len(g) >= 52000:
            print("  ✅ Passed 52k, stopping early")
            break

    after = len(g)
    print(f"\nBefore: {before} → After: {after} triples (+{after - before})")
    target_met = "✅ IN RANGE" if after >= 50000 else "⚠️ BELOW"
    print(f"Status: {target_met}")

    g.serialize(destination=FINAL_KG, format="turtle")
    nt_file = FINAL_KG.replace(".ttl", ".nt")
    g.serialize(destination=nt_file, format="nt")
    print(f"Saved to {FINAL_KG} and {nt_file}")


if __name__ == "__main__":
    main()