"""
Phase 4a: Entity Linking to Wikidata
Links entities from our knowledge graph to Wikidata items.
Produces an alignment file with owl:sameAs triples.
"""

import json
import os
import time
import requests
from rdflib import Graph, Namespace, Literal, URIRef, RDF, RDFS, OWL, XSD

# ── Configuration ──────────────────────────────────────────────
KG_FILE = "kg_artifacts/knowledge_graph.ttl"
ALIGNMENT_FILE = "kg_artifacts/alignment.ttl"
MAPPING_FILE = "kg_artifacts/entity_mapping.json"

EDAI = Namespace("http://example.org/edai/")
WD = Namespace("http://www.wikidata.org/entity/")
WDT = Namespace("http://www.wikidata.org/prop/direct/")

WIKIDATA_API = "https://www.wikidata.org/w/api.php"

# CRITICAL: Wikidata requires a descriptive User-Agent header
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "WebMiningKGProject/1.0 (CNAM student project; Python/requests)"
})

# How many entities to try linking per class
MAX_PERSONS = 50
MAX_ORGS = 40
MAX_LOCATIONS = 30


# ── Wikidata search ────────────────────────────────────────────
def search_wikidata(query: str, entity_type: str = None, limit: int = 3) -> list:
    """
    Search Wikidata for an entity.
    Returns list of {qid, label, description, score} dicts.
    """
    params = {
        "action": "wbsearchentities",
        "search": query,
        "language": "en",
        "limit": limit,
        "format": "json",
    }

    try:
        resp = SESSION.get(WIKIDATA_API, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        print(f"    ⚠️  API error for '{query}': {e}")
        return []

    results = []
    for item in data.get("search", []):
        results.append({
            "qid": item["id"],
            "label": item.get("label", ""),
            "description": item.get("description", ""),
            "score": round(1.0 - (len(results) * 0.15), 2),
        })

    return results


def is_good_match(query: str, result: dict, entity_type: str) -> bool:
    """
    Heuristic to determine if a Wikidata result is a good match.
    """
    label = result["label"].lower().strip()
    desc = result.get("description", "").lower()
    query_lower = query.lower().strip()

    # Exact or near-exact label match
    if label == query_lower or label.startswith(query_lower) or query_lower.startswith(label):
        if entity_type == "PERSON":
            bad_keywords = ["village", "city", "district", "river", "album", "film", "song", "genus"]
            if any(kw in desc for kw in bad_keywords):
                return False
            return True
        elif entity_type == "ORG":
            return True
        elif entity_type == "GPE":
            return True

    return False


# ── Extract top entities from the KG ───────────────────────────
def get_top_entities(kg: Graph) -> dict:
    """
    Extract the most important entities from the KG for linking.
    Returns dict of {uri: {text, label, degree}} sorted by degree.
    """
    degree = {}
    for s, p, o in kg:
        s_str = str(s)
        if s_str.startswith("http://example.org/edai/"):
            degree[s] = degree.get(s, 0) + 1
        if isinstance(o, URIRef) and str(o).startswith("http://example.org/edai/"):
            degree[o] = degree.get(o, 0) + 1

    entities = {}
    for uri, deg in sorted(degree.items(), key=lambda x: -x[1]):
        labels = list(kg.objects(uri, RDFS.label))
        ner_labels = list(kg.objects(uri, EDAI.entityLabel))

        if not labels or not ner_labels:
            continue

        text = str(labels[0])
        ner_label = str(ner_labels[0])

        entities[uri] = {
            "text": text,
            "label": ner_label,
            "degree": deg,
        }

    return entities


# ── Main linking pipeline ──────────────────────────────────────
def link_entities():
    """Link KG entities to Wikidata and produce alignment triples."""

    print("Loading knowledge graph...")
    kg = Graph()
    kg.parse(KG_FILE, format="turtle")
    print(f"  Loaded {len(kg)} triples")

    # Quick connectivity test
    print("\n  Testing Wikidata API connectivity...")
    test = search_wikidata("Google")
    if test:
        print(f"  ✅ API working — test search for 'Google' returned {len(test)} results")
    else:
        print("  ❌ API not reachable! Check your internet connection.")
        print("     If the problem persists, try again in a few minutes.")
        return []

    entities = get_top_entities(kg)
    print(f"  Found {len(entities)} entities with labels")

    persons = {u: e for u, e in entities.items() if e["label"] == "PERSON"}
    orgs = {u: e for u, e in entities.items() if e["label"] in ("ORG", "FAC")}
    locations = {u: e for u, e in entities.items() if e["label"] == "GPE"}

    print(f"  Persons: {len(persons)}, Orgs: {len(orgs)}, Locations: {len(locations)}")

    align_g = Graph()
    align_g.bind("edai", EDAI)
    align_g.bind("wd", WD)
    align_g.bind("owl", OWL)

    mappings = []
    stats = {"total_tried": 0, "total_linked": 0, "by_type": {}}

    for category, entity_dict, max_count in [
        ("PERSON", persons, MAX_PERSONS),
        ("ORG", orgs, MAX_ORGS),
        ("GPE", locations, MAX_LOCATIONS),
    ]:
        print(f"\n{'=' * 50}")
        print(f"  Linking {category} entities (top {max_count})")
        print(f"{'=' * 50}")

        linked = 0
        tried = 0

        for uri, ent in list(entity_dict.items())[:max_count]:
            text = ent["text"]
            tried += 1

            results = search_wikidata(text, category)
            time.sleep(0.3)

            if not results:
                print(f"  ❌ {text:40s} → no results")
                continue

            best = None
            for r in results:
                if is_good_match(text, r, category):
                    best = r
                    break

            if best is None:
                if results[0]["label"].lower().strip() == text.lower().strip():
                    best = results[0]

            if best:
                wd_uri = WD[best["qid"]]
                confidence = best["score"]

                align_g.add((uri, OWL.sameAs, wd_uri))
                align_g.add((uri, EDAI["alignmentConfidence"],
                            Literal(confidence, datatype=XSD.float)))

                mappings.append({
                    "local_uri": str(uri),
                    "local_label": text,
                    "ner_type": category,
                    "wikidata_qid": best["qid"],
                    "wikidata_label": best["label"],
                    "wikidata_desc": best.get("description", ""),
                    "confidence": confidence,
                })

                linked += 1
                print(f"  ✅ {text:40s} → {best['qid']:10s} ({best['label']}) [{confidence}]")
            else:
                desc_hint = results[0].get("description", "")[:40] if results else ""
                print(f"  ❌ {text:40s} → no good match (top: {results[0]['label']}: {desc_hint})")

        stats["by_type"][category] = {"tried": tried, "linked": linked}
        stats["total_tried"] += tried
        stats["total_linked"] += linked

    # Save alignment graph
    os.makedirs(os.path.dirname(ALIGNMENT_FILE), exist_ok=True)
    align_g.serialize(destination=ALIGNMENT_FILE, format="turtle")
    print(f"\nAlignment file saved to {ALIGNMENT_FILE}")

    # Save mappings
    with open(MAPPING_FILE, "w", encoding="utf-8") as f:
        json.dump(mappings, f, indent=2, ensure_ascii=False)
    print(f"Entity mappings saved to {MAPPING_FILE}")

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  ENTITY LINKING SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Total entities tried:   {stats['total_tried']}")
    print(f"  Total entities linked:  {stats['total_linked']}")
    print(f"  Link rate:              {100 * stats['total_linked'] // max(stats['total_tried'], 1)}%")
    for cat, s in stats["by_type"].items():
        print(f"    {cat:10s}: {s['linked']}/{s['tried']} linked")
    print(f"  Alignment triples:      {len(align_g)}")
    print(f"{'=' * 60}")

    return mappings


if __name__ == "__main__":
    print("=" * 60)
    print("  EDUCATION & AI — PHASE 4a: ENTITY LINKING")
    print("=" * 60)
    mappings = link_entities()