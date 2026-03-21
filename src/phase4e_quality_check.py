"""
Phase 4e: Expanded KB Quality Check
Validates the final expanded knowledge graph.
"""

from rdflib import Graph, Namespace, RDF, RDFS, OWL

EDAI = Namespace("http://example.org/edai/")
WD = Namespace("http://www.wikidata.org/entity/")
WDT = Namespace("http://www.wikidata.org/prop/direct/")

KG_FILE = "kg_artifacts/knowledge_graph_final.ttl"


def check():
    print("Loading knowledge graph (this may take a minute)...")
    g = Graph()
    g.parse(KG_FILE, format="turtle")

    print("=" * 65)
    print("  EXPANDED KB QUALITY REPORT")
    print("=" * 65)

    # 1. Basic counts
    total = len(g)
    subjects = set(g.subjects())
    predicates = set(g.predicates())
    objects = set(g.objects())
    print(f"\n1. BASIC COUNTS")
    print(f"   Total triples:       {total}")
    print(f"   Unique subjects:     {len(subjects)}")
    print(f"   Unique predicates:   {len(predicates)}")
    print(f"   Unique objects:      {len(objects)}")

    # 2. Triple source breakdown
    edai_triples = 0
    wd_triples = 0
    mixed_triples = 0
    other_triples = 0
    for s, p, o in g:
        s_str = str(s)
        p_str = str(p)
        if "example.org/edai" in s_str and "example.org/edai" in p_str:
            edai_triples += 1
        elif "wikidata.org" in p_str:
            wd_triples += 1
        elif "example.org/edai" in s_str or "example.org/edai" in p_str:
            mixed_triples += 1
        else:
            other_triples += 1

    print(f"\n2. TRIPLE SOURCE BREAKDOWN")
    print(f"   Our domain (edai:) triples:   {edai_triples}")
    print(f"   Wikidata (wdt:) triples:      {wd_triples}")
    print(f"   Mixed (edai↔wd) triples:      {mixed_triples}")
    print(f"   Other (rdfs, owl, etc):        {other_triples}")

    # 3. Our original entities
    print(f"\n3. OUR DOMAIN ENTITIES (edai: namespace)")
    class_q = """
    SELECT ?type (COUNT(DISTINCT ?s) AS ?cnt)
    WHERE {
        ?s rdf:type ?type .
        FILTER(STRSTARTS(STR(?type), "http://example.org/edai/"))
    }
    GROUP BY ?type ORDER BY DESC(?cnt)
    """
    for row in g.query(class_q):
        name = str(row[0]).replace("http://example.org/edai/", "edai:")
        print(f"   {name:30s} {row[1]}")

    # 4. Wikidata entities added
    wd_entities = set()
    for s in g.subjects():
        if str(s).startswith("http://www.wikidata.org/entity/Q"):
            wd_entities.add(s)
    for o in g.objects():
        if hasattr(o, '__str__') and str(o).startswith("http://www.wikidata.org/entity/Q"):
            wd_entities.add(o)
    print(f"\n4. WIKIDATA ENRICHMENT")
    print(f"   Wikidata entities referenced: {len(wd_entities)}")

    # Count Wikidata entities with labels
    wd_with_labels = 0
    for wd_e in list(wd_entities)[:5000]:  # Sample for speed
        if list(g.objects(wd_e, RDFS.label)):
            wd_with_labels += 1
    sampled = min(len(wd_entities), 5000)
    print(f"   With English labels:          {wd_with_labels}/{sampled} sampled ({100*wd_with_labels//max(sampled,1)}%)")

    # 5. owl:sameAs alignment check
    sameas_q = """
    SELECT (COUNT(*) AS ?cnt) WHERE {
        ?s owl:sameAs ?o .
    }
    """
    sameas = int(list(g.query(sameas_q))[0][0])
    print(f"\n5. ALIGNMENT (owl:sameAs)")
    print(f"   owl:sameAs triples:           {sameas}")

    # Sample some alignments
    sample_q = """
    SELECT ?local ?localLabel ?wd WHERE {
        ?local owl:sameAs ?wd .
        ?local rdfs:label ?localLabel .
    } LIMIT 8
    """
    print(f"   Sample alignments:")
    for row in g.query(sample_q):
        local = str(row[0]).replace("http://example.org/edai/", ":")
        wd = str(row[2]).replace("http://www.wikidata.org/entity/", "wd:")
        print(f"     {row[1]:40s} → {wd}")

    # 6. Top Wikidata predicates used
    print(f"\n6. TOP WIKIDATA PREDICATES IN EXPANDED KB")
    wd_pred_q = """
    SELECT ?p (COUNT(*) AS ?cnt)
    WHERE {
        ?s ?p ?o .
        FILTER(STRSTARTS(STR(?p), "http://www.wikidata.org/prop/direct/"))
    }
    GROUP BY ?p ORDER BY DESC(?cnt) LIMIT 15
    """
    for row in g.query(wd_pred_q):
        pred = str(row[0]).replace("http://www.wikidata.org/prop/direct/", "wdt:")
        print(f"   {pred:15s} {row[1]}")

    # 7. Sample expanded triples (showing Wikidata enrichment)
    print(f"\n7. SAMPLE ENRICHED TRIPLES (our entity + Wikidata facts)")
    enriched_q = """
    SELECT ?localLabel ?p ?oLabel WHERE {
        ?local owl:sameAs ?wd .
        ?local rdfs:label ?localLabel .
        ?wd ?p ?o .
        ?o rdfs:label ?oLabel .
        FILTER(STRSTARTS(STR(?p), "http://www.wikidata.org/prop/direct/"))
        FILTER(LANG(?oLabel) = "en")
    } LIMIT 12
    """
    for row in g.query(enriched_q):
        pred = str(row[1]).replace("http://www.wikidata.org/prop/direct/", "wdt:")
        print(f"   {row[0]:30s} → {pred:10s} → {row[2]}")

    # 8. Domain relevance check
    print(f"\n8. DOMAIN RELEVANCE CHECK")
    # Check for education/AI related Wikidata entities
    domain_keywords = ["education", "learning", "artificial intelligence",
                       "machine learning", "university", "school", "teacher",
                       "computer science", "technology", "online"]
    domain_count = 0
    total_labels = 0
    for s in g.subjects(RDF.type, None):
        for label in g.objects(s, RDFS.label):
            total_labels += 1
            label_str = str(label).lower()
            if any(kw in label_str for kw in domain_keywords):
                domain_count += 1

    print(f"   Entities with domain keywords: {domain_count}/{total_labels} labels")
    print(f"   Domain relevance:              {100*domain_count//max(total_labels,1)}%")

    # 9. Connectivity
    print(f"\n9. GRAPH CONNECTIVITY")
    edai_entities = set()
    for s in g.subjects(RDF.type, None):
        if str(s).startswith("http://example.org/edai/"):
            edai_entities.add(s)

    connected = 0
    for e in edai_entities:
        # Check if entity has any relation beyond type/label/entityLabel/sourceURL
        has_relation = False
        for p in g.predicates(e, None):
            p_str = str(p)
            if p_str not in (str(RDF.type), str(RDFS.label),
                            str(EDAI.entityLabel), str(EDAI.sourceURL)):
                has_relation = True
                break
        if not has_relation:
            for p in g.predicates(None, e):
                has_relation = True
                break
        if has_relation:
            connected += 1

    print(f"   Our entities (edai:):          {len(edai_entities)}")
    print(f"   Connected:                     {connected} ({100*connected//max(len(edai_entities),1)}%)")
    print(f"   Isolated:                      {len(edai_entities)-connected}")

    # 10. Verdict
    print(f"\n{'=' * 65}")
    issues = []
    if total < 50000:
        issues.append("Below 50k triple target")
    if sameas < 30:
        issues.append("Few entity alignments")
    if connected < len(edai_entities) * 0.5:
        issues.append("Low connectivity")

    if not issues:
        print("  ✅ VERDICT: GOOD QUALITY")
        print("  KB meets size target, has proper alignment, and good connectivity.")
    else:
        print("  ⚠️  VERDICT: NEEDS ATTENTION")
        for issue in issues:
            print(f"     - {issue}")

    print(f"\n  Ready for: SWRL reasoning + KGE training")
    print(f"{'=' * 65}")


if __name__ == "__main__":
    check()