"""
Phase 3b: Knowledge Graph Quality Check
Validates the generated RDF graph and reports issues.
"""

from rdflib import Graph, Namespace, RDF, RDFS

EDAI = Namespace("http://example.org/edai/")
KG_FILE = "kg_artifacts/knowledge_graph.ttl"


def check_quality():
    g = Graph()
    g.parse(KG_FILE, format="turtle")

    print("=" * 60)
    print("  KNOWLEDGE GRAPH QUALITY REPORT")
    print("=" * 60)

    # 1. Basic counts
    total = len(g)
    subjects = set(g.subjects())
    predicates = set(g.predicates())
    objects = set(g.objects())
    print(f"\n1. BASIC COUNTS")
    print(f"   Total triples:     {total}")
    print(f"   Unique subjects:   {len(subjects)}")
    print(f"   Unique predicates: {len(predicates)}")
    print(f"   Unique objects:    {len(objects)}")

    # 2. Class instance counts
    print(f"\n2. ENTITIES BY CLASS")
    class_query = """
    SELECT ?type (COUNT(DISTINCT ?s) AS ?cnt)
    WHERE {
        ?s rdf:type ?type .
        FILTER(STRSTARTS(STR(?type), "http://example.org/edai/"))
    }
    GROUP BY ?type ORDER BY DESC(?cnt)
    """
    for row in g.query(class_query):
        class_name = str(row[0]).replace("http://example.org/edai/", "edai:")
        print(f"   {class_name:30s} {row[1]}")

    # 3. Predicate usage
    print(f"\n3. TOP PREDICATES (domain-specific)")
    pred_query = """
    SELECT ?p (COUNT(*) AS ?cnt)
    WHERE {
        ?s ?p ?o .
        FILTER(STRSTARTS(STR(?p), "http://example.org/edai/"))
    }
    GROUP BY ?p ORDER BY DESC(?cnt) LIMIT 15
    """
    for row in g.query(pred_query):
        pred_name = str(row[0]).replace("http://example.org/edai/", "edai:")
        print(f"   {pred_name:35s} {row[1]}")

    # 4. Check for entities without labels
    no_label_query = """
    SELECT (COUNT(?s) AS ?cnt)
    WHERE {
        ?s rdf:type ?type .
        FILTER(STRSTARTS(STR(?type), "http://example.org/edai/"))
        FILTER NOT EXISTS { ?s rdfs:label ?label }
    }
    """
    result = list(g.query(no_label_query))
    no_labels = int(result[0][0])
    print(f"\n4. DATA QUALITY CHECKS")
    print(f"   Entities without rdfs:label:  {no_labels}", "⚠️" if no_labels > 0 else "✅")

    # 5. Check for isolated nodes (no relations beyond type/label)
    isolated_query = """
    SELECT (COUNT(DISTINCT ?s) AS ?cnt)
    WHERE {
        ?s rdf:type ?type .
        FILTER(STRSTARTS(STR(?type), "http://example.org/edai/"))
        FILTER NOT EXISTS {
            { ?s ?p ?o . FILTER(?p NOT IN (rdf:type, rdfs:label, <http://example.org/edai/entityLabel>, <http://example.org/edai/sourceURL>)) }
            UNION
            { ?o2 ?p2 ?s . }
        }
    }
    """
    result = list(g.query(isolated_query))
    isolated = int(result[0][0])
    total_entities_q = """
    SELECT (COUNT(DISTINCT ?s) AS ?cnt)
    WHERE {
        ?s rdf:type ?type .
        FILTER(STRSTARTS(STR(?type), "http://example.org/edai/"))
    }
    """
    total_ent = int(list(g.query(total_entities_q))[0][0])
    connected = total_ent - isolated
    print(f"   Total typed entities:         {total_ent}")
    print(f"   Connected entities:           {connected} ({100*connected//total_ent}%)")
    print(f"   Isolated entities:            {isolated} ({100*isolated//total_ent}%)")

    # 6. Sample triples
    print(f"\n5. SAMPLE TRIPLES (first 10 domain triples)")
    sample_q = """
    SELECT ?s ?p ?o WHERE {
        ?s ?p ?o .
        FILTER(STRSTARTS(STR(?s), "http://example.org/edai/"))
        FILTER(STRSTARTS(STR(?p), "http://example.org/edai/"))
        FILTER(ISURI(?o))
        FILTER(STRSTARTS(STR(?o), "http://example.org/edai/"))
    } LIMIT 10
    """
    for row in g.query(sample_q):
        s = str(row[0]).replace("http://example.org/edai/", ":")
        p = str(row[1]).replace("http://example.org/edai/", ":")
        o = str(row[2]).replace("http://example.org/edai/", ":")
        print(f"   {s:35s} → {p:20s} → {o}")

    # 7. Co-author network stats
    coauthor_q = """
    SELECT (COUNT(*) AS ?cnt) WHERE {
        ?p1 <http://example.org/edai/coAuthorWith> ?p2 .
    }
    """
    coauth = int(list(g.query(coauthor_q))[0][0])

    affil_q = """
    SELECT (COUNT(*) AS ?cnt) WHERE {
        ?p <http://example.org/edai/affiliatedWith> ?o .
    }
    """
    affil = int(list(g.query(affil_q))[0][0])

    print(f"\n6. RELATIONSHIP SUMMARY")
    print(f"   Co-author relations:          {coauth}")
    print(f"   Affiliation relations:        {affil}")

    print(f"\n{'=' * 60}")
    print(f"  VERDICT: {'GOOD' if total > 5000 and connected > total_ent * 0.3 else 'NEEDS WORK'}")
    print(f"  Initial KB is ready for entity linking & expansion (Day 3)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    check_quality()