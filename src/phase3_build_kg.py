"""
Phase 3: Build RDF Knowledge Graph from NER output
Domain: Education and AI
Converts entities_relations.jsonl → knowledge_graph.ttl
"""

import json
import os
import re
from collections import Counter

from rdflib import Graph, Literal, Namespace, RDF, RDFS, OWL, URIRef, XSD


# ── Configuration ──────────────────────────────────────────────
INPUT_FILE = "data/processed/entities_relations.jsonl"
ONTOLOGY_FILE = "kg_artifacts/ontology.ttl"
OUTPUT_FILE = "kg_artifacts/knowledge_graph.ttl"
STATS_FILE = "kg_artifacts/kb_statistics.txt"

# Namespace
EDAI = Namespace("http://example.org/edai/")


# ── URI helpers ────────────────────────────────────────────────
def clean_uri_name(text: str) -> str:
    """
    Convert entity text to a valid URI-safe string.
    e.g., 'Emily E. N. Miller' → 'EmilyENMiller'
    """
    # Remove content in parentheses
    text = re.sub(r"\(.*?\)", "", text)
    # Remove special characters, keep letters, digits, spaces
    text = re.sub(r"[^a-zA-Z0-9\s]", "", text)
    # Title case and remove spaces
    parts = text.strip().split()
    uri = "".join(word.capitalize() for word in parts if word)
    # Ensure it starts with a letter
    if uri and not uri[0].isalpha():
        uri = "E_" + uri
    return uri if uri else "Unknown"


def make_entity_uri(text: str, label: str) -> URIRef:
    """Create a URI for an entity based on its text and NER label."""
    clean = clean_uri_name(text)
    # Add a prefix based on the label to avoid collisions
    # (e.g., a person and org could have the same name)
    prefix_map = {
        "PERSON": "",
        "ORG": "Org_",
        "GPE": "Loc_",
        "DATE": "Date_",
        "WORK_OF_ART": "Pub_",
        "EVENT": "Event_",
        "FAC": "Fac_",
    }
    prefix = prefix_map.get(label, "")
    return EDAI[prefix + clean]


# ── NER label → RDF class mapping ─────────────────────────────
LABEL_TO_CLASS = {
    "PERSON": EDAI.Person,
    "ORG": EDAI.Organization,
    "GPE": EDAI.Location,
    "DATE": None,  # Dates are stored as literals, not class instances
    "WORK_OF_ART": EDAI.Publication,
    "EVENT": EDAI.Event,
    "FAC": EDAI.Organization,  # Facilities treated as organizations
}

# Known universities (for subclass assignment)
KNOWN_UNIVERSITIES = {
    "stanford", "mit", "purdue", "harvard", "cambridge", "oxford",
    "university", "college", "école", "cnam", "peking",
    "michigan state", "georgia state", "kansas state",
}

# Known journals
KNOWN_JOURNALS = {
    "review of educational research", "aera open", "educational researcher",
    "journal", "proceedings", "ieee", "lancet", "nature",
    "american educational research journal",
}

# Known tech companies
KNOWN_COMPANIES = {
    "google", "facebook", "microsoft", "apple", "amazon", "netflix",
    "youtube", "zoom", "skype", "coursera", "duolingo", "chatgpt",
    "code.org", "ibm",
}


def get_subclass(text: str, label: str) -> URIRef | None:
    """Determine if an ORG entity should be a University, Journal, or Company."""
    if label != "ORG":
        return None
    text_lower = text.lower()
    for kw in KNOWN_UNIVERSITIES:
        if kw in text_lower:
            return EDAI.University
    for kw in KNOWN_JOURNALS:
        if kw in text_lower:
            return EDAI.Journal
    for kw in KNOWN_COMPANIES:
        if kw in text_lower:
            return EDAI.Company
    return None


# ── Filters ────────────────────────────────────────────────────
def should_skip_entity(text: str, label: str) -> bool:
    """Skip entities that are too noisy to include in the KG."""
    text_clean = text.strip()
    
    # Skip very short entities
    if len(text_clean) < 2:
        return True
    
    # Skip DATE entities (we'll use them as literals, not nodes)
    if label == "DATE":
        return True
    
    # Skip entities that are just abbreviations of et al.
    if text_clean.lower() in ("et al.", "et al", "et"):
        return True
    
    # Skip single initials like "R.", "N.", "S."
    if re.match(r"^[A-Z]\.$", text_clean):
        return True
    
    return False


def should_skip_relation(source: str, target: str, source_label: str, target_label: str) -> bool:
    """Skip relations that are too noisy."""
    # Skip if either entity would be skipped
    if should_skip_entity(source, source_label) or should_skip_entity(target, target_label):
        return True
    # Skip DATE-DATE relations
    if source_label == "DATE" and target_label == "DATE":
        return True
    return False


# ── Main graph builder ─────────────────────────────────────────
def build_knowledge_graph():
    """Build the RDF knowledge graph from NER output."""
    
    # Create the graph and bind prefixes
    g = Graph()
    g.bind("edai", EDAI)
    g.bind("rdf", RDF)
    g.bind("rdfs", RDFS)
    g.bind("owl", OWL)
    g.bind("xsd", XSD)
    
    # Load ontology
    if os.path.exists(ONTOLOGY_FILE):
        g.parse(ONTOLOGY_FILE, format="turtle")
        print(f"Loaded ontology from {ONTOLOGY_FILE}")
    
    # Load NER data
    documents = []
    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        for line in f:
            documents.append(json.loads(line))
    print(f"Loaded {len(documents)} documents from {INPUT_FILE}")
    
    # Track statistics
    entity_count = 0
    relation_count = 0
    entity_types = Counter()
    seen_entities = set()
    
    # ── Step 1: Add entities as nodes ──────────────────────────
    print("\nStep 1: Adding entities...")
    for doc in documents:
        source_url = doc["url"]
        
        for ent in doc["entities"]:
            text = ent["text"]
            label = ent["label"]
            
            if should_skip_entity(text, label):
                continue
            
            uri = make_entity_uri(text, label)
            rdf_class = LABEL_TO_CLASS.get(label)
            
            if rdf_class is None:
                continue
            
            # Avoid duplicate triples
            if uri not in seen_entities:
                seen_entities.add(uri)
                
                # Add type triple
                g.add((uri, RDF.type, rdf_class))
                
                # Add subclass if applicable
                subclass = get_subclass(text, label)
                if subclass:
                    g.add((uri, RDF.type, subclass))
                
                # Add label
                clean_text = " ".join(text.split())
                g.add((uri, RDFS.label, Literal(clean_text, lang="en")))
                
                # Add NER label as metadata
                g.add((uri, EDAI.entityLabel, Literal(label)))
                
                entity_count += 1
                entity_types[label] += 1
            
            # Add source URL
            g.add((uri, EDAI.sourceURL, Literal(source_url, datatype=XSD.anyURI)))
    
    print(f"  Added {entity_count} unique entities")
    
    # ── Step 2: Add co-occurrence relations ────────────────────
    print("\nStep 2: Adding relations...")
    for doc in documents:
        for rel in doc["relations"]:
            source = rel["source"]
            target = rel["target"]
            source_label = rel["source_label"]
            target_label = rel["target_label"]
            
            if should_skip_relation(source, target, source_label, target_label):
                continue
            
            source_uri = make_entity_uri(source, source_label)
            target_uri = make_entity_uri(target, target_label)
            
            # Determine the predicate based on entity type pairs
            if source_label == "PERSON" and target_label == "PERSON":
                predicate = EDAI.coAuthorWith
            elif source_label == "PERSON" and target_label == "ORG":
                predicate = EDAI.affiliatedWith
            elif source_label == "ORG" and target_label == "PERSON":
                # Reverse: org mentioned with person
                predicate = EDAI.relatedTo
            elif source_label == "PERSON" and target_label == "GPE":
                predicate = EDAI.relatedTo
            elif source_label == "ORG" and target_label == "GPE":
                predicate = EDAI.locatedIn
            elif source_label == "GPE" and target_label == "GPE":
                predicate = EDAI.relatedTo
            elif source_label == "WORK_OF_ART" and target_label == "PERSON":
                predicate = EDAI.authorOf
                # Swap: person is the author
                source_uri, target_uri = target_uri, source_uri
            elif source_label == "PERSON" and target_label == "WORK_OF_ART":
                predicate = EDAI.authorOf
            elif source_label in ("WORK_OF_ART",) and target_label == "ORG":
                predicate = EDAI.publishedIn
            elif source_label == "ORG" and target_label == "ORG":
                predicate = EDAI.relatedTo
            else:
                predicate = EDAI.relatedTo
            
            g.add((source_uri, predicate, target_uri))
            relation_count += 1
    
    print(f"  Added {relation_count} relation triples")
    
    # ── Step 3: Add verb-based triples ─────────────────────────
    print("\nStep 3: Adding verb-based triples...")
    verb_count = 0
    for doc in documents:
        for vr in doc.get("verb_relations", []):
            subj = vr["subject"]
            rel = vr["relation"]
            obj = vr["object"]
            
            # Create URIs for verb relations (using generic entities)
            subj_uri = EDAI[clean_uri_name(subj)]
            obj_uri = EDAI[clean_uri_name(obj)]
            rel_uri = EDAI[clean_uri_name(rel)]
            
            g.add((subj_uri, rel_uri, obj_uri))
            verb_count += 1
    
    print(f"  Added {verb_count} verb-based triples")
    
    # ── Save output ────────────────────────────────────────────
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    g.serialize(destination=OUTPUT_FILE, format="turtle")
    print(f"\nKnowledge graph saved to {OUTPUT_FILE}")
    
    # ── Statistics ─────────────────────────────────────────────
    total_triples = len(g)
    unique_subjects = len(set(g.subjects()))
    unique_predicates = len(set(g.predicates()))
    unique_objects = len(set(g.objects()))
    
    stats = f"""
{'=' * 60}
  KNOWLEDGE BASE STATISTICS
{'=' * 60}
  Total triples:          {total_triples}
  Unique entities (nodes): {entity_count}
  Unique subjects:         {unique_subjects}
  Unique predicates:       {unique_predicates}
  Unique objects:          {unique_objects}
  
  Entity breakdown:
"""
    for label, count in entity_types.most_common():
        stats += f"    {label:20s} {count}\n"
    
    stats += f"""
  Relation triples:        {relation_count}
  Verb-based triples:      {verb_count}
  
  Source documents:         {len(documents)}
{'=' * 60}
"""
    
    print(stats)
    
    # Save statistics
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        f.write(stats)
    print(f"Statistics saved to {STATS_FILE}")
    
    return g


# ── SPARQL sanity checks ──────────────────────────────────────
def run_sanity_checks(g: Graph):
    """Run basic SPARQL queries to verify the graph makes sense."""
    print(f"\n{'=' * 60}")
    print("  SPARQL SANITY CHECKS")
    print(f"{'=' * 60}")
    
    # Query 1: Count entities by type
    q1 = """
    SELECT ?type (COUNT(?s) AS ?count)
    WHERE { ?s rdf:type ?type . }
    GROUP BY ?type
    ORDER BY DESC(?count)
    """
    print("\n1. Entity counts by class:")
    for row in g.query(q1):
        print(f"   {str(row[0]):50s} → {row[1]}")
    
    # Query 2: Sample persons
    q2 = """
    SELECT ?person ?label WHERE {
        ?person rdf:type <http://example.org/edai/Person> .
        ?person rdfs:label ?label .
    } LIMIT 10
    """
    print("\n2. Sample Person entities:")
    for row in g.query(q2):
        print(f"   {row.label}")
    
    # Query 3: Sample organizations
    q3 = """
    SELECT ?org ?label WHERE {
        ?org rdf:type <http://example.org/edai/Organization> .
        ?org rdfs:label ?label .
    } LIMIT 10
    """
    print("\n3. Sample Organization entities:")
    for row in g.query(q3):
        print(f"   {row.label}")
    
    # Query 4: Sample co-author relations
    q4 = """
    SELECT ?p1label ?p2label WHERE {
        ?p1 <http://example.org/edai/coAuthorWith> ?p2 .
        ?p1 rdfs:label ?p1label .
        ?p2 rdfs:label ?p2label .
    } LIMIT 10
    """
    print("\n4. Sample co-author relations:")
    for row in g.query(q4):
        print(f"   {row.p1label}  ←co-author→  {row.p2label}")
    
    # Query 5: Sample affiliations
    q5 = """
    SELECT ?plabel ?olabel WHERE {
        ?p <http://example.org/edai/affiliatedWith> ?o .
        ?p rdfs:label ?plabel .
        ?o rdfs:label ?olabel .
    } LIMIT 10
    """
    print("\n5. Sample affiliations:")
    for row in g.query(q5):
        print(f"   {row.plabel}  →affiliatedWith→  {row.olabel}")


def main():
    print("=" * 60)
    print("  EDUCATION & AI — PHASE 3: RDF KNOWLEDGE GRAPH")
    print("=" * 60)
    
    g = build_knowledge_graph()
    run_sanity_checks(g)


if __name__ == "__main__":
    main()