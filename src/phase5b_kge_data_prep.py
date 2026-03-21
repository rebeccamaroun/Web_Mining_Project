"""
Phase 5b v2: Improved KGE Data Preparation
Fixes:
1. Keeps rdf:type triples (valid KGE relations)
2. Converts important literals to entity nodes (dates, numbers)
3. Better 80/10/10 split strategy
4. Creates proper subsamples for size sensitivity
"""

import os
import random
from collections import Counter
from rdflib import Graph, Namespace, URIRef, Literal, RDF, RDFS, OWL, XSD

FINAL_KG = "kg_artifacts/knowledge_graph_final.ttl"
KGE_DIR = "kg_artifacts/kge_data"

EDAI = Namespace("http://example.org/edai/")
WD = Namespace("http://www.wikidata.org/entity/")
WDT = Namespace("http://www.wikidata.org/prop/direct/")


def clean_for_embedding(g: Graph) -> list:
    """
    Clean the KG for embedding training.
    More aggressive triple retention than v1.
    """
    print("  Step 1: Filtering triples...")

    # Predicates to SKIP entirely (metadata, not useful for KGE)
    skip_predicates = {
        str(RDFS.label),
        str(RDFS.comment),
        str(OWL.sameAs),
        str(OWL.equivalentProperty),
        str(OWL.equivalentClass),
        str(EDAI.entityLabel),
        str(EDAI.sourceURL),
        str(EDAI.alignmentConfidence),
        "http://schema.org/description",
        "http://www.w3.org/2004/02/skos/core#altLabel",
    }

    # OWL schema predicates to skip
    skip_prefixes_exact = {
        str(OWL.imports),
        str(RDFS.domain),
        str(RDFS.range),
        str(RDFS.subPropertyOf),
    }

    triples = []
    seen = set()
    stats = {
        "kept_uri": 0,
        "kept_type": 0,
        "kept_subclass": 0,
        "kept_literal_converted": 0,
        "skipped_label": 0,
        "skipped_meta": 0,
        "skipped_dup": 0,
        "skipped_selfloop": 0,
        "skipped_literal_unconv": 0,
    }

    for s, p, o in g:
        p_str = str(p)
        s_str = str(s)

        # Skip metadata predicates
        if p_str in skip_predicates or p_str in skip_prefixes_exact:
            stats["skipped_meta"] += 1
            continue

        # Skip if subject is not a URI
        if not isinstance(s, URIRef):
            continue

        # === KEEP rdf:type triples (entity → class) ===
        if p == RDF.type:
            if isinstance(o, URIRef):
                o_str = str(o)
                # Skip OWL metaclasses
                if o_str.startswith("http://www.w3.org/2002/07/owl#"):
                    stats["skipped_meta"] += 1
                    continue
                triple_key = (s_str, p_str, o_str)
                if triple_key not in seen:
                    seen.add(triple_key)
                    triples.append((s_str, "rdf:type", o_str))
                    stats["kept_type"] += 1
            continue

        # === KEEP rdfs:subClassOf (class hierarchy) ===
        if p == RDFS.subClassOf:
            if isinstance(o, URIRef):
                o_str = str(o)
                triple_key = (s_str, p_str, o_str)
                if triple_key not in seen:
                    seen.add(triple_key)
                    triples.append((s_str, "rdfs:subClassOf", o_str))
                    stats["kept_subclass"] += 1
            continue

        # === URI-URI triples: keep directly ===
        if isinstance(o, URIRef):
            o_str = str(o)
            # Skip self-loops
            if s_str == o_str:
                stats["skipped_selfloop"] += 1
                continue

            # Use short predicate name
            if "/" in p_str:
                p_short = p_str.split("/")[-1]
            else:
                p_short = p_str
            if "#" in p_short:
                p_short = p_short.split("#")[-1]

            triple_key = (s_str, p_short, o_str)
            if triple_key not in seen:
                seen.add(triple_key)
                triples.append((s_str, p_short, o_str))
                stats["kept_uri"] += 1
            else:
                stats["skipped_dup"] += 1
            continue

        # === Literal triples: convert selected ones ===
        if isinstance(o, Literal):
            # Only convert if it's from a Wikidata or edai predicate
            if "wikidata.org" in p_str or "example.org/edai" in p_str:
                # Convert literal to a pseudo-entity URI
                lit_val = str(o).strip()
                if not lit_val or len(lit_val) > 100:
                    stats["skipped_literal_unconv"] += 1
                    continue

                # Create a clean URI for the literal value
                safe_val = lit_val.replace(" ", "_").replace("/", "_").replace("\\", "_")
                safe_val = safe_val.replace("(", "").replace(")", "").replace(",", "")
                safe_val = safe_val.replace("'", "").replace('"', "").replace(":", "_")
                safe_val = safe_val[:80]  # Limit length

                lit_uri = f"http://example.org/edai/lit_{safe_val}"

                p_short = p_str.split("/")[-1]
                if "#" in p_short:
                    p_short = p_short.split("#")[-1]

                triple_key = (s_str, p_short, lit_uri)
                if triple_key not in seen:
                    seen.add(triple_key)
                    triples.append((s_str, p_short, lit_uri))
                    stats["kept_literal_converted"] += 1
                else:
                    stats["skipped_dup"] += 1
            else:
                stats["skipped_literal_unconv"] += 1
            continue

    print(f"  Results:")
    print(f"    URI-URI triples kept:        {stats['kept_uri']}")
    print(f"    rdf:type triples kept:       {stats['kept_type']}")
    print(f"    rdfs:subClassOf kept:        {stats['kept_subclass']}")
    print(f"    Literals converted:          {stats['kept_literal_converted']}")
    print(f"    Skipped labels/meta:         {stats['skipped_meta'] + stats['skipped_label']}")
    print(f"    Skipped duplicates:          {stats['skipped_dup']}")
    print(f"    Skipped self-loops:          {stats['skipped_selfloop']}")
    print(f"    Skipped unconverted lits:    {stats['skipped_literal_unconv']}")
    print(f"    TOTAL KEPT:                  {len(triples)}")

    return triples


def filter_relations(triples: list, max_relations: int = 150) -> list:
    """Keep only relations that appear frequently enough."""
    rel_counts = Counter(p for _, p, _ in triples)

    # Start with min_count=3 and increase until under max_relations
    min_count = 3
    while True:
        kept = {r for r, c in rel_counts.items() if c >= min_count}
        if len(kept) <= max_relations:
            break
        min_count += 1

    filtered = [(s, p, o) for s, p, o in triples if p in kept]
    print(f"  Relation filter: min_count={min_count}, kept {len(kept)} relations, {len(filtered)} triples")
    return filtered


def smart_split(triples: list) -> tuple:
    """
    Better 80/10/10 split that ensures:
    1. Every entity in val/test also appears in train
    2. Split ratios are close to 80/10/10
    """
    random.seed(42)

    # First, find entities by degree
    entity_degree = Counter()
    for s, p, o in triples:
        entity_degree[s] += 1
        entity_degree[o] += 1

    # Sort triples: put triples with low-degree entities first (they go to train)
    def triple_min_degree(t):
        return min(entity_degree[t[0]], entity_degree[t[2]])

    sorted_triples = sorted(triples, key=triple_min_degree)

    # Phase 1: All triples with entities that appear <= 2 times go to train
    train = []
    remaining = []
    train_entities = set()

    for s, p, o in sorted_triples:
        if entity_degree[s] <= 2 or entity_degree[o] <= 2:
            train.append((s, p, o))
            train_entities.add(s)
            train_entities.add(o)
        else:
            remaining.append((s, p, o))

    print(f"  Phase 1: {len(train)} triples to train (low-degree entities)")
    print(f"  Remaining for split: {len(remaining)}")

    # Phase 2: From remaining, allocate to get close to 80/10/10 overall
    total = len(triples)
    target_val = total // 10
    target_test = total // 10

    random.shuffle(remaining)

    val = []
    test = []

    for s, p, o in remaining:
        if s in train_entities and o in train_entities:
            if len(test) < target_test:
                test.append((s, p, o))
            elif len(val) < target_val:
                val.append((s, p, o))
            else:
                train.append((s, p, o))
        else:
            # Entity not in train yet, must go to train
            train.append((s, p, o))
            train_entities.add(s)
            train_entities.add(o)

    return train, val, test


def save_splits(train, val, test, output_dir):
    """Save in PyKEEN-compatible TSV format."""
    os.makedirs(output_dir, exist_ok=True)

    for name, data in [("train.txt", train), ("valid.txt", val), ("test.txt", test)]:
        path = os.path.join(output_dir, name)
        with open(path, "w", encoding="utf-8") as f:
            for s, p, o in data:
                f.write(f"{s}\t{p}\t{o}\n")
        print(f"  {name}: {len(data)} triples")

    # Entity and relation mappings
    entities = set()
    relations = set()
    for split in [train, val, test]:
        for s, p, o in split:
            entities.add(s)
            entities.add(o)
            relations.add(p)

    entity2id = {e: i for i, e in enumerate(sorted(entities))}
    relation2id = {r: i for i, r in enumerate(sorted(relations))}

    with open(os.path.join(output_dir, "entity2id.txt"), "w", encoding="utf-8") as f:
        f.write(f"{len(entity2id)}\n")
        for e, i in sorted(entity2id.items(), key=lambda x: x[1]):
            f.write(f"{e}\t{i}\n")

    with open(os.path.join(output_dir, "relation2id.txt"), "w", encoding="utf-8") as f:
        f.write(f"{len(relation2id)}\n")
        for r, i in sorted(relation2id.items(), key=lambda x: x[1]):
            f.write(f"{r}\t{i}\n")

    return entities, relations


def create_subsamples(all_triples, output_dir):
    """Create subsamples for size sensitivity experiment."""
    total = len(all_triples)

    for name, target in [("10k", 10000), ("15k", 15000)]:
        if target > total:
            print(f"  ⚠️  {name}: only {total} triples available, using {total}")
            target = total

        random.seed(42)
        sampled = random.sample(all_triples, target)

        # Split
        random.shuffle(sampled)
        n = len(sampled)
        n_test = max(100, n // 10)
        n_val = max(100, n // 10)

        sub_dir = os.path.join(output_dir, f"subsample_{name}")
        os.makedirs(sub_dir, exist_ok=True)

        sub_train = sampled[:n - n_val - n_test]
        sub_val = sampled[n - n_val - n_test:n - n_test]
        sub_test = sampled[n - n_test:]

        for fname, data in [("train.txt", sub_train), ("valid.txt", sub_val), ("test.txt", sub_test)]:
            path = os.path.join(sub_dir, fname)
            with open(path, "w", encoding="utf-8") as f:
                for s, p, o in data:
                    f.write(f"{s}\t{p}\t{o}\n")

        print(f"  {name}: {len(sampled)} triples ({len(sub_train)}/{len(sub_val)}/{len(sub_test)})")


def main():
    print("=" * 60)
    print("  KGE DATA PREPARATION v2 (IMPROVED)")
    print("=" * 60)

    # Load KG
    print("\nLoading knowledge graph...")
    g = Graph()
    g.parse(FINAL_KG, format="turtle")
    print(f"  Raw triples: {len(g)}")

    # Clean
    print(f"\n--- CLEANING ---")
    triples = clean_for_embedding(g)

    # Filter relations
    print(f"\n--- RELATION FILTERING ---")
    triples = filter_relations(triples, max_relations=180)

    # Split
    print(f"\n--- SMART SPLIT ---")
    train, val, test = smart_split(triples)

    total = len(train) + len(val) + len(test)
    print(f"\n  Final split:")
    print(f"    Train: {len(train)} ({100*len(train)//total}%)")
    print(f"    Valid: {len(val)} ({100*len(val)//total}%)")
    print(f"    Test:  {len(test)} ({100*len(test)//total}%)")

    # Save
    print(f"\n--- SAVING ---")
    entities, relations = save_splits(train, val, test, KGE_DIR)

    # Subsamples
    print(f"\n--- SUBSAMPLES ---")
    all_triples = train + val + test
    create_subsamples(all_triples, KGE_DIR)

    # Final report
    print(f"\n{'=' * 60}")
    print(f"  FINAL KGE DATA STATISTICS")
    print(f"{'=' * 60}")
    print(f"  Total triples:    {total}")
    print(f"  Entities:         {len(entities)}")
    print(f"  Relations:        {len(relations)}")
    print(f"  Avg degree:       {2*total/max(len(entities),1):.1f}")
    print(f"\n  Split:")
    print(f"    Train: {len(train):6d} ({100*len(train)//total}%)")
    print(f"    Valid: {len(val):6d} ({100*len(val)//total}%)")
    print(f"    Test:  {len(test):6d} ({100*len(test)//total}%)")
    print(f"\n  Lab 3 targets:")
    t_ok = "✅" if total >= 15000 else "⚠️"
    e_ok = "✅" if 5000 <= len(entities) <= 30000 else "⚠️"
    r_ok = "✅" if 50 <= len(relations) <= 200 else "⚠️"
    print(f"    Triples:   {total:,} {t_ok}")
    print(f"    Entities:  {len(entities):,} {e_ok}")
    print(f"    Relations: {len(relations)} {r_ok}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()