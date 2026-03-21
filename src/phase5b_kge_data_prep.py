"""
Phase 5b: KGE Data Preparation
Cleans the expanded KB and creates train/validation/test splits
for Knowledge Graph Embedding training.

Lab 3 requirements:
- Remove duplicate triples
- Remove inconsistent URIs
- Ensure unique entity/relation indexing
- Remove literal-heavy predicates
- 80/10/10 split with no entity leakage
"""

import os
import random
from collections import Counter
from rdflib import Graph, Namespace, URIRef, Literal, RDF, RDFS, OWL

FINAL_KG = "kg_artifacts/knowledge_graph_final.ttl"
KGE_DIR = "kg_artifacts/kge_data"

EDAI = Namespace("http://example.org/edai/")
WD = Namespace("http://www.wikidata.org/entity/")
WDT = Namespace("http://www.wikidata.org/prop/direct/")


def clean_for_embedding(g: Graph) -> list:
    """
    Clean the KG for embedding training.
    Returns list of (subject, predicate, object) string tuples.
    """
    print("  Step 1: Filtering triples...")

    # Skip predicates that are not useful for KGE
    skip_predicates = {
        str(RDF.type),
        str(RDFS.label),
        str(RDFS.comment),
        str(RDFS.subClassOf),
        str(RDFS.subPropertyOf),
        str(OWL.sameAs),
        str(OWL.equivalentProperty),
        str(OWL.equivalentClass),
        str(EDAI.entityLabel),
        str(EDAI.sourceURL),
        str(EDAI.alignmentConfidence),
        "http://schema.org/description",
        "http://www.w3.org/2004/02/skos/core#altLabel",
    }

    # Also skip OWL meta-predicates
    skip_prefixes = [
        "http://www.w3.org/2002/07/owl#",
        "http://www.w3.org/2000/01/rdf-schema#",
        "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    ]

    triples = []
    skipped_literal = 0
    skipped_meta = 0
    skipped_dup = 0
    seen = set()

    for s, p, o in g:
        # Skip if predicate is in skip list
        p_str = str(p)
        if p_str in skip_predicates:
            skipped_meta += 1
            continue

        # Skip OWL/RDF/RDFS meta predicates
        if any(p_str.startswith(prefix) for prefix in skip_prefixes):
            skipped_meta += 1
            continue

        # Skip literal objects (KGE needs entity-entity-relation triples)
        if isinstance(o, Literal):
            skipped_literal += 1
            continue

        # Skip if subject or object is not a URI
        if not isinstance(s, URIRef) or not isinstance(o, URIRef):
            continue

        s_str = str(s)
        o_str = str(o)

        # Skip self-loops
        if s_str == o_str:
            continue

        # Deduplicate
        triple_key = (s_str, p_str, o_str)
        if triple_key in seen:
            skipped_dup += 1
            continue
        seen.add(triple_key)

        triples.append((s_str, p_str, o_str))

    print(f"  Kept: {len(triples)} triples")
    print(f"  Skipped literals: {skipped_literal}")
    print(f"  Skipped meta/schema: {skipped_meta}")
    print(f"  Skipped duplicates: {skipped_dup}")

    return triples


def build_indices(triples: list) -> tuple:
    """Build entity and relation indices."""
    entities = set()
    relations = set()

    for s, p, o in triples:
        entities.add(s)
        entities.add(o)
        relations.add(p)

    entity2id = {e: i for i, e in enumerate(sorted(entities))}
    relation2id = {r: i for i, r in enumerate(sorted(relations))}

    return entity2id, relation2id


def split_data(triples: list, entity2id: dict) -> tuple:
    """
    Split into 80/10/10 train/val/test.
    Ensures no entity appears ONLY in val/test.
    """
    random.seed(42)
    random.shuffle(triples)

    n = len(triples)
    n_test = max(1, n // 10)
    n_val = max(1, n // 10)
    n_train = n - n_val - n_test

    # First pass: identify which entities appear in training
    train_candidates = triples[:n_train]
    val_candidates = triples[n_train:n_train + n_val]
    test_candidates = triples[n_train + n_val:]

    # Get entities in training set
    train_entities = set()
    for s, p, o in train_candidates:
        train_entities.add(s)
        train_entities.add(o)

    # Move triples with unseen entities from val/test to train
    train = list(train_candidates)
    val = []
    test = []
    moved_to_train = 0

    for s, p, o in val_candidates:
        if s not in train_entities or o not in train_entities:
            train.append((s, p, o))
            train_entities.add(s)
            train_entities.add(o)
            moved_to_train += 1
        else:
            val.append((s, p, o))

    for s, p, o in test_candidates:
        if s not in train_entities or o not in train_entities:
            train.append((s, p, o))
            train_entities.add(s)
            train_entities.add(o)
            moved_to_train += 1
        else:
            test.append((s, p, o))

    print(f"  Moved {moved_to_train} triples to train (entity coverage)")
    print(f"  Train: {len(train)}, Val: {len(val)}, Test: {len(test)}")

    return train, val, test


def save_splits(train, val, test, entity2id, relation2id, output_dir):
    """Save the splits in PyKEEN-compatible format."""
    os.makedirs(output_dir, exist_ok=True)

    # Save triples as TSV (head, relation, tail)
    for name, data in [("train.txt", train), ("valid.txt", val), ("test.txt", test)]:
        path = os.path.join(output_dir, name)
        with open(path, "w", encoding="utf-8") as f:
            for s, p, o in data:
                f.write(f"{s}\t{p}\t{o}\n")
        print(f"  Saved {path}: {len(data)} triples")

    # Save entity and relation mappings
    ent_path = os.path.join(output_dir, "entity2id.txt")
    with open(ent_path, "w", encoding="utf-8") as f:
        f.write(f"{len(entity2id)}\n")
        for entity, idx in sorted(entity2id.items(), key=lambda x: x[1]):
            f.write(f"{entity}\t{idx}\n")
    print(f"  Saved {ent_path}: {len(entity2id)} entities")

    rel_path = os.path.join(output_dir, "relation2id.txt")
    with open(rel_path, "w", encoding="utf-8") as f:
        f.write(f"{len(relation2id)}\n")
        for rel, idx in sorted(relation2id.items(), key=lambda x: x[1]):
            f.write(f"{rel}\t{idx}\n")
    print(f"  Saved {rel_path}: {len(relation2id)} relations")


def print_statistics(train, val, test, entity2id, relation2id):
    """Print detailed statistics about the prepared data."""

    all_triples = train + val + test

    # Relation distribution
    rel_counts = Counter(p for _, p, _ in all_triples)
    top_rels = rel_counts.most_common(15)

    # Entity degree
    entity_degree = Counter()
    for s, p, o in all_triples:
        entity_degree[s] += 1
        entity_degree[o] += 1

    avg_degree = sum(entity_degree.values()) / max(len(entity_degree), 1)

    print(f"\n{'=' * 60}")
    print(f"  KGE DATA STATISTICS")
    print(f"{'=' * 60}")
    print(f"  Total triples:    {len(all_triples)}")
    print(f"  Total entities:   {len(entity2id)}")
    print(f"  Total relations:  {len(relation2id)}")
    print(f"  Avg entity degree:{avg_degree:.1f}")
    print(f"\n  Split:")
    print(f"    Train:  {len(train):6d} ({100*len(train)//len(all_triples)}%)")
    print(f"    Valid:  {len(val):6d} ({100*len(val)//len(all_triples)}%)")
    print(f"    Test:   {len(test):6d} ({100*len(test)//len(all_triples)}%)")

    print(f"\n  Top 15 relations:")
    for rel, count in top_rels:
        short = rel.split("/")[-1] if "/" in rel else rel
        print(f"    {short:40s} {count}")

    # Check target ranges from lab
    print(f"\n  Lab 3 Target Ranges:")
    print(f"    Triples:   {len(all_triples):,} (target: 50k-200k) {'✅' if 5000 <= len(all_triples) else '⚠️'}")
    print(f"    Entities:  {len(entity2id):,} (target: 5k-30k) {'✅' if 5000 <= len(entity2id) <= 30000 else '⚠️'}")
    print(f"    Relations: {len(relation2id)} (target: 50-200) {'✅' if 50 <= len(relation2id) <= 200 else '⚠️'}")


def create_subsamples(train, val, test, output_dir):
    """
    Create subsampled versions for KB Size Sensitivity experiment.
    Lab requires: 20k, 50k, and full dataset comparisons.
    """
    all_triples = train + val + test
    total = len(all_triples)

    for target_name, target_size in [("20k", 20000), ("50k", 50000)]:
        if target_size >= total:
            print(f"  ⚠️  {target_name}: dataset has only {total} triples, skipping")
            continue

        # Subsample
        random.seed(42)
        sampled = random.sample(all_triples, min(target_size, total))

        # Split 80/10/10
        random.shuffle(sampled)
        n = len(sampled)
        n_test = max(1, n // 10)
        n_val = max(1, n // 10)

        sub_train = sampled[:n - n_val - n_test]
        sub_val = sampled[n - n_val - n_test:n - n_test]
        sub_test = sampled[n - n_test:]

        sub_dir = os.path.join(output_dir, f"subsample_{target_name}")
        os.makedirs(sub_dir, exist_ok=True)

        for name, data in [("train.txt", sub_train), ("valid.txt", sub_val), ("test.txt", sub_test)]:
            path = os.path.join(sub_dir, name)
            with open(path, "w", encoding="utf-8") as f:
                for s, p, o in data:
                    f.write(f"{s}\t{p}\t{o}\n")

        print(f"  Created {target_name} subsample: {len(sampled)} triples in {sub_dir}")


def main():
    print("=" * 60)
    print("  EDUCATION & AI — PHASE 5b: KGE DATA PREPARATION")
    print("=" * 60)

    # Load the expanded KG
    print("\nLoading knowledge graph (this may take a minute)...")
    g = Graph()
    g.parse(FINAL_KG, format="turtle")
    print(f"  Loaded {len(g)} triples")

    # Step 1: Clean for embedding
    print(f"\n{'=' * 50}")
    print("  STEP 1: CLEANING FOR EMBEDDING")
    print(f"{'=' * 50}")
    triples = clean_for_embedding(g)

    # Step 2: Build indices
    print(f"\n{'=' * 50}")
    print("  STEP 2: BUILDING ENTITY/RELATION INDICES")
    print(f"{'=' * 50}")
    entity2id, relation2id = build_indices(triples)
    print(f"  Unique entities:  {len(entity2id)}")
    print(f"  Unique relations: {len(relation2id)}")

    # Step 3: Split data
    print(f"\n{'=' * 50}")
    print("  STEP 3: TRAIN/VALIDATION/TEST SPLIT")
    print(f"{'=' * 50}")
    train, val, test = split_data(triples, entity2id)

    # Step 4: Save
    print(f"\n{'=' * 50}")
    print("  STEP 4: SAVING FILES")
    print(f"{'=' * 50}")
    save_splits(train, val, test, entity2id, relation2id, KGE_DIR)

    # Step 5: Create subsamples for size sensitivity
    print(f"\n{'=' * 50}")
    print("  STEP 5: CREATING SUBSAMPLES (Size Sensitivity)")
    print(f"{'=' * 50}")
    create_subsamples(train, val, test, KGE_DIR)

    # Print statistics
    print_statistics(train, val, test, entity2id, relation2id)

    print(f"\n{'=' * 60}")
    print(f"  KGE DATA PREPARATION COMPLETE")
    print(f"  Output directory: {KGE_DIR}")
    print(f"  Ready for PyKEEN training (Day 5)")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()