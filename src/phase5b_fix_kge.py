"""
Phase 5b-fix: Fix KGE data to meet lab targets.
- Filter to top 100-150 relations (target: 50-200)
- Rebalance train/val/test to proper 80/10/10
- Create subsamples (10k, 15k)
"""

import os
import random
from collections import Counter

KGE_DIR = "kg_artifacts/kge_data"
TRAIN_FILE = os.path.join(KGE_DIR, "train.txt")
VALID_FILE = os.path.join(KGE_DIR, "valid.txt")
TEST_FILE = os.path.join(KGE_DIR, "test.txt")


def load_triples(path):
    triples = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            parts = line.strip().split("\t")
            if len(parts) == 3:
                triples.append(tuple(parts))
    return triples


def save_triples(triples, path):
    with open(path, "w", encoding="utf-8") as f:
        for s, p, o in triples:
            f.write(f"{s}\t{p}\t{o}\n")


def main():
    print("=" * 60)
    print("  FIX KGE DATA: FILTER RELATIONS + REBALANCE")
    print("=" * 60)

    # Load all triples
    all_triples = []
    for f in [TRAIN_FILE, VALID_FILE, TEST_FILE]:
        all_triples.extend(load_triples(f))
    print(f"Loaded {len(all_triples)} total triples")

    # Count relations
    rel_counts = Counter(p for _, p, _ in all_triples)
    print(f"Current relations: {len(rel_counts)}")

    # Keep only relations with >= 5 occurrences (brings us to ~100-150 range)
    min_count = 5
    kept_relations = {r for r, c in rel_counts.items() if c >= min_count}
    print(f"Relations with >= {min_count} occurrences: {len(kept_relations)}")

    # If still too many, increase threshold
    if len(kept_relations) > 200:
        min_count = 10
        kept_relations = {r for r, c in rel_counts.items() if c >= min_count}
        print(f"Raised threshold to >= {min_count}: {len(kept_relations)} relations")

    if len(kept_relations) > 200:
        # Take top 150 by frequency
        top_rels = [r for r, _ in rel_counts.most_common(150)]
        kept_relations = set(top_rels)
        print(f"Capped at top 150 relations")

    # Filter triples
    filtered = [(s, p, o) for s, p, o in all_triples if p in kept_relations]
    print(f"Filtered triples: {len(filtered)} (from {len(all_triples)})")

    # Deduplicate
    filtered = list(set(filtered))
    print(f"After dedup: {len(filtered)}")

    # Count entities and relations
    entities = set()
    relations = set()
    for s, p, o in filtered:
        entities.add(s)
        entities.add(o)
        relations.add(p)

    print(f"Entities: {len(entities)}, Relations: {len(relations)}")

    # Split 80/10/10 with entity coverage guarantee
    random.seed(42)
    random.shuffle(filtered)

    n = len(filtered)
    n_test = max(1, n // 10)
    n_val = max(1, n // 10)

    # First pass
    train_candidates = filtered[:n - n_val - n_test]
    val_candidates = filtered[n - n_val - n_test:n - n_test]
    test_candidates = filtered[n - n_test:]

    # Get training entities
    train_entities = set()
    for s, p, o in train_candidates:
        train_entities.add(s)
        train_entities.add(o)

    # Move triples with unseen entities to train
    train = list(train_candidates)
    val = []
    test = []

    for s, p, o in val_candidates:
        if s not in train_entities or o not in train_entities:
            train.append((s, p, o))
            train_entities.add(s)
            train_entities.add(o)
        else:
            val.append((s, p, o))

    for s, p, o in test_candidates:
        if s not in train_entities or o not in train_entities:
            train.append((s, p, o))
            train_entities.add(s)
            train_entities.add(o)
        else:
            test.append((s, p, o))

    # Save
    save_triples(train, TRAIN_FILE)
    save_triples(val, VALID_FILE)
    save_triples(test, TEST_FILE)

    # Save updated entity/relation mappings
    entity2id = {e: i for i, e in enumerate(sorted(entities))}
    relation2id = {r: i for i, r in enumerate(sorted(relations))}

    with open(os.path.join(KGE_DIR, "entity2id.txt"), "w", encoding="utf-8") as f:
        f.write(f"{len(entity2id)}\n")
        for e, i in sorted(entity2id.items(), key=lambda x: x[1]):
            f.write(f"{e}\t{i}\n")

    with open(os.path.join(KGE_DIR, "relation2id.txt"), "w", encoding="utf-8") as f:
        f.write(f"{len(relation2id)}\n")
        for r, i in sorted(relation2id.items(), key=lambda x: x[1]):
            f.write(f"{r}\t{i}\n")

    # Create subsamples for size sensitivity
    all_clean = train + val + test
    for name, target in [("10k", 10000), ("15k", 15000)]:
        if target >= len(all_clean):
            print(f"  ⚠️  {name}: not enough triples ({len(all_clean)}), skipping")
            continue

        random.seed(42)
        sampled = random.sample(all_clean, target)
        random.shuffle(sampled)

        sn = len(sampled)
        st = max(1, sn // 10)
        sv = max(1, sn // 10)

        sub_dir = os.path.join(KGE_DIR, f"subsample_{name}")
        os.makedirs(sub_dir, exist_ok=True)
        save_triples(sampled[:sn - sv - st], os.path.join(sub_dir, "train.txt"))
        save_triples(sampled[sn - sv - st:sn - st], os.path.join(sub_dir, "valid.txt"))
        save_triples(sampled[sn - st:], os.path.join(sub_dir, "test.txt"))
        print(f"  Created {name} subsample: {target} triples")

    # Final stats
    total = len(train) + len(val) + len(test)
    print(f"\n{'=' * 60}")
    print(f"  FIXED KGE DATA STATISTICS")
    print(f"{'=' * 60}")
    print(f"  Total triples:    {total}")
    print(f"  Entities:         {len(entities)}")
    print(f"  Relations:        {len(relations)}")
    print(f"  Train:            {len(train)} ({100*len(train)//total}%)")
    print(f"  Valid:            {len(val)} ({100*len(val)//total}%)")
    print(f"  Test:             {len(test)} ({100*len(test)//total}%)")
    print(f"\n  Lab 3 targets:")
    print(f"    Relations: {len(relations)} (target: 50-200) {'✅' if 50 <= len(relations) <= 200 else '⚠️'}")
    print(f"    Entities:  {len(entities)} (target: 5k-30k) {'✅' if 5000 <= len(entities) <= 30000 else '⚠️'}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()