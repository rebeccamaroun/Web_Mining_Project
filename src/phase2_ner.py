"""
Phase 2: Named Entity Recognition & Relation Extraction
Domain: Education and AI
Uses the transformer-based spaCy model (en_core_web_trf) for higher accuracy.
"""

import json
import os

import spacy

# ── Configuration ──────────────────────────────────────────────
INPUT_FILE = "data/raw/edtech_corpus.jsonl"
OUTPUT_FILE = "data/processed/entities_relations.jsonl"

# Use transformer model for better NER accuracy
NLP_MODEL = "en_core_web_trf"

# Entity types we care about for the knowledge graph
ALLOWED_LABELS = {"PERSON", "ORG", "GPE", "DATE", "WORK_OF_ART", "EVENT", "FAC"}

# Minimum entity length (filters out noise like single characters)
MIN_ENTITY_LENGTH = 2

# Entities to skip (common false positives from academic text)
SKIP_ENTITIES = {
    "limitations", "promises", "findings", "results", "methods",
    "discussion", "conclusion", "abstract", "introduction",
    "figure", "table", "section", "chapter",
}


# ── Load model ─────────────────────────────────────────────────
def load_nlp_model():
    """Load the spaCy transformer model."""
    try:
        nlp = spacy.load(NLP_MODEL)
        print(f"Loaded spaCy model: {NLP_MODEL}")
    except OSError:
        print(f"Model '{NLP_MODEL}' not found. Installing...")
        os.system(f"python -m spacy download {NLP_MODEL}")
        nlp = spacy.load(NLP_MODEL)
        print(f"Loaded spaCy model: {NLP_MODEL}")
    return nlp


# ── Entity extraction ──────────────────────────────────────────
def is_valid_entity(ent_text: str, ent_label: str) -> bool:
    """Filter out noisy / low-quality entities."""
    text_clean = ent_text.strip().lower()

    # Too short
    if len(text_clean) < MIN_ENTITY_LENGTH:
        return False

    # Known false positives
    if text_clean in SKIP_ENTITIES:
        return False

    # Contains newlines (likely a parsing artifact)
    if "\n" in ent_text:
        return False

    # All digits but not a date (likely noise)
    if text_clean.isdigit() and ent_label != "DATE":
        return False

    return True


def extract_entities(doc):
    """
    Extract named entities from a spaCy doc.
    Returns deduplicated list of {text, label} dicts.
    """
    entities = []
    seen = set()

    for ent in doc.ents:
        if ent.label_ not in ALLOWED_LABELS:
            continue
        if not is_valid_entity(ent.text, ent.label_):
            continue

        # Normalize whitespace
        clean_text = " ".join(ent.text.split())
        key = (clean_text.lower(), ent.label_)

        if key not in seen:
            seen.add(key)
            entities.append({
                "text": clean_text,
                "label": ent.label_
            })

    return entities


# ── Relation extraction ────────────────────────────────────────
def extract_relations(doc):
    """
    Extract sentence-level relations (co-occurring entities).
    Returns list of {source, source_label, target, target_label, sentence} dicts.
    """
    relations = []

    for sent in doc.sents:
        ents = [
            ent for ent in sent.ents
            if ent.label_ in ALLOWED_LABELS and is_valid_entity(ent.text, ent.label_)
        ]

        if len(ents) >= 2:
            for i in range(len(ents) - 1):
                source_text = " ".join(ents[i].text.split())
                target_text = " ".join(ents[i + 1].text.split())

                relations.append({
                    "source": source_text,
                    "source_label": ents[i].label_,
                    "target": target_text,
                    "target_label": ents[i + 1].label_,
                    "sentence": sent.text.strip(),
                })

    return relations


def extract_verb_relations(doc):
    """
    Extract verb-based triples using dependency parsing.
    Returns list of {subject, relation, object, sentence} dicts.
    """
    triples = []

    for sent in doc.sents:
        subject = None
        verb = None
        obj = None

        for token in sent:
            if token.dep_ == "nsubj":
                subject = token.text
                verb = token.head.text
            elif token.dep_ == "dobj":
                obj = token.text

        if subject and verb and obj:
            triples.append({
                "subject": subject,
                "relation": verb,
                "object": obj,
                "sentence": sent.text.strip(),
            })

    return triples


# ── Main pipeline ──────────────────────────────────────────────
def process_corpus(nlp, input_file=INPUT_FILE, output_file=OUTPUT_FILE):
    """Process entire corpus: extract entities + relations."""
    os.makedirs(os.path.dirname(output_file), exist_ok=True)

    # Load documents
    documents = []
    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            documents.append(json.loads(line))

    print(f"\nLoaded {len(documents)} documents from {input_file}")

    results = []

    for i, doc_data in enumerate(documents, 1):
        print(f"\n[{i}/{len(documents)}] Processing: {doc_data['url'][:70]}...")

        text = doc_data["text"]
        doc = nlp(text)

        entities = extract_entities(doc)
        relations = extract_relations(doc)
        verb_relations = extract_verb_relations(doc)

        print(f"  Entities: {len(entities)}  |  Relations: {len(relations)}  |  Verb triples: {len(verb_relations)}")

        result = {
            "url": doc_data["url"],
            "domain": doc_data.get("domain", ""),
            "word_count": doc_data["word_count"],
            "entities": entities,
            "relations": relations,
            "verb_relations": verb_relations,
        }
        results.append(result)

    # Save
    with open(output_file, "w", encoding="utf-8") as f:
        for result in results:
            json.dump(result, f, ensure_ascii=False)
            f.write("\n")

    # Summary
    total_ents = sum(len(r["entities"]) for r in results)
    total_rels = sum(len(r["relations"]) for r in results)
    total_verbs = sum(len(r["verb_relations"]) for r in results)

    print(f"\n{'=' * 60}")
    print(f"  NER & RELATION EXTRACTION COMPLETE")
    print(f"{'=' * 60}")
    print(f"  Documents processed:    {len(results)}")
    print(f"  Unique entities:        {total_ents}")
    print(f"  Sentence relations:     {total_rels}")
    print(f"  Verb-based triples:     {total_verbs}")
    print(f"  Output: {output_file}")
    print(f"{'=' * 60}")

    return results


# ── Ambiguity analysis (for report) ───────────────────────────
def show_ambiguity_examples(results):
    """
    Print examples of entity ambiguity for the report.
    The prof requires 3 ambiguity cases in the final report.
    """
    print(f"\n{'=' * 60}")
    print(f"  AMBIGUITY EXAMPLES (for report)")
    print(f"{'=' * 60}")

    print("""
  1. PERSON vs ORG confusion:
     Academic text often contains author names adjacent to journal
     names (e.g., "Sarah Pedersen AERA Open"), causing the NER model
     to merge them into a single PERSON entity.

  2. GPE vs ORG ambiguity:
     "MIT" could be tagged as ORG (the university) or GPE
     (the location). Similarly "Cambridge" could be either.

  3. DATE granularity:
     "2024" vs "January 2024" vs "the 2024 academic year" are all
     tagged as DATE, but have very different semantics for a KG.
     The model cannot distinguish between publication dates and
     event dates.
    """)


def main():
    print("=" * 60)
    print("  EDUCATION & AI — PHASE 2: NER & RELATIONS")
    print("=" * 60)

    nlp = load_nlp_model()
    results = process_corpus(nlp)

    # Show sample output
    if results:
        print(f"\nSample entities (first document):")
        for ent in results[0]["entities"][:10]:
            print(f"  {ent['text']:40s} [{ent['label']}]")

        print(f"\nSample verb relations (first document):")
        for vr in results[0]["verb_relations"][:5]:
            print(f"  ({vr['subject']}) --[{vr['relation']}]--> ({vr['object']})")

    show_ambiguity_examples(results)


if __name__ == "__main__":
    main()