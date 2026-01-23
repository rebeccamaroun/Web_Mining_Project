"""
Phase 2: Named Entity Recognition & Relation Extraction
Domain: Education and AI
Extracted from teammate's Colab notebook
"""

import json
import spacy

# Load spaCy model
nlp = spacy.load("en_core_web_sm")

# Allowed entity types (graph nodes)
ALLOWED_LABELS = {"PERSON", "ORG", "GPE", "DATE"}


def extract_entities(text):
    """
    Extract named entities from text.
    Returns list of {text, label} dictionaries.
    """
    doc = nlp(text)
    entities = []

    for ent in doc.ents:
        if ent.label_ in ALLOWED_LABELS:
            entities.append({
                "text": ent.text,
                "label": ent.label_
            })
    
    return entities


def extract_relations(text):
    """
    Extract sentence-level relations (co-occurring entities).
    Returns list of {source, target, sentence} dictionaries.
    """
    doc = nlp(text)
    relations = []

    for sent in doc.sents:
        ents = [ent for ent in sent.ents if ent.label_ in ALLOWED_LABELS]

        if len(ents) >= 2:
            for i in range(len(ents) - 1):
                relations.append({
                    "source": ents[i].text,
                    "target": ents[i + 1].text,
                    "sentence": sent.text
                })

    return relations


def extract_verb_relations(text):
    """
    Extract verb-based triples using dependency parsing.
    Returns list of {subject, relation, object, sentence} dictionaries.
    """
    doc = nlp(text)
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
                "sentence": sent.text
            })

    return triples


def process_corpus(input_file="data/raw/edtech_corpus.jsonl", 
                   output_file="data/processed/entities_relations.jsonl"):
    """
    Process entire corpus and extract entities + relations.
    """
    import os
    os.makedirs("data/processed", exist_ok=True)
    
    # Load documents
    documents = []
    with open(input_file, "r", encoding="utf-8") as f:
        for line in f:
            documents.append(json.loads(line))
    
    print(f"📚 Loaded {len(documents)} documents")
    
    # Process each document
    results = []
    
    for i, doc in enumerate(documents, 1):
        print(f"\n🔍 Processing document {i}/{len(documents)}")
        print(f"   URL: {doc['url'][:60]}...")
        
        text = doc["text"]
        
        # Extract entities
        entities = extract_entities(text)
        print(f"   ✅ Extracted {len(entities)} entities")
        
        # Extract relations
        relations = extract_relations(text)
        print(f"   ✅ Extracted {len(relations)} sentence-level relations")
        
        # Extract verb-based triples
        verb_relations = extract_verb_relations(text)
        print(f"   ✅ Extracted {len(verb_relations)} verb-based triples")
        
        result = {
            "url": doc["url"],
            "word_count": doc["word_count"],
            "entities": entities,
            "relations": relations,
            "verb_relations": verb_relations
        }
        
        results.append(result)
    
    # Save results
    with open(output_file, "w", encoding="utf-8") as f:
        for result in results:
            json.dump(result, f, ensure_ascii=False)
            f.write("\n")
    
    print(f"\n{'='*60}")
    print(f"✅ Phase 2 Complete!")
    print(f"Output: {output_file}")
    print(f"{'='*60}")
    
    # Print summary statistics
    total_entities = sum(len(r["entities"]) for r in results)
    total_relations = sum(len(r["relations"]) for r in results)
    total_verb_relations = sum(len(r["verb_relations"]) for r in results)
    
    print(f"\n📊 Summary:")
    print(f"   Total entities: {total_entities}")
    print(f"   Total sentence relations: {total_relations}")
    print(f"   Total verb triples: {total_verb_relations}")
    
    return results


def main():
    """Main function to run NER and relation extraction"""
    print("="*60)
    print("📚 EDUCATION & AI DOMAIN - PHASE 2: NER & RELATIONS")
    print("="*60)
    
    results = process_corpus()
    
    # Show sample entities from first document
    print("\n" + "="*60)
    print("📝 Sample Entities (First Document):")
    print("="*60)
    for ent in results[0]["entities"][:10]:
        print(f"   • {ent['text']} [{ent['label']}]")
    
    # Show sample verb relations from first document
    print("\n" + "="*60)
    print("📝 Sample Verb Relations (First Document):")
    print("="*60)
    for vr in results[0]["verb_relations"][:5]:
        print(f"   • ({vr['subject']}) --[{vr['relation']}]--> ({vr['object']})")


if __name__ == "__main__":
    main()