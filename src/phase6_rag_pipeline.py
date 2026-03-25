"""
Phase 6: RAG Pipeline (v2 - improved prompting)
Uses few-shot examples to help the small LLM generate valid SPARQL.
"""

import re
import sys
import json
from typing import List, Tuple
from rdflib import Graph
import requests

# ── Configuration ──────────────────────────────────────────────
TTL_FILE = "kg_artifacts/knowledge_graph_final.ttl"
OLLAMA_URL = "http://localhost:11434/api/generate"
MODEL = "gemma:2b"
MAX_REPAIR_ATTEMPTS = 2


# ── LLM Call ───────────────────────────────────────────────────
def ask_local_llm(prompt: str) -> str:
    payload = {"model": MODEL, "prompt": prompt, "stream": False}
    try:
        response = requests.post(OLLAMA_URL, json=payload, timeout=120)
        if response.status_code != 200:
            return f"[Ollama error {response.status_code}]"
        return response.json().get("response", "")
    except requests.exceptions.ConnectionError:
        return "[Error: Cannot connect to Ollama]"
    except Exception as e:
        return f"[Error: {e}]"


# ── Load Graph ─────────────────────────────────────────────────
def load_graph(ttl_path: str) -> Graph:
    g = Graph()
    g.parse(ttl_path, format="turtle")
    print(f"  Loaded {len(g)} triples from {ttl_path}")
    return g


# ── Build Schema Summary (compact) ────────────────────────────
def build_schema_summary(g: Graph) -> str:
    classes_q = """
    SELECT DISTINCT ?cls (COUNT(?s) AS ?cnt) WHERE {
        ?s a ?cls .
        FILTER(STRSTARTS(STR(?cls), "http://example.org/edai/"))
    } GROUP BY ?cls ORDER BY DESC(?cnt)
    """
    classes = [(str(r[0]), int(r[1])) for r in g.query(classes_q)]

    preds_q = """
    SELECT DISTINCT ?p (COUNT(*) AS ?cnt) WHERE {
        ?s ?p ?o .
        FILTER(STRSTARTS(STR(?p), "http://example.org/edai/"))
    } GROUP BY ?p ORDER BY DESC(?cnt) LIMIT 20
    """
    preds = [(str(r[0]), int(r[1])) for r in g.query(preds_q)]

    samples_q = """
    SELECT ?s ?label ?type WHERE {
        ?s rdfs:label ?label .
        ?s a ?type .
        FILTER(STRSTARTS(STR(?type), "http://example.org/edai/"))
    } LIMIT 15
    """
    samples = [(str(r[0]), str(r[1]), str(r[2])) for r in g.query(samples_q)]

    cls_lines = "\n".join(f"  edai:{c.split('/')[-1]} ({cnt} instances)" for c, cnt in classes)
    pred_lines = "\n".join(f"  edai:{p.split('/')[-1]} ({cnt} triples)" for p, cnt in preds)
    sample_lines = "\n".join(f"  <{s}> rdfs:label \"{l}\" ; a <{t}>" for s, l, t in samples[:10])

    return f"""PREFIX edai: <http://example.org/edai/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>

CLASSES:
{cls_lines}

PREDICATES (edai namespace):
{pred_lines}

SAMPLE ENTITIES:
{sample_lines}"""


# ── SPARQL Generation with Few-Shot ───────────────────────────
FEW_SHOT_PROMPT = """You are a SPARQL query generator. Convert the QUESTION into a SPARQL SELECT query.

RULES:
- Use ONLY the prefixes and predicates from the SCHEMA below.
- Return ONLY the SPARQL query inside ```sparql ... ``` tags.
- No explanations. No extra text.
- Use (COUNT(?x) AS ?count) not COUNT(*).
- Always use PREFIX declarations at the top.

SCHEMA:
{schema}

EXAMPLES:

Question: How many persons are in the graph?
```sparql
PREFIX edai: <http://example.org/edai/>
SELECT (COUNT(?p) AS ?count) WHERE {{
  ?p a edai:Person .
}}
```

Question: List all universities.
```sparql
PREFIX edai: <http://example.org/edai/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?uni ?name WHERE {{
  ?uni a edai:University .
  ?uni rdfs:label ?name .
}}
```

Question: Who are the co-authors of a specific person?
```sparql
PREFIX edai: <http://example.org/edai/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?coauthor ?name WHERE {{
  ?person rdfs:label "Sarah McGrew" .
  ?person edai:coAuthorWith ?coauthor .
  ?coauthor rdfs:label ?name .
}}
```

Question: Which organizations is a person affiliated with?
```sparql
PREFIX edai: <http://example.org/edai/>
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
SELECT ?person ?personName ?org ?orgName WHERE {{
  ?person edai:affiliatedWith ?org .
  ?person rdfs:label ?personName .
  ?org rdfs:label ?orgName .
}} LIMIT 10
```

Question: How many co-author relationships exist?
```sparql
PREFIX edai: <http://example.org/edai/>
SELECT (COUNT(?s) AS ?count) WHERE {{
  ?s edai:coAuthorWith ?o .
}}
```

Now answer this:

Question: {question}
"""

CODE_BLOCK_RE = re.compile(r"```(?:sparql)?\s*(.*?)```", re.IGNORECASE | re.DOTALL)


def extract_sparql(text: str) -> str:
    m = CODE_BLOCK_RE.search(text)
    if m:
        query = m.group(1).strip()
        query = re.sub(r'</start_of_turn>.*', '', query, flags=re.DOTALL)
        query = re.sub(r'<end_of_turn>.*', '', query, flags=re.DOTALL)
        return query.strip()
    lines = text.strip().split('\n')
    sparql_lines = []
    in_query = False
    for line in lines:
        if 'SELECT' in line.upper() or 'PREFIX' in line.upper():
            in_query = True
        if in_query:
            clean = re.sub(r'</start_of_turn>.*', '', line)
            sparql_lines.append(clean)
            if '}' in line:
                break
    if sparql_lines:
        return '\n'.join(sparql_lines).strip()
    return text.strip()


def generate_sparql(question: str, schema: str) -> str:
    prompt = FEW_SHOT_PROMPT.format(schema=schema, question=question)
    raw = ask_local_llm(prompt)
    return extract_sparql(raw)


# ── Execute + Self-Repair ─────────────────────────────────────
def run_sparql(g: Graph, query: str) -> Tuple[List[str], List[Tuple]]:
    res = g.query(query)
    vars_ = [str(v) for v in res.vars] if res.vars else []
    rows = [tuple(str(cell) for cell in r) for r in res]
    return vars_, rows


def repair_sparql(schema: str, question: str, bad_query: str, error: str) -> str:
    prompt = f"""The SPARQL query below failed. Fix it using ONLY predicates from the schema.

SCHEMA:
{schema}

QUESTION: {question}

FAILED QUERY:
{bad_query}

ERROR: {error}

Return ONLY the corrected query in ```sparql ... ``` tags. No explanations.
"""
    raw = ask_local_llm(prompt)
    return extract_sparql(raw)


def answer_with_rag(g: Graph, schema: str, question: str) -> dict:
    sparql = generate_sparql(question, schema)
    try:
        vars_, rows = run_sparql(g, sparql)
        return {"query": sparql, "vars": vars_, "rows": rows,
                "repaired": False, "repair_attempts": 0, "error": None}
    except Exception as e:
        err = str(e)

    current = sparql
    for attempt in range(1, MAX_REPAIR_ATTEMPTS + 1):
        repaired = repair_sparql(schema, question, current, err)
        try:
            vars_, rows = run_sparql(g, repaired)
            return {"query": repaired, "vars": vars_, "rows": rows,
                    "repaired": True, "repair_attempts": attempt, "error": None}
        except Exception as e2:
            err = str(e2)
            current = repaired

    return {"query": current, "vars": [], "rows": [],
            "repaired": True, "repair_attempts": MAX_REPAIR_ATTEMPTS, "error": err}


# ── Baseline ──────────────────────────────────────────────────
def answer_no_rag(question: str) -> str:
    return ask_local_llm(f"Answer this question as best you can:\n\n{question}")


# ── Pretty Print ──────────────────────────────────────────────
def pretty_print(result: dict):
    print(f"\n  [SPARQL Query]")
    for line in result['query'].split('\n'):
        print(f"  {line}")
    if result["repaired"]:
        print(f"  [Self-repair: {result['repair_attempts']} attempt(s)]")
    if result.get("error"):
        print(f"  [Execution Error] {result['error']}")
        return
    vars_ = result.get("vars", [])
    rows = result.get("rows", [])
    if not rows:
        print("  [No results returned]")
        return
    print(f"\n  [Results] ({len(rows)} rows)")
    print(f"  {' | '.join(vars_)}")
    print(f"  {'-' * max(40, len(' | '.join(vars_)))}")
    for r in rows[:15]:
        short = [c.replace("http://example.org/edai/", "edai:")
                  .replace("http://www.wikidata.org/entity/", "wd:")
                 for c in r]
        print(f"  {' | '.join(short)}")
    if len(rows) > 15:
        print(f"  ... ({len(rows)-15} more)")


# ── Evaluation ────────────────────────────────────────────────
EVAL_QUESTIONS = [
    "How many persons are in the knowledge graph?",
    "List all universities in the knowledge graph.",
    "Who are the co-authors of Sarah McGrew?",
    "Which organizations are located in the United States?",
    "What is Google's country of origin?",
    "How many co-author relationships exist in the graph?",
    "List 5 persons and their affiliations.",
]


def run_eval(g, schema):
    print("\n" + "=" * 65)
    print("  EVALUATION: BASELINE vs RAG")
    print("=" * 65)
    results = []
    for i, q in enumerate(EVAL_QUESTIONS, 1):
        print(f"\n{'─' * 65}")
        print(f"  Q{i}: {q}")
        print(f"{'─' * 65}")
        print("\n  ── Baseline (No RAG) ──")
        baseline = answer_no_rag(q)
        print(f"  {baseline[:300]}{'...' if len(baseline) > 300 else ''}")
        print("\n  ── RAG (SPARQL Generation) ──")
        rag = answer_with_rag(g, schema, q)
        pretty_print(rag)
        results.append({
            "question": q, "baseline": baseline[:500],
            "rag_query": rag["query"], "rag_rows": len(rag.get("rows", [])),
            "rag_repaired": rag["repaired"], "rag_error": rag.get("error"),
        })
    with open("kg_artifacts/rag_evaluation.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    print(f"\n  Saved: kg_artifacts/rag_evaluation.json")
    return results


# ── CLI Demo ──────────────────────────────────────────────────
def cli_demo(g, schema):
    print("\n" + "=" * 65)
    print("  RAG DEMO — Ask questions about the Education & AI KB")
    print("  Type 'quit' to exit, 'eval' to re-run evaluation.")
    print("=" * 65)
    while True:
        try:
            q = input("\n  Question: ").strip()
        except (EOFError, KeyboardInterrupt):
            break
        if not q: continue
        if q.lower() == "quit": break
        if q.lower() == "eval":
            run_eval(g, schema)
            continue
        print("\n  ── Baseline (No RAG) ──")
        print(f"  {answer_no_rag(q)[:400]}")
        print("\n  ── RAG (SPARQL Generation) ──")
        pretty_print(answer_with_rag(g, schema, q))


def main():
    print("=" * 65)
    print("  EDUCATION & AI — PHASE 6: RAG PIPELINE")
    print("=" * 65)
    print("\n  Checking Ollama...")
    test = ask_local_llm("Say hello in one word.")
    if "[Error" in test:
        print(f"  {test}")
        print("  Start Ollama first: ollama serve")
        sys.exit(1)
    print(f"  ✅ Ollama connected ({MODEL})")
    print("\n  Loading knowledge graph...")
    g = load_graph(TTL_FILE)
    print("  Building schema summary...")
    schema = build_schema_summary(g)
    print(f"  Schema: {len(schema)} chars")
    run_eval(g, schema)
    cli_demo(g, schema)
    print("\n  Done!")


if __name__ == "__main__":
    main()