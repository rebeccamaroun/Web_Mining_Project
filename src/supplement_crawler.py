"""
Supplement crawler: add more URLs to reach 7-8 saved articles.
Appends to the existing edtech_corpus.jsonl.
"""

import json
import os
import time
from datetime import datetime, timezone
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import trafilatura

MIN_WORDS = 500
USER_AGENT = "EduAI-ResearchBot/1.0 (university project)"
OUTPUT_FILE = "data/raw/edtech_corpus.jsonl"

EXTRA_URLS = [
    "https://pmc.ncbi.nlm.nih.gov/articles/PMC9069679/",
    "https://pmc.ncbi.nlm.nih.gov/articles/PMC10020843/",
    "https://pmc.ncbi.nlm.nih.gov/articles/PMC8893233/",
    "https://hai.stanford.edu/news/ai-and-education-are-two-key-research-areas",
    "https://www.sciencedirect.com/science/article/pii/S2666920X21000199",
]


def is_allowed_by_robots(url):
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    rp = RobotFileParser()
    rp.set_url(robots_url)
    try:
        rp.read()
        return rp.can_fetch(USER_AGENT, url)
    except Exception:
        return True


def extract_main_text(url):
    html = trafilatura.fetch_url(url)
    if html is None:
        return None
    return trafilatura.extract(html, include_comments=False, include_tables=False)


def main():
    print("=" * 60)
    print("  SUPPLEMENTING CORPUS WITH EXTRA URLs")
    print("=" * 60)

    saved = 0

    # Append mode — don't overwrite existing data
    with open(OUTPUT_FILE, "a", encoding="utf-8") as f:
        for i, url in enumerate(EXTRA_URLS, 1):
            print(f"\n[{i}/{len(EXTRA_URLS)}] {url}")

            if not is_allowed_by_robots(url):
                print(f"  ❌ BLOCKED by robots.txt")
                continue

            print(f"  ✅ Allowed — fetching...")
            text = extract_main_text(url)

            if text is None or len(text.split()) < MIN_WORDS:
                wc = len(text.split()) if text else 0
                print(f"  ❌ Skipped ({wc} words)")
                continue

            record = {
                "url": url,
                "domain": urlparse(url).netloc,
                "word_count": len(text.split()),
                "crawled_at": datetime.now(timezone.utc).isoformat(),
                "text": text,
            }
            json.dump(record, f, ensure_ascii=False)
            f.write("\n")
            saved += 1
            print(f"  ✅ Saved ({record['word_count']} words)")
            time.sleep(1)

    print(f"\n  Added {saved} articles to corpus")
    print("=" * 60)


if __name__ == "__main__":
    main()