"""
Phase 1: Web Crawling & Cleaning
Domain: Education and AI
Respects robots.txt, filters by word count, stores JSONL output.
"""
 
import json
import os
import time
from datetime import datetime, timezone
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser
 
import trafilatura
 
 
# ── Configuration ──────────────────────────────────────────────
MIN_WORDS = 500
USER_AGENT = "EduAI-ResearchBot/1.0 (university project)"
OUTPUT_DIR = "data/raw"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "edtech_corpus.jsonl")
 
 
# ── Seed URLs (Education & AI domain, 10 sources) ─────────────
SEED_URLS = [
    # Academic / research
    "https://www.aera.net/Newsroom/Trending-Topic-Research-Files/Trending-Topic-Research-File-Education-Technology-and-Online-Learning",
    "https://pmc.ncbi.nlm.nih.gov/articles/PMC9247945/",
    "https://pmc.ncbi.nlm.nih.gov/articles/PMC8455229/",
    # University / institutional
    "https://education.purdue.edu/news/2024/01/01/how-has-technology-changed-education/",
    "https://www.brookings.edu/articles/how-artificial-intelligence-is-transforming-the-world/",
    "https://www.unesco.org/en/digital-education",
    # Industry / practice
    "https://elearningindustry.com/the-use-of-technology-in-online-education",
    "https://www.edweek.org/technology/what-is-ai-in-education/2024/10",
    # Policy / reports
    "https://www2.ed.gov/documents/ai-report/ai-report.pdf",
    "https://hai.stanford.edu/news/ai-will-transform-teaching-and-learning-lets-get-it-right",
]
 
 
# ── robots.txt checker ─────────────────────────────────────────
def is_allowed_by_robots(url: str) -> bool:
    """
    Check if our user-agent is allowed to crawl the given URL
    according to the site's robots.txt.
    Returns True if allowed or if robots.txt cannot be fetched.
    """
    parsed = urlparse(url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
 
    rp = RobotFileParser()
    rp.set_url(robots_url)
 
    try:
        rp.read()
        allowed = rp.can_fetch(USER_AGENT, url)
        return allowed
    except Exception as e:
        # If we can't read robots.txt, we allow (common practice)
        print(f"  ⚠️  Could not fetch robots.txt for {parsed.netloc}: {e}")
        print(f"      Proceeding cautiously (assuming allowed)")
        return True
 
 
# ── Content extraction ─────────────────────────────────────────
def extract_main_text(url: str) -> str | None:
    """Extract main content from URL using trafilatura."""
    html = trafilatura.fetch_url(url)
    if html is None:
        return None
 
    text = trafilatura.extract(
        html,
        include_comments=False,
        include_tables=False,
    )
    return text
 
 
def is_useful(text: str | None) -> bool:
    """Check if extracted text meets minimum word count."""
    if text is None:
        return False
    return len(text.split()) >= MIN_WORDS
 
 
# ── Main crawl pipeline ───────────────────────────────────────
def crawl_and_save(urls: list[str], output_file: str = OUTPUT_FILE):
    """
    Crawl a list of URLs with robots.txt compliance,
    extract main text, and save useful pages as JSONL.
    """
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
 
    stats = {"total": len(urls), "saved": 0, "blocked": 0, "discarded": 0, "failed": 0}
 
    with open(output_file, "w", encoding="utf-8") as f:
        for i, url in enumerate(urls, 1):
            print(f"\n[{i}/{len(urls)}] {url}")
 
            # Step 1: Check robots.txt
            print(f"  Checking robots.txt...")
            if not is_allowed_by_robots(url):
                print(f"  ❌ BLOCKED by robots.txt — skipping")
                stats["blocked"] += 1
                continue
            print(f"  ✅ Allowed by robots.txt")
 
            # Step 2: Fetch and extract text
            print(f"  Fetching content...")
            text = extract_main_text(url)
 
            if text is None:
                print(f"  ❌ Failed to fetch or extract content")
                stats["failed"] += 1
                continue
 
            # Step 3: Check usefulness
            word_count = len(text.split())
            if not is_useful(text):
                print(f"  ❌ Discarded (only {word_count} words, need {MIN_WORDS}+)")
                stats["discarded"] += 1
                continue
 
            # Step 4: Save record
            parsed = urlparse(url)
            record = {
                "url": url,
                "domain": parsed.netloc,
                "word_count": word_count,
                "crawled_at": datetime.now(timezone.utc).isoformat(),
                "text": text,
            }
            json.dump(record, f, ensure_ascii=False)
            f.write("\n")
 
            stats["saved"] += 1
            print(f"  ✅ Saved ({word_count} words)")
 
            # Polite delay between requests
            time.sleep(1)
 
    return stats
 
 
def main():
    print("=" * 60)
    print("  EDUCATION & AI — PHASE 1: WEB CRAWLING")
    print("=" * 60)
    print(f"  User-Agent: {USER_AGENT}")
    print(f"  Min words:  {MIN_WORDS}")
    print(f"  Seed URLs:  {len(SEED_URLS)}")
    print("=" * 60)
 
    stats = crawl_and_save(SEED_URLS)
 
    print("\n" + "=" * 60)
    print("  CRAWL SUMMARY")
    print("=" * 60)
    print(f"  Total URLs:   {stats['total']}")
    print(f"  Saved:        {stats['saved']}")
    print(f"  Blocked:      {stats['blocked']} (by robots.txt)")
    print(f"  Discarded:    {stats['discarded']} (too short)")
    print(f"  Failed:       {stats['failed']} (fetch error)")
    print(f"  Output:       {OUTPUT_FILE}")
    print("=" * 60)
 
 
if __name__ == "__main__":
    main()