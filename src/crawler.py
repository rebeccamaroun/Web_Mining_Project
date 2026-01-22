import json
import time
from pathlib import Path
from urllib import robotparser
from urllib.parse import urlparse

import requests
import trafilatura

ROBOTS_CACHE = {}


def load_seeds(path: str = "data/seeds.txt") -> list[str]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Seeds file not found: {path}")
    urls = []
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#"):
            urls.append(line)
    return urls


def can_fetch(url: str, user_agent: str = "Mozilla/5.0") -> bool:
    netloc = urlparse(url).netloc.lower()
    if netloc.endswith("wikipedia.org"):
        return True

    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    robots_url = f"{base}/robots.txt"

    if base not in ROBOTS_CACHE:
        rp = robotparser.RobotFileParser()
        rp.set_url(robots_url)
        try:
            rp.read()
        except Exception:
            # If robots.txt can't be fetched, allow by default.
            return True
        ROBOTS_CACHE[base] = rp

    return ROBOTS_CACHE[base].can_fetch(user_agent, url)


def fetch_html(url: str, timeout: int = 20, user_agent: str = "Mozilla/5.0") -> str | None:
    headers = {"User-Agent": user_agent}
    try:
        r = requests.get(url, headers=headers, timeout=timeout)
        if r.status_code != 200:
            return None
        ctype = r.headers.get("Content-Type", "")
        if "text/html" not in ctype and "application/xhtml+xml" not in ctype:
            return None
        return r.text
    except requests.RequestException:
        return None


def extract_main_text(html: str, url: str) -> str | None:
    text = trafilatura.extract(
        html,
        url=url,
        include_comments=False,
        include_tables=False,
        favor_recall=False,
    )
    if not text:
        return None
    return " ".join(text.split())


def is_useful(text: str, min_words: int = 500) -> bool:
    return bool(text) and len(text.split()) >= min_words


def append_jsonl(path: str, record: dict) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def main(
    seeds_path: str = "data/seeds.txt",
    out_jsonl: str = "data/raw/pages.jsonl",
    min_words: int = 500,
    delay_s: float = 1.0,
    user_agent: str = "Mozilla/5.0",
):
    seeds = load_seeds(seeds_path)
    kept = 0
    skipped = 0

    print(f"Loaded {len(seeds)} seed URLs from {seeds_path}")

    for url in seeds:
        print(f"\n→ {url}")

        if not can_fetch(url, user_agent=user_agent):
            print("  SKIP: blocked by robots.txt")
            skipped += 1
            continue

        html = fetch_html(url, user_agent=user_agent)
        if not html:
            print("  SKIP: fetch failed / not HTML")
            skipped += 1
            continue

        text = extract_main_text(html, url=url)
        if not text:
            print("  SKIP: extraction failed")
            skipped += 1
            continue

        wc = len(text.split())
        if not is_useful(text, min_words=min_words):
            print(f"  SKIP: too short ({wc} words < {min_words})")
            skipped += 1
            continue

        record = {
            "url": url,
            "domain": urlparse(url).netloc,
            "word_count": wc,
            "text": text,
        }
        append_jsonl(out_jsonl, record)
        kept += 1
        print(f"  OK: saved ({wc} words)")

        time.sleep(delay_s)

    print(f"\nDONE ✅ kept={kept} skipped={skipped}")
    print(f"Output: {out_jsonl}")


if __name__ == "__main__":
    main()
