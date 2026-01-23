"""
Phase 1: Web Crawling & Cleaning
Domain: Education and AI
Extracted from teammate's Colab notebook
"""

import json
import trafilatura


def is_useful(text, min_words=500):
    """Check if extracted text meets minimum word count"""
    if text is None:
        return False
    return len(text.split()) >= min_words


def extract_main_text(url):
    """Extract main content from URL using trafilatura"""
    html = trafilatura.fetch_url(url)
    if html is None:
        return None

    text = trafilatura.extract(
        html,
        include_comments=False,
        include_tables=False
    )
    return text


def crawl_and_save(urls, output_file="data/raw/edtech_corpus.jsonl"):
    """Crawl URLs and save to JSONL"""
    with open(output_file, "w", encoding="utf-8") as f:
        for url in urls:
            print(f"\n🔍 Processing: {url}")
            text = extract_main_text(url)

            if is_useful(text):
                record = {
                    "url": url,
                    "word_count": len(text.split()),
                    "text": text
                }
                json.dump(record, f, ensure_ascii=False)
                f.write("\n")
                print(f"✅ Saved: {url} ({record['word_count']} words)")
            else:
                wc = len(text.split()) if text else 0
                print(f"❌ Discarded (too short or failed): {url} ({wc} words)")


def main():
    """Main function to run the crawler"""
    # Education & AI domain URLs
    urls = [
        "https://www.aera.net/Newsroom/Trending-Topic-Research-Files/Trending-Topic-Research-File-Education-Technology-and-Online-Learning",
        "https://elearningindustry.com/the-use-of-technology-in-online-education",
        "https://education.purdue.edu/news/2024/01/01/how-has-technology-changed-education/",
        "https://pmc.ncbi.nlm.nih.gov/articles/PMC9247945/"
    ]
    
    print("="*60)
    print("📚 EDUCATION & AI DOMAIN - PHASE 1: WEB CRAWLING")
    print("="*60)
    
    crawl_and_save(urls)
    
    print("\n" + "="*60)
    print("✅ Phase 1 Complete!")
    print("Output: data/raw/edtech_corpus.jsonl")
    print("="*60)


if __name__ == "__main__":
    main()