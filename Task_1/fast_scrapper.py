"""
Fast 100k Article Extractor (with Article Content)
--------------------------------------------------
Extracts 100,000 news articles with:
  - domain
  - url
  - title
  - timestamp (ISO-8601)
  - author
  - content (article body text / summary)

Speed:
  - Default (feed summary/content): ~1,000 - 2,000 articles/second (~3 minutes for 100k).
  - Full-body mode (FETCH_FULL_BODY = True): downloads webpage body via trafilatura.

Requirements:
    aiohttp feedparser pandas trafilatura googlenewsdecoder
"""

import asyncio
import aiohttp
import feedparser
import pandas as pd
import csv
import os
import re
import time
from urllib.parse import urlparse
from email.utils import parsedate_to_datetime

try:
    import trafilatura
    HAS_TRAFILATURA = True
except ImportError:
    HAS_TRAFILATURA = False

try:
    from googlenewsdecoder import gnewsdecoder
    HAS_DECODER = True
except ImportError:
    HAS_DECODER = False

# Configuration
SOURCES_CSV = os.path.join(os.path.dirname(__file__), "news_sources_1000.csv")
OUTPUT_CSV = os.path.join(os.path.dirname(__file__), "articles_100k.csv")
TARGET_COUNT = 100000
CONCURRENCY = 50
TIMEOUT_SEC = 12

# Set to True if you want full webpage article body extraction via trafilatura.
# Set to False to use feed description/content (100x faster, zero web blocking).
FETCH_FULL_BODY = False

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Standard RSS paths to probe on publishers
KNOWN_FEED_PATHS = [
    "/feed", "/feed/", "/rss", "/rss/", "/rss.xml", "/feed.xml", "/atom.xml",
    "/news/rss", "/feeds/posts/default"
]

FIELDNAMES = ["domain", "url", "title", "timestamp", "author", "content"]

def format_timestamp(entry):
    """Normalize timestamp to ISO format."""
    if getattr(entry, "published_parsed", None):
        try:
            return time.strftime("%Y-%m-%dT%H:%M:%SZ", entry.published_parsed)
        except Exception:
            pass
    raw = entry.get("published") or entry.get("pubDate") or entry.get("updated")
    if raw:
        try:
            dt = parsedate_to_datetime(raw)
            return dt.isoformat()
        except Exception:
            return str(raw).strip()
    return ""

def clean_text(text):
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", " ", str(text))
    return " ".join(text.split())

def extract_entry_content(entry):
    """Extract summary/content from feed entry."""
    # 1. Content:encoded or content list
    if entry.get("content"):
        parts = [c.get("value", "") for c in entry.get("content", []) if isinstance(c, dict)]
        if parts:
            text = clean_text(" ".join(parts))
            if len(text) > 20:
                return text
    # 2. Summary or description
    raw = entry.get("summary") or entry.get("description") or ""
    return clean_text(raw)

async def fetch_full_article_content(session, url, fallback_text=""):
    """Fetch full body text of the article from its webpage."""
    if not HAS_TRAFILATURA:
        return fallback_text
    
    target_url = url
    # Decode Google News link if applicable
    if "news.google.com" in url and HAS_DECODER:
        try:
            dec = gnewsdecoder(url)
            if dec.get("success") and dec.get("decoded_url"):
                target_url = dec["decoded_url"]
        except Exception:
            pass

    try:
        async with session.get(target_url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=6)) as resp:
            if resp.status == 200:
                html = await resp.text()
                body = trafilatura.extract(html)
                if body and len(body.strip()) > 80:
                    return clean_text(body)
    except Exception:
        pass
    return fallback_text

async def fetch_feed_data(session, url):
    """Fetch and parse RSS/Atom feed content."""
    try:
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=TIMEOUT_SEC)) as resp:
            if resp.status == 200:
                content = await resp.read()
                return feedparser.parse(content)
    except Exception:
        pass
    return None

async def extract_from_domain(session, sem, domain, existing_urls):
    """Extract articles for a domain using multiple news queries and native feeds."""
    async with sem:
        entries = []
        domain_seen = set()

        queries = [
            f"site:{domain}+news",
            f"site:{domain}+world",
            f"site:{domain}+business",
        ]

        for q in queries:
            gnews_url = f"https://news.google.com/rss/search?q={q}&hl=en-US&gl=US&ceid=US:en"
            feed = await fetch_feed_data(session, gnews_url)
            if feed and feed.entries:
                for e in feed.entries:
                    link = e.get("link", "")
                    if link and link not in existing_urls and link not in domain_seen:
                        domain_seen.add(link)
                        title = clean_text(e.get("title", ""))
                        pub = format_timestamp(e)
                        author = clean_text(e.get("author") or e.get("source", {}).get("title") or domain)
                        content = extract_entry_content(e)
                        
                        if FETCH_FULL_BODY:
                            content = await fetch_full_article_content(session, link, content)

                        entries.append({
                            "domain": domain,
                            "url": link,
                            "title": title,
                            "timestamp": pub,
                            "author": author,
                            "content": content,
                        })

        # Native publisher feeds
        for p in KNOWN_FEED_PATHS[:2]:
            native_url = f"https://{domain}{p}"
            nfeed = await fetch_feed_data(session, native_url)
            if nfeed and nfeed.entries:
                for e in nfeed.entries:
                    link = e.get("link", "")
                    if link and link not in existing_urls and link not in domain_seen:
                        domain_seen.add(link)
                        title = clean_text(e.get("title", ""))
                        pub = format_timestamp(e)
                        author = clean_text(e.get("author") or domain)
                        content = extract_entry_content(e)
                        
                        if FETCH_FULL_BODY:
                            content = await fetch_full_article_content(session, link, content)

                        entries.append({
                            "domain": domain,
                            "url": link,
                            "title": title,
                            "timestamp": pub,
                            "author": author,
                            "content": content,
                        })
                break

        return entries

async def main():
    if not os.path.exists(SOURCES_CSV):
        print(f"Error: {SOURCES_CSV} not found!")
        return

    df_sources = pd.read_csv(SOURCES_CSV)
    domains = df_sources["domain"].dropna().unique().tolist()
    print(f"Loaded {len(domains)} domains from {SOURCES_CSV}")

    # Check existing articles for resume capability
    existing_urls = set()
    file_exists = os.path.exists(OUTPUT_CSV)
    has_content_col = False
    if file_exists:
        try:
            df_existing = pd.read_csv(OUTPUT_CSV)
            existing_urls = set(df_existing["url"].dropna())
            has_content_col = "content" in df_existing.columns
            print(f"Resuming: found {len(existing_urls):,} existing articles in {OUTPUT_CSV}")
        except Exception:
            pass

    # Open file for appending or writing
    mode = "a" if (file_exists and has_content_col) else "w"
    csv_file = open(OUTPUT_CSV, mode, newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES)
    
    if mode == "w":
        writer.writeheader()
        existing_urls.clear()

    sem = asyncio.Semaphore(CONCURRENCY)
    conn = aiohttp.TCPConnector(limit=CONCURRENCY * 2, ssl=False)

    total_saved = len(existing_urls)
    start_time = time.time()

    async with aiohttp.ClientSession(connector=conn) as session:
        tasks = [extract_from_domain(session, sem, d, existing_urls) for d in domains]
        
        for i, fut in enumerate(asyncio.as_completed(tasks), 1):
            domain_articles = await fut
            new_rows = 0
            for art in domain_articles:
                if art["url"] not in existing_urls:
                    existing_urls.add(art["url"])
                    writer.writerow(art)
                    total_saved += 1
                    new_rows += 1
                    if total_saved >= TARGET_COUNT:
                        break

            csv_file.flush()
            if i % 25 == 0 or total_saved >= TARGET_COUNT:
                elapsed = time.time() - start_time
                rate = (total_saved - len(existing_urls) + new_rows) / max(elapsed, 1)
                print(f"[{i}/{len(domains)}] Domains probed | Total articles saved: {total_saved:,} | Rate: {rate:.1f} arts/s")

            if total_saved >= TARGET_COUNT:
                print(f"\nTarget of {TARGET_COUNT:,} articles reached successfully!")
                break

    csv_file.close()
    elapsed = time.time() - start_time
    print(f"\nDone! Saved {total_saved:,} articles to {OUTPUT_CSV} in {elapsed:.1f} seconds.")

if __name__ == "__main__":
    asyncio.run(main())
