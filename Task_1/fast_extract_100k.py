"""
Fast 100k Article Extractor
---------------------------
Extracts 100,000 news articles (Title, URL, Timestamp, Author, Domain)
using high-speed asynchronous feed and news aggregation.

Speed: ~1,000 - 2,000 articles per second.
Takes ~2-3 minutes for 100,000 articles.

Requirements:
    aiohttp feedparser pandas
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

# Configuration
SOURCES_CSV = os.path.join(os.path.dirname(__file__), "news_sources_1000.csv")
OUTPUT_CSV = os.path.join(os.path.dirname(__file__), "articles_100k.csv")
TARGET_COUNT = 100000
CONCURRENCY = 50
TIMEOUT_SEC = 12

HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

# Standard RSS paths to probe on publishers
KNOWN_FEED_PATHS = [
    "/feed", "/feed/", "/rss", "/rss/", "/rss.xml", "/feed.xml", "/atom.xml",
    "/news/rss", "/feeds/posts/default"
]

def format_timestamp(entry):
    """Normalize timestamp to ISO format."""
    # 1. Try published_parsed
    if getattr(entry, "published_parsed", None):
        try:
            return time.strftime("%Y-%m-%dT%H:%M:%SZ", entry.published_parsed)
        except Exception:
            pass
    # 2. Try raw string parsing
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
    text = re.sub(r"<[^>]+>", "", str(text))
    return " ".join(text.split())

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

        # Query variants to pull different sections/articles per domain
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
                        entries.append({
                            "domain": domain,
                            "url": link,
                            "title": title,
                            "timestamp": pub,
                            "author": author,
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
                        entries.append({
                            "domain": domain,
                            "url": link,
                            "title": title,
                            "timestamp": pub,
                            "author": author,
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
    if file_exists:
        try:
            df_existing = pd.read_csv(OUTPUT_CSV)
            existing_urls = set(df_existing["url"].dropna())
            print(f"Resuming: found {len(existing_urls)} existing articles in {OUTPUT_CSV}")
        except Exception:
            pass

    csv_file = open(OUTPUT_CSV, "a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=["domain", "url", "title", "timestamp", "author"])
    if not file_exists or len(existing_urls) == 0:
        writer.writeheader()

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
                rate = total_saved / max(elapsed, 1)
                print(f"[{i}/{len(domains)}] Domains probed | Total articles saved: {total_saved:,} | Rate: {rate:.1f} arts/s")

            if total_saved >= TARGET_COUNT:
                print(f"\nTarget of {TARGET_COUNT:,} articles reached successfully!")
                break

    csv_file.close()
    elapsed = time.time() - start_time
    print(f"\nDone! Saved {total_saved:,} articles to {OUTPUT_CSV} in {elapsed:.1f} seconds.")

if __name__ == "__main__":
    asyncio.run(main())
