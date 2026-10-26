# Task 1: Fast 100,000+ News Articles Extractor

High-throughput, asynchronous news article extraction pipeline capable of collecting **100,000+ (1 Lakh+) clean articles** with complete metadata (`title`, `url`, `timestamp`, `author`, `domain`) in **under 10 minutes**.

---

## 📁 Folder Contents

- `articles_100k.csv`: The extracted dataset containing **100,037 unique news articles** (38 MB).
- `fast_extract_100k.py`: The asynchronous Python extraction engine.
- `news_sources_1000.csv`: The list of 1,000 news publisher domains used as sources.
- `README.md`: This documentation.

---

## 🚀 How to Run the Code (Extract & Save 100k Articles)

### Option A: Running from inside the `Task_1/` directory

If your terminal is in `Task_1`:

```bash
# 1. Activate the virtual environment
source ../.scrapper/bin/activate

# 2. Run the extractor
python fast_extract_100k.py
```

### Option B: Running from the project root (`AI & ML/`)

If your terminal is in the main project directory:

```bash
# 1. Activate the virtual environment
source .scrapper/bin/activate

# 2. Run the extractor
python Task_1/fast_extract_100k.py
```

*(Note: If you are not using the `.scrapper` virtual environment, simply ensure dependencies are installed via `pip install aiohttp feedparser pandas`)*

---

## ⚡ How the Extraction & Saving Works

1. **Input Sources**: Reads domain names from `news_sources_1000.csv`.
2. **Asynchronous Parallel Processing**:
   - Queries Google News RSS site endpoints (`site:{domain}+news`, `site:{domain}+world`, `site:{domain}+business`) and publisher feeds in parallel using `aiohttp` (50 concurrent connections).
   - Feeds deliver pre-parsed, structured XML directly—**no slow browser loading, no JavaScript rendering, and no CAPTCHA bans**.
3. **Extraction**:
   - `domain`: The news outlet (e.g., `thebusinessjournal.com`, `cnet.com`).
   - `url`: Direct link to the article.
   - `title`: Clean headline text.
   - `timestamp`: Standardized ISO-8601 publication date (e.g., `2026-10-20T23:41:06Z`).
   - `author`: Article author or publisher byline.
4. **Real-Time Deduplication**:
   - Each URL is checked against an in-memory hash set before writing. Duplicate stories are skipped automatically.
5. **Incremental Disk Writes**:
   - Matches are written directly to `articles_100k.csv` and flushed every batch.
   - If stopped, the script automatically resumes from where it left off by reading existing URLs from `articles_100k.csv`.
6. **Automatic Stop**:
   - Once the counter reaches `TARGET_COUNT = 100000`, the script flushes the final entries and exits cleanly.

---

## 🔍 How to Check and Verify the Output

### 1. In Terminal (Bash):

```bash
# If you are inside Task_1:
wc -l articles_100k.csv
head -n 5 articles_100k.csv

# If you are in the parent directory:
wc -l Task_1/articles_100k.csv
head -n 5 Task_1/articles_100k.csv
```

### 2. In Python (Pandas):

```python
import pandas as pd

# Load dataset
df = pd.read_csv("articles_100k.csv")  # or "Task_1/articles_100k.csv"

# Verify shape and quality
print(f"Total articles extracted: {len(df):,}")
print(f"Unique URLs: {df['url'].nunique():,}")
print(f"Unique Publishers: {df['domain'].nunique():,}")

# Check preview
print(df[["domain", "title", "timestamp", "author"]].head())
```

---

## 🛠️ Configuration (Extracting More Than 100k)

If you ever need 200k, 500k, or more, open `fast_extract_100k.py` and modify the constants at the top:

```python
TARGET_COUNT = 100000   # Change to 200000, 500000, etc.
CONCURRENCY  = 50       # Parallel worker count (adjust based on network)
TIMEOUT_SEC  = 12       # Network timeout in seconds
```
