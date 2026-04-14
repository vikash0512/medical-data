# Medical Data Extractor Platform - Complete User Guide

This guide explains your platform end-to-end: what it does, how to run it, what each crawler setting means, and how to use Single File Extract.

## 1. What This Platform Does

Medical Data Extractor Platform helps you build healthcare-focused datasets for RAG systems.

Main workflow:
1. You provide a trusted source website (for example WHO, CDC, NIH, NHS, government health portals).
2. The crawler discovers pages from that site.
3. It extracts paragraph-level content.
4. It cleans and filters content to keep medical relevance.
5. It structures content into a medical schema.
6. It exports the dataset in JSON, JSONL, and CSV formats.

## 2. Core Features

1. Website crawler with same-site link discovery.
2. Sitemap and robots.txt sitemap support.
3. Structured output fields for medical use-cases.
4. Language detection (basic English/Hindi routing).
5. Source verification tags (WHO/Government domain detection).
6. Downloadable datasets in multiple formats.
7. Single File Extract for PDF/TXT/DOCX ingestion.
8. Strict completeness filtering for crawl records.
9. JSON dataset merge and deduplication across multiple files.

## 3. Data Schema Produced

Each accepted record is structured like this:

```json
{
  "title": "",
  "category": "disease",

  "symptoms": [],
  "common_symptoms": [],
  "rare_symptoms": [],

  "description": "",

  "differential_questions": [
    "Do you have high fever (>102°F)?",
    "Are you experiencing body pain?",
    "Since how many days do you have symptoms?"
  ],

  "severity_levels": {
    "mild": {
      "conditions": [],
      "advice": []
    },
    "moderate": {
      "conditions": [],
      "advice": []
    },
    "severe": {
      "conditions": [],
      "advice": []
    }
  },

  "home_care": [],
  "lifestyle_tips": [],

  "warning_signs": [],
  "when_to_seek_doctor": "",

  "prevention": [],

  "risk_groups": [
    "children",
    "pregnant women",
    "elderly"
  ],

  "possible_confusions": [
    "Common cold",
    "Flu",
    "COVID-19"
  ],

  "confidence_rules": {
    "min_symptoms_match": 2,
    "high_confidence_threshold": 0.7
  },

  "source": "",
  "verified": true,
  "source_url": ""
}
```

## 4. Record Acceptance Rules (Important)

Crawler output is strict by design to improve RAG quality.

A record is accepted only when these required sections are non-empty:
1. `title`
2. `category`
3. `symptoms`
4. `description`
5. `warning_signs`
6. `when_to_seek_doctor`
7. `prevention`

If major sections are empty, that page is rejected as low-value/partial data.

## 5. Crawler Settings Explained

### 5.1 Website Link
The starting URL/domain to crawl, for example:
- `who.int`
- `https://www.cdc.gov`

### 5.2 Max Pages
Maximum number of pages the crawler will queue/scrape in one job.

Guidance:
1. Start small (`50-200`) to verify quality.
2. Increase gradually for large dataset generation.

### 5.3 Link Depth
How far link traversal is allowed from the start page.

Depth meaning:
1. `0`: only the starting page.
2. `1`: start page + links found on it.
3. `2`: links from depth-1 pages too.
4. Higher depth: broader crawl, more time, more irrelevant risk.

Recommended start: `1`.

### 5.4 Workers
Number of concurrent crawler workers (parallel requests).

Guidance:
1. Higher workers = faster crawl.
2. Higher workers also increase risk of website rate-limits (HTTP 429).
3. For strict sites (for example WHO), use `1`.

### 5.5 Include Sitemap
If enabled, crawler also uses:
1. `/sitemap.xml`
2. Sitemap entries from `robots.txt`

This helps discover important pages faster.

## 6. Why WHO Can Sometimes Fail

WHO may return HTTP 429 (rate-limit) from Cloudflare depending on IP/network conditions.

If this happens:
1. Reduce workers to `1`.
2. Keep depth low (`1`).
3. Retry after the `Retry-After` window.
4. Use other trusted sources in parallel (CDC/NIH/NHS) or file upload while waiting.

## 7. Single File Extract (What It Is)

Single File Extract lets you process one local file directly without website crawling.

Supported file types:
1. `.pdf`
2. `.txt`
3. `.docx`

Use cases:
1. Import official health reports/PDFs.
2. Build records from documents when websites are blocked/rate-limited.
3. Add curated documents to your RAG pipeline quickly.

How it behaves:
1. Reads file text.
2. Cleans and filters text.
3. Structures into the same medical schema.
4. Shows result in preview.

## 8. Merge JSON Datasets

Use this when you already have multiple JSON exports from different websites or platforms and want one deduplicated dataset.

How it behaves:
1. Upload multiple `.json` files.
2. The platform reads each file and validates the records.
3. It removes duplicates using a stable record fingerprint.
4. It returns one merged JSON dataset.

What counts as a duplicate:
1. Same title and source URL.
2. Same description and medical content.
3. Same symptom/prevention/warning sections after normalization.

Best practice:
1. Merge files from the same schema version.
2. Prefer JSON arrays of records.
3. Review the merged preview before using it in RAG indexing.

## 9. Export Formats

### JSON
Clean list of structured records (best for app consumption).

### JSONL
Line-by-line JSON payload including raw accepted blocks (best for indexing pipelines and audits).

### CSV
Spreadsheet-friendly export for review/manual QA.

## 10. How To Run the Platform

Run from project root:

```bash
cd /Users/vikashkumar/Desktop/capestone
```

Create virtual environment (first time):

```bash
python3 -m venv .venv
```

Activate environment:

```bash
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r backend/requirements.txt
```

Start server:

```bash
uvicorn backend.app.main:app --host 127.0.0.1 --port 8000 --reload
```

Open in browser:

```text
http://127.0.0.1:8000
```

If port 8000 is in use:

```bash
uvicorn backend.app.main:app --host 127.0.0.1 --port 8001 --reload
```

Then open:

```text
http://127.0.0.1:8001
```

## 11. Suggested Safe Crawl Strategy

For best quality and stability:
1. Start with `max_pages=50`, `depth=1`, `workers=1`.
2. Check preview and rejected/accepted counts.
3. Increase max pages only after quality check.
4. Avoid high concurrency on strict websites.
5. Keep only trusted medical domains for production datasets.

## 12. Quick Troubleshooting

### Server not opening
1. Check server command is running in terminal.
2. Verify health endpoint:
   - `curl http://127.0.0.1:8000/api/health`

### Crawl failed with no data
1. Check Recent Crawl Notes in UI.
2. If 429/rate-limit, wait and retry with lower workers.
3. Lower depth and pages for initial runs.

### Too much irrelevant data
1. Keep depth low.
2. Use trusted domains.
3. Review accepted/rejected records before large runs.

---

If you want, this guide can be split into:
1. `USER_GUIDE.md` (non-technical users)
2. `DEVELOPER_GUIDE.md` (technical setup and architecture)
