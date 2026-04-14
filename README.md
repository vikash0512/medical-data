# Medical Data Extractor Platform

A full-stack FastAPI application for building large medical RAG datasets from trusted health websites and uploaded medical files.

The main workflow is now a website crawler: paste one or more trusted platform URLs such as `who.int` and `medlineplus.gov`, start a crawl, watch progress, preview structured records, and export the full dataset as JSON, JSONL, or CSV.

## Features

- Domain crawler for trusted health websites.
- Sitemap and `robots.txt` sitemap discovery.
- Multi-site crawling from a list of platforms with configurable page limit, depth, and worker count.
- Per-platform progress bars for each entered website or platform.
- Live performance monitor in the UI showing CPU, RAM, process memory, threads, and load average.
- Background crawl jobs with status polling and on-disk checkpoints.
- Pause, resume, and cancel controls for active crawls.
- Terminal-style recent crawl notes in the UI for live progress and warnings.
- Graceful completion when some platforms have no pages or no accepted records.
- URL page extraction with HTTP fetching and BeautifulSoup parsing.
- File extraction for PDF, TXT, and DOCX sources.
- Cleaning rules for short text, duplicates, and whitespace normalization.
- Filtering that keeps only content containing medical keywords such as symptoms, treatment, prevention, and cause.
- Keyword-based structuring into:

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
  "verified_from": "WHO",
  "source_url": ""
}
```

- WHO and government source tagging.
- Lightweight Hindi/English language detection.
- JSONL export includes raw accepted text blocks for RAG indexing and audit review.

## Project Structure

```text
backend/
  app/
    main.py
    models.py
    services/
      crawler.py
      scraper.py
      file_extractor.py
      cleaner.py
      filters.py
      language.py
      structurer.py
  tests/
frontend/
  index.html
  styles.css
  app.js
```

## Setup

```bash
cd /Users/vikashkumar/Desktop/capestone
python3 -m venv .venv
source .venv/bin/activate
pip install -r backend/requirements.txt
uvicorn backend.app.main:app --reload
```

Open `http://127.0.0.1:8000`.

## Website Crawl API

Start a background crawl:

```http
POST /api/crawl
```

```json
{
  "urls": ["who.int", "medlineplus.gov"],
  "max_pages": 1000,
  "max_depth": 3,
  "include_sitemap": true,
  "concurrency": 4
}
```

You can still send a single `url` for backward compatibility, but the UI now accepts one platform per line or CSV-style input and distributes crawl workers across the list.

The crawler enforces a hard cap of 3 workers per platform (for example, 3 platforms can use up to 9 workers total).

If one platform has no pages or cannot be fetched, the crawl continues for the other platforms and completes gracefully.

The page limit applies per platform, so the total crawl budget grows as you add more platforms, while the worker count is capped to keep large multi-site crawls stable.

Poll status:

```http
GET /api/crawl/{job_id}
```

Download completed datasets:

```http
GET /api/crawl/{job_id}/download/json
GET /api/crawl/{job_id}/download/jsonl
GET /api/crawl/{job_id}/download/csv
```

## Single Source API

`POST /api/extract`

```json
{
  "url": "https://www.who.int/...",
  "file": {
    "filename": "guidance.txt",
    "content_base64": "..."
  }
}
```

You may provide a URL, a file, or both. Uploaded files are sent as base64 JSON so the app can run without multipart middleware.

## Notes for RAG Dataset Quality

- `verified` is true when the website URL matches a trusted health platform.
- `verified_from` shows the verification source label, such as `WHO`, `MedlinePlus`, `CDC`, or `NHS`.
- Uploaded files are treated as unverified unless paired with a trusted URL.
- The crawler exports structured records plus raw accepted blocks in JSONL for traceable indexing.
- Very large crawl jobs can take time. Increase `max_pages` gradually, then run larger crawls once the filters look right. The UI currently allows up to 50,000 pages per job.
- Crawl checkpoints are written to disk so a page refresh can reconnect to the same job and server restarts can restore incomplete jobs.
- The crawl panel includes a live Performance Monitor to help diagnose slow runs. High CPU, high RAM use, or rising load averages usually indicate system pressure.
- JSON deduplication supports both a single uploaded file and multiple uploaded files.
- Translation is intentionally left as an integration point for an approved medical translation model or API. The current implementation detects Hindi text but does not invent translated medical guidance.

## Tests

```bash
python3 -m unittest discover backend/tests
```
