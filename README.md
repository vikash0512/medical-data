# Medical Data Extractor Platform

A full-stack FastAPI application for building large medical RAG datasets from trusted health websites and uploaded medical files.

The main workflow is now a website crawler: paste a domain such as `who.int`, start a crawl, watch progress, preview structured records, and export the full dataset as JSON, JSONL, or CSV.

## Features

- Domain crawler for trusted health websites.
- Sitemap and `robots.txt` sitemap discovery.
- Same-site link crawling with configurable page limit, depth, and worker count.
- Background crawl jobs with status polling.
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
  "description": "",
  "home_care": [],
  "warning_signs": [],
  "when_to_seek_doctor": "",
  "prevention": [],
  "source": "",
  "verified": true,
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
  "url": "who.int",
  "max_pages": 1000,
  "max_depth": 3,
  "include_sitemap": true,
  "concurrency": 4
}
```

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

- `verified` is true when the website URL matches WHO or a government health domain.
- Uploaded files are treated as unverified unless paired with a trusted URL.
- The crawler exports structured records plus raw accepted blocks in JSONL for traceable indexing.
- Very large domains can take time. Increase `max_pages` gradually, then run larger crawls once the filters look right. The UI currently allows up to 50,000 pages per job.
- Translation is intentionally left as an integration point for an approved medical translation model or API. The current implementation detects Hindi text but does not invent translated medical guidance.

## Tests

```bash
python3 -m unittest discover backend/tests
```
