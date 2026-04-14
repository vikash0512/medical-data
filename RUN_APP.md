# Run Guide - Medical Data Extractor Platform

This guide is written so anyone can run the app on a fresh machine.

## Prerequisites

Install these first:

- Python `3.10+` (recommended: `3.11` or `3.12`)
- `pip` (comes with Python)
- Terminal (macOS/Linux) or PowerShell (Windows)

Quick check:

```bash
python3 --version
```

If that fails on Windows, use:

```powershell
py --version
```

## 1. Open terminal in the project folder

If you already have the project locally:

```bash
cd /path/to/capestone
```

If you are inside the project folder already, skip `cd`.

## 2. Create a virtual environment (first time only)

macOS/Linux:

```bash
python3 -m venv .venv
```

Windows (PowerShell):

```powershell
py -m venv .venv
```

## 3. Activate the virtual environment

macOS/Linux:

```bash
source .venv/bin/activate
```

Windows (PowerShell):

```powershell
.\.venv\Scripts\Activate.ps1
```

You should now see `(.venv)` in your shell prompt.

## 4. Install dependencies

Upgrade packaging tools first (recommended):

```bash
python -m pip install --upgrade pip setuptools wheel
```

Install project requirements:

```bash
python -m pip install -r backend/requirements.txt
```

## 5. Start the app server

```bash
python -m uvicorn backend.app.main:app --host 127.0.0.1 --port 8000
```

## 6. Open app in browser

```text
http://127.0.0.1:8000
```

## If port 8000 is already in use

Run on another port:

```bash
python -m uvicorn backend.app.main:app --host 127.0.0.1 --port 8001
```

Then open:

```text
http://127.0.0.1:8001
```

## Quick troubleshooting (dependency install + startup)

1. `pip install` fails with permission errors

- Make sure virtual environment is activated (`(.venv)` visible).
- Use `python -m pip ...` (do not use `sudo pip ...`).

2. `python3` or `python` command not found

- macOS/Linux: install Python and try again with `python3`.
- Windows: use `py`.

3. `ModuleNotFoundError` when starting app

- You are likely outside venv, or install failed.
- Re-run:

```bash
source .venv/bin/activate
python -m pip install -r backend/requirements.txt
```

4. App exits quickly (example: exit code `137`)

```bash
pkill -f "uvicorn backend.app.main:app" || true
lsof -nP -iTCP:8000 -sTCP:LISTEN
python -m uvicorn backend.app.main:app --host 127.0.0.1 --port 8000
```

Notes:

- Use `--reload` only for development (uses more resources).
- Exit code `137` usually means the process was killed by OS or process manager.

## Optional: run tests

```bash
python -m unittest discover backend/tests
```

## Optional: auto-resume unfinished crawl jobs

```bash
AUTO_RESUME_CRAWLS=1 python -m uvicorn backend.app.main:app --host 127.0.0.1 --port 8000
```

## Optional: low-resource mode (16 GB RAM)

```bash
CRAWL_MANIFEST_URL_SAMPLE_LIMIT=2000 CRAWL_MANIFEST_QUEUE_ITEM_SAMPLE_LIMIT=1000 CRAWL_STATE_WRITE_THROTTLE_SECONDS=1.25 python -m uvicorn backend.app.main:app --host 127.0.0.1 --port 8000
```

This keeps persisted crawl state compact and reduces startup load.

## Optional: one-time cleanup of old heavy crawl manifests

Use only if you do not need to resume old crawls.

```bash
find data/crawl_jobs -name job.json -print0 | xargs -0 sed -i '' -e '/"queue_items"/,/\],/c\
	"queue_items": [],' -e '/"in_progress_items"/,/\],/c\
	"in_progress_items": [],' -e '/"queued_urls"/,/\],/c\
	"queued_urls": [],' -e '/"in_progress_urls"/,/\],/c\
	"in_progress_urls": [],' -e '/"processed_urls"/,/\],/c\
	"processed_urls": []'
```
