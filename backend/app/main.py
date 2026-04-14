import asyncio
import gc
import json
import os
import time
from pathlib import Path
from typing import List, Set

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .models import (
    CrawlControlRequest,
    CrawlJobResponse,
    CrawlStartRequest,
    ExtractRequest,
    ExtractResponse,
    ExtractionMetadata,
    MergeJsonRequest,
    MergeJsonResponse,
    StructuredMedicalData,
)
from .services.cleaner import clean_blocks
from .services.crawler import CrawlJobManager, run_crawl_job, save_job_state
from .services.deduplicator import deduplicate_records
from .services.exceptions import ExtractionError
from .services.file_extractor import extract_file
from .services.filters import filter_medical_blocks
from .services.language import detect_language
from .services.scraper import scrape_url
from .services.structurer import structure_medical_data
from .services.structurer import has_minimum_required_fields
from .services.uploads import decode_uploaded_file


ROOT_DIR = Path(__file__).resolve().parents[2]
FRONTEND_DIR = ROOT_DIR / "frontend"
CRAWL_EXPORT_DIR = ROOT_DIR / "data" / "crawl_jobs"
CRAWL_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
CRAWL_JOBS = CrawlJobManager(CRAWL_EXPORT_DIR)
ACTIVE_CRAWL_TASKS: Set[asyncio.Task] = set()

try:
    import psutil
except Exception:  # pragma: no cover - optional dependency fallback.
    psutil = None


METRICS_STATE = {
    "last_wall_time": time.time(),
    "last_process_cpu": time.process_time(),
}

app = FastAPI(
    title="Medical Data Extractor Platform",
    description="Extracts, cleans, filters, and structures trusted medical text for RAG datasets.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")


def schedule_crawl_task(job, *, resume: bool = False) -> None:
    task = asyncio.create_task(run_crawl_job(job, resume=resume))
    ACTIVE_CRAWL_TASKS.add(task)

    def _cleanup(done_task: asyncio.Task) -> None:
        ACTIVE_CRAWL_TASKS.discard(done_task)

    task.add_done_callback(_cleanup)


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/api/health")
async def health() -> dict:
    return {"status": "ok", "service": "medical-data-extractor-platform"}


@app.get("/api/system/metrics")
async def system_metrics() -> dict:
    now = time.time()
    process_cpu = time.process_time()
    elapsed_wall = max(0.001, now - float(METRICS_STATE["last_wall_time"]))
    elapsed_cpu = max(0.0, process_cpu - float(METRICS_STATE["last_process_cpu"]))
    approx_process_cpu_percent = min(100.0, (elapsed_cpu / elapsed_wall) * 100.0)

    METRICS_STATE["last_wall_time"] = now
    METRICS_STATE["last_process_cpu"] = process_cpu

    cpu_percent = None
    memory_percent = None
    memory_used_mb = None
    memory_total_mb = None
    process_memory_mb = None
    process_threads = None
    load_avg = None

    if psutil is not None:
        try:
            vm = psutil.virtual_memory()
            proc = psutil.Process(os.getpid())
            cpu_percent = psutil.cpu_percent(interval=None)
            memory_percent = vm.percent
            memory_used_mb = round(vm.used / (1024 * 1024), 2)
            memory_total_mb = round(vm.total / (1024 * 1024), 2)
            process_memory_mb = round(proc.memory_info().rss / (1024 * 1024), 2)
            process_threads = proc.num_threads()
        except Exception:
            pass

    if load_avg is None and hasattr(os, "getloadavg"):
        try:
            load_avg_tuple = os.getloadavg()
            load_avg = {
                "1m": round(load_avg_tuple[0], 2),
                "5m": round(load_avg_tuple[1], 2),
                "15m": round(load_avg_tuple[2], 2),
            }
        except OSError:
            load_avg = None

    if cpu_percent is None:
        cpu_percent = round(approx_process_cpu_percent, 2)

    return {
        "timestamp": int(now * 1000),
        "cpu_percent": round(float(cpu_percent), 2),
        "memory_percent": memory_percent,
        "memory_used_mb": memory_used_mb,
        "memory_total_mb": memory_total_mb,
        "process_memory_mb": process_memory_mb,
        "process_threads": process_threads,
        "load_average": load_avg,
        "psutil_available": psutil is not None,
    }


@app.get("/favicon.ico", include_in_schema=False)
async def favicon() -> Response:
    return Response(status_code=204)


@app.on_event("startup")
async def resume_incomplete_crawls() -> None:
    auto_resume = os.getenv("AUTO_RESUME_CRAWLS", "0").strip().lower() in {"1", "true", "yes", "on"}
    if not auto_resume:
        return

    for job in CRAWL_JOBS.pending_jobs():
        schedule_crawl_task(job, resume=True)


def parse_uploaded_json_records(filename: str, content: bytes) -> List[StructuredMedicalData]:
    if not filename.lower().endswith(".json"):
        raise ExtractionError("Only JSON files are supported for dataset merging.")

    try:
        payload = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ExtractionError(f"{filename} is not valid UTF-8 JSON.") from exc

    if isinstance(payload, list):
        items = payload
    elif isinstance(payload, dict) and isinstance(payload.get("records"), list):
        items = payload["records"]
    else:
        raise ExtractionError(
            f"{filename} must contain a JSON array of records or an object with a records array."
        )

    records: List[StructuredMedicalData] = []
    for item in items:
        if not isinstance(item, dict):
            continue
        records.append(StructuredMedicalData.model_validate(item))

    return records


@app.post("/api/extract", response_model=ExtractResponse)
async def extract(payload: ExtractRequest) -> ExtractResponse:
    if not payload.url and not payload.file:
        raise HTTPException(status_code=400, detail="Provide a URL or upload a file.")

    blocks: List[str] = []
    tags: List[str] = []
    messages: List[str] = []
    source_titles: List[str] = []
    source_names: List[str] = []
    source_url = ""
    verified = False
    verified_from = ""
    source_types: List[str] = []

    try:
        if payload.url:
            scrape_result = await scrape_url(payload.url)
            blocks.extend(scrape_result.blocks)
            tags.extend(scrape_result.tags)
            source_titles.append(scrape_result.title)
            source_names.append(scrape_result.source_name)
            source_url = scrape_result.source_url
            verified = verified or scrape_result.verified
            if scrape_result.verified_from:
                verified_from = scrape_result.verified_from
            source_types.append("url")

        if payload.file:
            content = decode_uploaded_file(payload.file.content_base64)
            file_result = extract_file(payload.file.filename, content)
            blocks.extend(file_result.blocks)
            source_titles.append(file_result.title)
            source_names.append(file_result.source_name)
            source_types.append("file")
            messages.append("Uploaded files are treated as unverified unless paired with a trusted URL.")

        cleaned_blocks = clean_blocks(blocks)
        accepted_blocks, rejected_blocks, quality_score = filter_medical_blocks(cleaned_blocks)

        if not accepted_blocks:
            raise ExtractionError(
                "Content was rejected because it did not contain required medical keywords: symptoms, treatment, prevention, or cause."
            )

        combined_text = " ".join(accepted_blocks)
        language = detect_language(combined_text)
        data = structure_medical_data(
            accepted_blocks,
            source_title=" + ".join(source_titles),
            source_name=" + ".join(source_names),
            source_url=source_url,
            verified=verified,
            verified_from=verified_from,
        )

        if not has_minimum_required_fields(data):
            raise ExtractionError(
                "Structured output is incomplete. Required fields must be non-empty: "
                "title, category, symptoms, description, warning_signs, when_to_seek_doctor, prevention."
            )

        metadata = ExtractionMetadata(
            source_type="+".join(source_types),
            language=language,
            tags=sorted(set(tags)),
            accepted_blocks=len(accepted_blocks),
            rejected_blocks=len(rejected_blocks),
            quality_score=quality_score,
            messages=messages,
        )

        return ExtractResponse(data=data, raw_blocks=accepted_blocks, metadata=metadata)
    except ExtractionError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post("/api/merge-json", response_model=MergeJsonResponse)
async def merge_json_datasets(payload: MergeJsonRequest) -> MergeJsonResponse:
    if not payload.files:
        raise HTTPException(status_code=400, detail="Provide one or more JSON files.")

    all_records: List[StructuredMedicalData] = []
    messages: List[str] = []

    for file_payload in payload.files:
        content = decode_uploaded_file(file_payload.content_base64)
        records = parse_uploaded_json_records(file_payload.filename, content)
        all_records.extend(records)

    unique_records, duplicate_count = deduplicate_records(all_records)
    messages.append(
        f"Merged {len(all_records)} records from {len(payload.files)} files into {len(unique_records)} unique records."
    )

    return MergeJsonResponse(
        merged_count=len(unique_records),
        duplicate_count=duplicate_count,
        source_file_count=len(payload.files),
        records=unique_records,
        messages=messages,
    )


@app.post("/api/crawl", response_model=CrawlJobResponse)
async def start_crawl(payload: CrawlStartRequest) -> CrawlJobResponse:
    start_urls = [url.strip() for url in payload.urls if url.strip()]
    if payload.url and payload.url.strip():
        start_urls.insert(0, payload.url.strip())

    try:
        job = CRAWL_JOBS.create_job(
            start_urls=start_urls,
            max_pages=payload.max_pages,
            max_depth=payload.max_depth,
            include_sitemap=payload.include_sitemap,
            concurrency=payload.concurrency,
        )
    except ExtractionError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    schedule_crawl_task(job)
    return job.to_response()


@app.post("/api/crawl/reset")
async def reset_crawl_platform() -> dict:
    for job in list(CRAWL_JOBS.jobs.values()):
        job.cancel_requested = True
        job.pause_requested = False

    tasks = list(ACTIVE_CRAWL_TASKS)
    for task in tasks:
        task.cancel()

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

    cancelled_tasks = len(tasks)
    cleared_jobs = CRAWL_JOBS.reset_all_jobs(remove_exports=True)
    gc.collect()

    return {
        "status": "ok",
        "message": "All crawl jobs were reset and runtime memory was cleared.",
        "cleared_jobs": cleared_jobs,
        "cancelled_tasks": cancelled_tasks,
    }


@app.get("/api/crawl/{job_id}", response_model=CrawlJobResponse)
async def get_crawl_status(job_id: str) -> CrawlJobResponse:
    job = CRAWL_JOBS.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Crawl job was not found.")

    return job.to_response()


@app.post("/api/crawl/{job_id}/control", response_model=CrawlJobResponse)
async def control_crawl(job_id: str, payload: CrawlControlRequest) -> CrawlJobResponse:
    job = CRAWL_JOBS.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Crawl job was not found.")

    if job.status in {"completed", "failed", "cancelled"}:
        return job.to_response()

    if payload.action == "pause":
        if job.status == "paused":
            return job.to_response()
        job.pause_requested = True
        job.status = "paused"
        job.status_message = "Pause requested. Finishing active pages..."
        save_job_state(job)
        return job.to_response()

    if payload.action == "cancel":
        job.cancel_requested = True
        job.pause_requested = False
        job.status = "cancelled"
        job.status_message = "Cancel requested. Stopping crawl..."
        save_job_state(job)
        return job.to_response()

    if job.status == "running" and not job.pause_requested:
        return job.to_response()

    job.pause_requested = False
    job.cancel_requested = False
    job.status = "running"
    job.status_message = "Resuming crawl..."
    save_job_state(job)
    schedule_crawl_task(job, resume=True)
    return job.to_response()


@app.get("/api/crawl/{job_id}/download/{format_name}")
async def download_crawl_dataset(job_id: str, format_name: str) -> FileResponse:
    job = CRAWL_JOBS.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Crawl job was not found.")
    if job.status != "completed":
        raise HTTPException(status_code=409, detail="Crawl job is not complete yet.")
    if format_name not in job.export_paths:
        raise HTTPException(status_code=404, detail="Unsupported export format.")

    media_types = {
        "json": "application/json",
        "jsonl": "application/x-ndjson",
        "csv": "text/csv",
    }
    return FileResponse(
        job.export_paths[format_name],
        media_type=media_types[format_name],
        filename=f"medical-crawl-{job_id}.{format_name}",
    )
