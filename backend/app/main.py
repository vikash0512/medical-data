import asyncio
from pathlib import Path
from typing import List

from fastapi import FastAPI, HTTPException, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .models import (
    CrawlJobResponse,
    CrawlStartRequest,
    ExtractRequest,
    ExtractResponse,
    ExtractionMetadata,
)
from .services.cleaner import clean_blocks
from .services.crawler import CrawlJobManager, run_crawl_job
from .services.exceptions import ExtractionError
from .services.file_extractor import extract_file
from .services.filters import filter_medical_blocks
from .services.language import detect_language
from .services.scraper import scrape_url
from .services.structurer import structure_medical_data
from .services.uploads import decode_uploaded_file


ROOT_DIR = Path(__file__).resolve().parents[2]
FRONTEND_DIR = ROOT_DIR / "frontend"
CRAWL_EXPORT_DIR = ROOT_DIR / "data" / "crawl_jobs"
CRAWL_EXPORT_DIR.mkdir(parents=True, exist_ok=True)
CRAWL_JOBS = CrawlJobManager(CRAWL_EXPORT_DIR)

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


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(FRONTEND_DIR / "index.html")


@app.get("/api/health")
async def health() -> dict:
    return {"status": "ok", "service": "medical-data-extractor-platform"}


@app.get("/favicon.ico", include_in_schema=False)
async def favicon() -> Response:
    return Response(status_code=204)


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


@app.post("/api/crawl", response_model=CrawlJobResponse)
async def start_crawl(payload: CrawlStartRequest) -> CrawlJobResponse:
    try:
        job = CRAWL_JOBS.create_job(
            start_url=payload.url,
            max_pages=payload.max_pages,
            max_depth=payload.max_depth,
            include_sitemap=payload.include_sitemap,
            concurrency=payload.concurrency,
        )
    except ExtractionError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    asyncio.create_task(run_crawl_job(job))
    return job.to_response()


@app.get("/api/crawl/{job_id}", response_model=CrawlJobResponse)
async def get_crawl_status(job_id: str) -> CrawlJobResponse:
    job = CRAWL_JOBS.get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Crawl job was not found.")

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
