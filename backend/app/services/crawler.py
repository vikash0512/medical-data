import asyncio
import csv
import itertools
import json
import os
import shutil
import time
import uuid
import xml.etree.ElementTree as ET
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse

import httpx

from ..models import CrawlJobResponse, CrawlRecord, PlatformProgress
from .cleaner import clean_blocks
from .exceptions import ExtractionError
from .filters import filter_medical_blocks, is_condition_reference_page, is_healthcare_relevant_url
from .language import detect_language
from .scraper import (
    REQUEST_HEADERS,
    classify_source,
    canonicalize_url,
    extract_page_links,
    fetch_html_with_client,
    is_probably_html_url,
    normalize_url,
    parse_html,
)
from .structurer import structure_medical_data
from .structurer import has_minimum_required_fields


EXPORT_FIELDS = (
    "title",
    "category",
    "symptoms",
    "common_symptoms",
    "rare_symptoms",
    "description",
    "differential_questions",
    "severity_levels",
    "home_care",
    "lifestyle_tips",
    "warning_signs",
    "when_to_seek_doctor",
    "prevention",
    "risk_groups",
    "possible_confusions",
    "confidence_rules",
    "source",
    "verified",
    "verified_from",
    "source_url",
)

JOB_MANIFEST_NAME = "job.json"
MAX_WORKERS_PER_PLATFORM = 3
TERMINAL_JOB_STATUSES = {"completed", "failed", "cancelled"}
MANIFEST_URL_SAMPLE_LIMIT = max(100, int(os.getenv("CRAWL_MANIFEST_URL_SAMPLE_LIMIT", "2000")))
MANIFEST_QUEUE_ITEM_SAMPLE_LIMIT = max(
    100, int(os.getenv("CRAWL_MANIFEST_QUEUE_ITEM_SAMPLE_LIMIT", "1000"))
)
STATE_WRITE_THROTTLE_SECONDS = max(
    0.2, float(os.getenv("CRAWL_STATE_WRITE_THROTTLE_SECONDS", "1.25"))
)
SITEMAP_DISCOVERY_TIMEOUT_SECONDS = max(
    3.0, float(os.getenv("CRAWL_SITEMAP_DISCOVERY_TIMEOUT_SECONDS", "8.0"))
)
_JOB_LAST_SAVED_AT: Dict[str, float] = {}


def model_to_dict(model) -> dict:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def root_host(url: str) -> str:
    host = urlparse(url).netloc.lower()
    return host.removeprefix("www.")


def is_same_site(url: str, base_host: str) -> bool:
    host = urlparse(url).netloc.lower().removeprefix("www.")
    return host == base_host or host.endswith(f".{base_host}")


def get_text_from_xml(element: ET.Element) -> str:
    if element.text:
        return element.text.strip()
    return ""


def parse_sitemap_xml(xml_text: str) -> Tuple[List[str], List[str]]:
    urls: List[str] = []
    nested_sitemaps: List[str] = []
    root = ET.fromstring(xml_text)
    root_name = root.tag.split("}")[-1].lower()

    if root_name == "sitemapindex":
        for element in root.iter():
            if element.tag.split("}")[-1].lower() == "loc":
                loc = get_text_from_xml(element)
                if loc:
                    nested_sitemaps.append(loc)
    elif root_name == "urlset":
        for element in root.iter():
            if element.tag.split("}")[-1].lower() == "loc":
                loc = get_text_from_xml(element)
                if loc:
                    urls.append(loc)

    return urls, nested_sitemaps


@dataclass
class CrawlQueueItem:
    url: str
    depth: int
    platform_id: str
    root_host: str
    source_name: str
    verified: bool
    verified_from: str
    tags: List[str] = field(default_factory=list)


@dataclass
class PlatformState:
    platform_id: str
    label: str
    start_url: str
    root_host: str
    max_pages: int
    discovered_pages: int = 0
    scraped_pages: int = 0
    accepted_pages: int = 0
    rejected_pages: int = 0
    status: str = "queued"
    status_message: str = "Queued."

    def to_response(self) -> PlatformProgress:
        return PlatformProgress(
            platform_id=self.platform_id,
            label=self.label,
            start_url=self.start_url,
            max_pages=self.max_pages,
            discovered_pages=self.discovered_pages,
            scraped_pages=self.scraped_pages,
            accepted_pages=self.accepted_pages,
            rejected_pages=self.rejected_pages,
            status=self.status,
            status_message=self.status_message,
        )


@dataclass
class CrawlJob:
    job_id: str
    start_url: str
    max_pages: int
    max_depth: int
    include_sitemap: bool
    concurrency: int
    export_root: Path
    start_urls: List[str] = field(default_factory=list)
    platform_max_pages: int = 0
    status: str = "queued"
    status_message: str = "Queued."
    discovered_pages: int = 0
    scraped_pages: int = 0
    accepted_pages: int = 0
    rejected_pages: int = 0
    failed_pages: int = 0
    records: List[CrawlRecord] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    export_paths: Dict[str, Path] = field(default_factory=dict)
    queue_items: List[CrawlQueueItem] = field(default_factory=list)
    queued_urls: Set[str] = field(default_factory=set)
    in_progress_items: List[CrawlQueueItem] = field(default_factory=list)
    in_progress_urls: Set[str] = field(default_factory=set)
    processed_urls: Set[str] = field(default_factory=set)
    platform_states: List[PlatformState] = field(default_factory=list)
    pause_requested: bool = False
    cancel_requested: bool = False
    messages: List[str] = field(default_factory=list)

    def to_response(self) -> CrawlJobResponse:
        download_json_url = None
        download_jsonl_url = None
        download_csv_url = None

        if self.status == "completed":
            download_json_url = f"/api/crawl/{self.job_id}/download/json"
            download_jsonl_url = f"/api/crawl/{self.job_id}/download/jsonl"
            download_csv_url = f"/api/crawl/{self.job_id}/download/csv"

        return CrawlJobResponse(
            job_id=self.job_id,
            status=self.status,
            status_message=self.status_message,
            start_url=self.start_url,
            start_urls=self.start_urls,
            max_pages=self.max_pages,
            max_depth=self.max_depth,
            include_sitemap=self.include_sitemap,
            concurrency=self.concurrency,
            discovered_pages=self.discovered_pages,
            scraped_pages=self.scraped_pages,
            accepted_pages=self.accepted_pages,
            rejected_pages=self.rejected_pages,
            failed_pages=self.failed_pages,
            platform_progress=[platform.to_response() for platform in self.platform_states],
            records_preview=self.records[:10],
            errors=self.errors[-20:],
            messages=self.messages[-20:],
            download_json_url=download_json_url,
            download_jsonl_url=download_jsonl_url,
            download_csv_url=download_csv_url,
        )


class CrawlJobManager:
    def __init__(self, export_root: Path) -> None:
        self.export_root = export_root
        self.jobs: Dict[str, CrawlJob] = {}
        self.job_manifests: Dict[str, Path] = {}
        self._index_jobs_from_disk()

    def create_job(
        self,
        *,
        start_urls: List[str],
        max_pages: int,
        max_depth: int,
        include_sitemap: bool,
        concurrency: int,
    ) -> CrawlJob:
        normalized_urls: List[str] = []
        seen_urls: Set[str] = set()
        for url in start_urls:
            if not url.strip():
                continue
            normalized_url = normalize_url(url)
            if normalized_url in seen_urls:
                continue
            seen_urls.add(normalized_url)
            normalized_urls.append(normalized_url)
        if not normalized_urls:
            raise ExtractionError("Provide at least one website link.")

        max_allowed_workers = len(normalized_urls) * MAX_WORKERS_PER_PLATFORM
        effective_concurrency = max(1, min(concurrency, max_allowed_workers))
        total_max_pages = max_pages * len(normalized_urls)

        platform_states = []
        for start_url in normalized_urls:
            source_name, _, _, _ = classify_source(start_url)
            root = root_host(start_url)
            platform_states.append(
                PlatformState(
                    platform_id=start_url,
                    label=source_name,
                    start_url=start_url,
                    root_host=root,
                    max_pages=max_pages,
                )
            )

        job_id = uuid.uuid4().hex[:12]
        job = CrawlJob(
            job_id=job_id,
            start_url=normalized_urls[0],
            start_urls=normalized_urls,
            platform_max_pages=max_pages,
            max_pages=total_max_pages,
            max_depth=max_depth,
            include_sitemap=include_sitemap,
            concurrency=effective_concurrency,
            export_root=self.export_root,
            platform_states=platform_states,
            messages=[
                f"Crawl configured for {len(normalized_urls)} platform(s), {max_pages} pages per platform, and {effective_concurrency} worker(s) with max {MAX_WORKERS_PER_PLATFORM} per platform."
            ],
        )
        self.jobs[job_id] = job
        self.job_manifests[job_id] = job_manifest_path(self.export_root, job_id)
        prepare_exports(job, reset=True)
        save_job_state(job, force=True)
        return job

    def get_job(self, job_id: str) -> Optional[CrawlJob]:
        job = self.jobs.get(job_id)
        if job:
            return job

        manifest_path = self.job_manifests.get(job_id)
        if manifest_path is None:
            manifest_path = job_manifest_path(self.export_root, job_id)
            if manifest_path.exists():
                self.job_manifests[job_id] = manifest_path

        if manifest_path is None or not manifest_path.exists():
            return None

        job = load_job_state(manifest_path, self.export_root)
        self.jobs[job_id] = job
        return job

    def _index_jobs_from_disk(self) -> None:
        if not self.export_root.exists():
            return

        for manifest_path in self.export_root.glob(f"*/{JOB_MANIFEST_NAME}"):
            self.job_manifests[manifest_path.parent.name] = manifest_path

    def _manifest_has_pending_work(self, manifest_path: Path) -> bool:
        try:
            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
        except Exception:
            return False

        status = str(payload.get("status", "queued"))
        if status not in {"queued", "running"}:
            return False

        return bool(payload.get("queue_items") or payload.get("in_progress_urls"))

    def pending_jobs(self) -> List[CrawlJob]:
        pending: List[CrawlJob] = [
            job
            for job in self.jobs.values()
            if job.status in {"queued", "running"} and (job.queue_items or job.in_progress_urls)
        ]

        loaded_ids = {job.job_id for job in pending}
        for job_id, manifest_path in self.job_manifests.items():
            if job_id in loaded_ids:
                continue
            if not self._manifest_has_pending_work(manifest_path):
                continue
            try:
                job = load_job_state(manifest_path, self.export_root)
            except Exception:
                continue
            self.jobs[job_id] = job
            pending.append(job)

        return pending

    def reset_all_jobs(self, *, remove_exports: bool = True) -> int:
        cleared_jobs = len(set([*self.jobs.keys(), *self.job_manifests.keys()]))
        self.jobs.clear()
        self.job_manifests.clear()
        _JOB_LAST_SAVED_AT.clear()

        if remove_exports and self.export_root.exists():
            for child in self.export_root.iterdir():
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)

        self.export_root.mkdir(parents=True, exist_ok=True)

        if not remove_exports:
            self._index_jobs_from_disk()

        return cleared_jobs


async def fetch_text(client: httpx.AsyncClient, url: str) -> str:
    response = await client.get(url, timeout=SITEMAP_DISCOVERY_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response.text


async def robots_sitemaps(client: httpx.AsyncClient, start_url: str) -> List[str]:
    parsed = urlparse(start_url)
    robots_url = f"{parsed.scheme}://{parsed.netloc}/robots.txt"
    sitemaps: List[str] = []

    try:
        robots_text = await fetch_text(client, robots_url)
    except httpx.HTTPError:
        return sitemaps

    for line in robots_text.splitlines():
        if line.lower().startswith("sitemap:"):
            sitemap_url = line.split(":", 1)[1].strip()
            if sitemap_url:
                sitemaps.append(sitemap_url)

    return sitemaps


async def collect_sitemap_urls(
    client: httpx.AsyncClient,
    sitemap_url: str,
    *,
    base_host: str,
    max_urls: int,
    seen_sitemaps: Set[str],
) -> List[str]:
    if sitemap_url in seen_sitemaps or len(seen_sitemaps) > 200:
        return []
    seen_sitemaps.add(sitemap_url)

    try:
        xml_text = await fetch_text(client, sitemap_url)
        urls, nested_sitemaps = parse_sitemap_xml(xml_text)
    except (httpx.HTTPError, ET.ParseError):
        return []

    collected = [
        url
        for url in urls
        if is_same_site(url, base_host)
        and is_probably_html_url(url)
        and is_healthcare_relevant_url(url)
    ][:max_urls]

    for nested_url in nested_sitemaps:
        if len(collected) >= max_urls:
            break
        collected.extend(
            await collect_sitemap_urls(
                client,
                nested_url,
                base_host=base_host,
                max_urls=max_urls - len(collected),
                seen_sitemaps=seen_sitemaps,
            )
        )

    return collected[:max_urls]


async def discover_sitemap_pages(
    client: httpx.AsyncClient, start_url: str, base_host: str, max_urls: int
) -> List[str]:
    parsed = urlparse(start_url)
    candidates = [f"{parsed.scheme}://{parsed.netloc}/sitemap.xml"]
    candidates.extend(await robots_sitemaps(client, start_url))

    pages: List[str] = []
    seen_pages: Set[str] = set()
    seen_sitemaps: Set[str] = set()

    for sitemap_url in candidates:
        for page_url in await collect_sitemap_urls(
            client,
            sitemap_url,
            base_host=base_host,
            max_urls=max_urls - len(pages),
            seen_sitemaps=seen_sitemaps,
        ):
            if page_url not in seen_pages:
                seen_pages.add(page_url)
                pages.append(page_url)
            if len(pages) >= max_urls:
                return pages

    return pages


def add_error(job: CrawlJob, message: str) -> None:
    job.errors.append(message)
    if len(job.errors) > 100:
        job.errors = job.errors[-100:]
    save_job_state(job)


def job_directory(export_root: Path, job_id: str) -> Path:
    return export_root / job_id


def job_manifest_path(export_root: Path, job_id: str) -> Path:
    return job_directory(export_root, job_id) / JOB_MANIFEST_NAME


def ensure_export_paths(job: CrawlJob) -> None:
    export_dir = job_directory(job.export_root, job.job_id)
    job.export_paths = {
        "json": export_dir / "records.json",
        "jsonl": export_dir / "records.jsonl",
        "csv": export_dir / "records.csv",
    }


def serialize_queue_item(item: CrawlQueueItem) -> dict:
    return asdict(item)


def deserialize_queue_item(payload: dict) -> CrawlQueueItem:
    root_host_value = str(payload.get("root_host", ""))
    return CrawlQueueItem(
        url=str(payload.get("url", "")),
        depth=int(payload.get("depth", 0)),
        platform_id=str(payload.get("platform_id") or root_host_value or payload.get("url", "")),
        root_host=root_host_value,
        source_name=str(payload.get("source_name", "")),
        verified=bool(payload.get("verified", False)),
        verified_from=str(payload.get("verified_from", "")),
        tags=[str(tag) for tag in payload.get("tags", []) if str(tag)],
    )


def serialize_platform_state(platform: PlatformState) -> dict:
    return asdict(platform)


def deserialize_platform_state(payload: dict) -> PlatformState:
    return PlatformState(
        platform_id=str(payload.get("platform_id", "")),
        label=str(payload.get("label", "")),
        start_url=str(payload.get("start_url", "")),
        root_host=str(payload.get("root_host", "")),
        max_pages=int(payload.get("max_pages", 0)),
        discovered_pages=int(payload.get("discovered_pages", 0)),
        scraped_pages=int(payload.get("scraped_pages", 0)),
        accepted_pages=int(payload.get("accepted_pages", 0)),
        rejected_pages=int(payload.get("rejected_pages", 0)),
        status=str(payload.get("status", "queued")),
        status_message=str(payload.get("status_message", "Queued.")),
    )


def job_to_snapshot(job: CrawlJob) -> dict:
    active_runtime_state = job.status not in TERMINAL_JOB_STATUSES
    queue_items_payload = [serialize_queue_item(item) for item in job.queue_items]
    in_progress_items_payload = [serialize_queue_item(item) for item in job.in_progress_items]
    queued_urls_payload = list(job.queued_urls)
    in_progress_urls_payload = list(job.in_progress_urls)
    processed_urls_payload = list(job.processed_urls)

    if active_runtime_state:
        queue_items_payload = queue_items_payload[:MANIFEST_QUEUE_ITEM_SAMPLE_LIMIT]
        in_progress_items_payload = in_progress_items_payload[:MANIFEST_QUEUE_ITEM_SAMPLE_LIMIT]
        queued_urls_payload = list(itertools.islice(iter(job.queued_urls), MANIFEST_URL_SAMPLE_LIMIT))
        in_progress_urls_payload = list(
            itertools.islice(iter(job.in_progress_urls), MANIFEST_URL_SAMPLE_LIMIT)
        )
        processed_urls_payload = list(itertools.islice(iter(job.processed_urls), MANIFEST_URL_SAMPLE_LIMIT))
    else:
        queue_items_payload = []
        in_progress_items_payload = []
        queued_urls_payload = []
        in_progress_urls_payload = []
        processed_urls_payload = []

    return {
        "job_id": job.job_id,
        "start_url": job.start_url,
        "start_urls": job.start_urls,
        "platform_max_pages": job.platform_max_pages,
        "max_pages": job.max_pages,
        "max_depth": job.max_depth,
        "include_sitemap": job.include_sitemap,
        "concurrency": job.concurrency,
        "status": job.status,
        "status_message": job.status_message,
        "discovered_pages": job.discovered_pages,
        "scraped_pages": job.scraped_pages,
        "accepted_pages": job.accepted_pages,
        "rejected_pages": job.rejected_pages,
        "failed_pages": job.failed_pages,
        "records": [model_to_dict(record) for record in job.records],
        "errors": job.errors,
        "queue_items": queue_items_payload,
        "in_progress_items": in_progress_items_payload,
        "queued_urls": queued_urls_payload,
        "in_progress_urls": in_progress_urls_payload,
        "processed_urls": processed_urls_payload,
        "platform_states": [serialize_platform_state(platform) for platform in job.platform_states],
        "pause_requested": job.pause_requested,
        "cancel_requested": job.cancel_requested,
        "messages": job.messages,
    }


def save_job_state(job: CrawlJob, *, force: bool = False) -> None:
    now = time.monotonic()
    must_flush = (
        force
        or job.status in TERMINAL_JOB_STATUSES
        or job.pause_requested
        or job.cancel_requested
    )
    if not must_flush:
        last_saved_at = _JOB_LAST_SAVED_AT.get(job.job_id)
        if last_saved_at is not None and (now - last_saved_at) < STATE_WRITE_THROTTLE_SECONDS:
            return

    ensure_export_paths(job)
    export_dir = job_directory(job.export_root, job.job_id)
    export_dir.mkdir(parents=True, exist_ok=True)
    snapshot = job_to_snapshot(job)
    if job.status in TERMINAL_JOB_STATUSES:
        payload = json.dumps(snapshot, ensure_ascii=False, indent=2)
    else:
        payload = json.dumps(snapshot, ensure_ascii=False, separators=(",", ":"))
    job_manifest_path(job.export_root, job.job_id).write_text(payload, encoding="utf-8")
    _JOB_LAST_SAVED_AT[job.job_id] = now


def load_job_state(manifest_path: Path, export_root: Path) -> CrawlJob:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    job = CrawlJob(
        job_id=str(payload.get("job_id", manifest_path.parent.name)),
        start_url=str(payload.get("start_url", "")),
        start_urls=[str(url) for url in payload.get("start_urls", []) if str(url)],
        platform_max_pages=int(payload.get("platform_max_pages", 0)),
        max_pages=int(payload.get("max_pages", 1000)),
        max_depth=int(payload.get("max_depth", 2)),
        include_sitemap=bool(payload.get("include_sitemap", True)),
        concurrency=int(payload.get("concurrency", 4)),
        export_root=export_root,
        status=str(payload.get("status", "queued")),
        status_message=str(payload.get("status_message", "Queued.")),
        discovered_pages=int(payload.get("discovered_pages", 0)),
        scraped_pages=int(payload.get("scraped_pages", 0)),
        accepted_pages=int(payload.get("accepted_pages", 0)),
        rejected_pages=int(payload.get("rejected_pages", 0)),
        failed_pages=int(payload.get("failed_pages", 0)),
        records=[CrawlRecord.model_validate(record) for record in payload.get("records", [])],
        errors=[str(error) for error in payload.get("errors", [])],
        queue_items=[deserialize_queue_item(item) for item in payload.get("queue_items", [])],
        queued_urls={str(url) for url in payload.get("queued_urls", []) if str(url)},
        in_progress_items=[deserialize_queue_item(item) for item in payload.get("in_progress_items", [])],
        in_progress_urls={str(url) for url in payload.get("in_progress_urls", []) if str(url)},
        processed_urls={str(url) for url in payload.get("processed_urls", []) if str(url)},
        platform_states=[deserialize_platform_state(item) for item in payload.get("platform_states", [])],
        pause_requested=bool(payload.get("pause_requested", False)),
        cancel_requested=bool(payload.get("cancel_requested", False)),
        messages=[str(message) for message in payload.get("messages", [])],
    )
    if job.start_urls:
        unique_start_urls: List[str] = []
        seen_start_urls: Set[str] = set()
        for url in job.start_urls:
            if url in seen_start_urls:
                continue
            seen_start_urls.add(url)
            unique_start_urls.append(url)
        job.start_urls = unique_start_urls

    if not job.platform_states and job.start_urls:
        job.platform_states = []
        per_platform_limit = int(payload.get("platform_max_pages", job.max_pages)) or job.max_pages
        for start_url in job.start_urls:
            source_name, _, _, _ = classify_source(start_url)
            job.platform_states.append(
                PlatformState(
                    platform_id=start_url,
                    label=source_name,
                    start_url=start_url,
                    root_host=root_host(start_url),
                    max_pages=per_platform_limit,
                )
            )
    elif job.platform_states:
        deduped_platforms: List[PlatformState] = []
        seen_platform_ids: Set[str] = set()
        for platform in job.platform_states:
            if platform.platform_id in seen_platform_ids:
                continue
            seen_platform_ids.add(platform.platform_id)
            deduped_platforms.append(platform)
        job.platform_states = deduped_platforms
    ensure_export_paths(job)
    return job


def get_platform_state(job: CrawlJob, platform_id: str) -> Optional[PlatformState]:
    for platform in job.platform_states:
        if platform.platform_id == platform_id or platform.root_host == platform_id:
            return platform
    return None


def build_platform_worker_limits(platform_ids: List[str], total_workers: int) -> Dict[str, int]:
    unique_platform_ids: List[str] = []
    seen: Set[str] = set()
    for platform_id in platform_ids:
        if platform_id in seen:
            continue
        seen.add(platform_id)
        unique_platform_ids.append(platform_id)

    if not unique_platform_ids:
        return {}

    worker_count = max(1, total_workers)
    platform_count = len(unique_platform_ids)
    base = worker_count // platform_count
    remainder = worker_count % platform_count

    limits: Dict[str, int] = {}
    if base == 0:
        for platform_id in unique_platform_ids:
            limits[platform_id] = 1
        return limits

    for index, platform_id in enumerate(unique_platform_ids):
        limits[platform_id] = min(MAX_WORKERS_PER_PLATFORM, base + (1 if index < remainder else 0))
    return limits


def build_record(
    *,
    title: str,
    source_name: str,
    source_url: str,
    verified: bool,
    verified_from: str = "",
    tags: List[str],
    blocks: List[str],
) -> Optional[CrawlRecord]:
    if not is_healthcare_relevant_url(source_url):
        return None

    cleaned_blocks = clean_blocks(blocks)
    accepted_blocks, _, quality_score = filter_medical_blocks(cleaned_blocks)
    if not accepted_blocks:
        return None

    if not is_condition_reference_page(title, accepted_blocks, source_url):
        return None

    language = detect_language(" ".join(accepted_blocks))
    data = structure_medical_data(
        accepted_blocks,
        source_title=title,
        source_name=source_name,
        source_url=source_url,
        verified=verified,
        verified_from=verified_from,
    )

    # Keep only rich medical entries for RAG indexing quality.
    if not has_minimum_required_fields(data):
        return None

    return CrawlRecord(
        data=data,
        raw_blocks=accepted_blocks,
        language=language,
        quality_score=quality_score,
        tags=tags,
    )


def clear_runtime_crawl_state(job: CrawlJob) -> None:
    # Free large URL queues/sets once a job cannot be resumed.
    job.queue_items.clear()
    job.queued_urls.clear()
    job.in_progress_items.clear()
    job.in_progress_urls.clear()
    job.processed_urls.clear()


async def run_crawl_job(job: CrawlJob, resume: bool = False) -> None:
    if job.cancel_requested:
        job.status = "cancelled"
        job.status_message = "Crawl was cancelled."
        clear_runtime_crawl_state(job)
        save_job_state(job, force=True)
        return

    queue: asyncio.Queue[CrawlQueueItem] = asyncio.Queue()
    lock = asyncio.Lock()
    discovery_complete = False
    platform_active_workers: Dict[str, int] = defaultdict(int)
    platform_worker_limits = build_platform_worker_limits(
        [platform.platform_id for platform in job.platform_states], job.concurrency
    )

    def enqueue_item(item: CrawlQueueItem) -> bool:
        if len(job.queued_urls) >= job.max_pages:
            job.discovered_pages = len(job.queued_urls)
            return False

        platform = get_platform_state(job, item.platform_id)
        if platform and platform.discovered_pages >= platform.max_pages:
            return False

        if (
            item.url in job.queued_urls
            or item.url in job.in_progress_urls
            or item.url in job.processed_urls
            or not is_same_site(item.url, item.root_host)
            or not is_probably_html_url(item.url)
            or (item.depth > 0 and not is_healthcare_relevant_url(item.url))
        ):
            return False

        job.queued_urls.add(item.url)
        job.queue_items.append(item)
        if platform:
            platform.discovered_pages = min(platform.max_pages, platform.discovered_pages + 1)
            platform.status = "running"
            platform.status_message = f"Queued {platform.discovered_pages}/{platform.max_pages}."
        return True

    async def enqueue(urls: Iterable[str], depth: int, parent: CrawlQueueItem) -> None:
        for url in urls:
            normalized_url = canonicalize_url(url)
            item = CrawlQueueItem(
                url=normalized_url,
                depth=depth,
                platform_id=parent.platform_id,
                root_host=parent.root_host,
                source_name=parent.source_name,
                verified=parent.verified,
                verified_from=parent.verified_from,
                tags=parent.tags,
            )
            if enqueue_item(item):
                await queue.put(item)
        job.discovered_pages = len(job.queued_urls)
        save_job_state(job)

    async def worker(client: httpx.AsyncClient) -> None:
        while True:
            if job.cancel_requested or job.pause_requested:
                return

            try:
                item = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                if not discovery_complete:
                    continue
                async with lock:
                    has_in_progress = bool(job.in_progress_items)
                if has_in_progress or not queue.empty():
                    continue
                return

            if job.cancel_requested or job.pause_requested:
                await queue.put(item)
                queue.task_done()
                return

            should_requeue = False

            async with lock:
                platform = get_platform_state(job, item.platform_id)
                platform_limit = platform_worker_limits.get(item.platform_id, job.concurrency)
                active_platform_workers = platform_active_workers[item.platform_id]
                if active_platform_workers >= platform_limit:
                    should_requeue = True
                if should_requeue:
                    pass
                else:
                    try:
                        job.queue_items.remove(item)
                    except ValueError:
                        pass

                    if (
                        len(job.processed_urls) >= job.max_pages
                        or item.url in job.processed_urls
                        or (platform and platform.scraped_pages >= platform.max_pages)
                    ):
                        queue.task_done()
                        continue
                    job.in_progress_items.append(item)
                    job.in_progress_urls.add(item.url)
                    platform_active_workers[item.platform_id] += 1
                    if platform:
                        platform.scraped_pages = min(platform.max_pages, platform.scraped_pages + 1)
                        platform.status = "running"
                        platform.status_message = f"Scraping {platform.scraped_pages}/{platform.max_pages}."
                    job.status_message = f"Scraping {len(job.processed_urls) + 1}/{job.max_pages}: {item.url}"
                    save_job_state(job)

            if should_requeue:
                queue.put_nowait(item)
                queue.task_done()
                await asyncio.sleep(0.02)
                continue

            try:
                html = await fetch_html_with_client(client, item.url)
                title, blocks = parse_html(html)
                record = build_record(
                    title=title,
                    source_name=item.source_name,
                    source_url=item.url,
                    verified=item.verified,
                    verified_from=item.verified_from,
                    tags=item.tags,
                    blocks=blocks,
                )

                if record:
                    async with lock:
                        append_record_export(job, record)
                        if len(job.records) < 10:
                            job.records.append(record)
                        job.accepted_pages += 1
                        platform = get_platform_state(job, item.platform_id)
                        if platform:
                            platform.accepted_pages += 1
                else:
                    job.rejected_pages += 1
                    platform = get_platform_state(job, item.platform_id)
                    if platform:
                        platform.rejected_pages += 1

                if item.depth < job.max_depth and len(job.queued_urls) < job.max_pages:
                    await enqueue(extract_page_links(html, item.url), item.depth + 1, item)
            except ExtractionError as exc:
                job.failed_pages += 1
                add_error(job, f"{item.url}: {exc}")
            finally:
                async with lock:
                    job.in_progress_urls.discard(item.url)
                    platform_active_workers[item.platform_id] = max(
                        0, platform_active_workers[item.platform_id] - 1
                    )
                    try:
                        job.in_progress_items.remove(item)
                    except ValueError:
                        pass
                    job.processed_urls.add(item.url)
                    job.scraped_pages = len(job.processed_urls)
                    platform = get_platform_state(job, item.platform_id)
                    if platform:
                        if platform.scraped_pages == 0 and platform.discovered_pages == 0:
                            platform.status = "empty"
                            platform.status_message = "No pages found yet."
                        elif platform.scraped_pages >= platform.max_pages:
                            platform.status = "completed"
                            platform.status_message = (
                                f"Reached crawl limit {platform.scraped_pages}/{platform.max_pages}."
                            )
                        elif platform.status not in {"paused", "cancelled"}:
                            platform.status = "running"
                    save_job_state(job)
                queue.task_done()

    job.status = "running"
    job.pause_requested = False
    per_platform_worker_limit = max(platform_worker_limits.values()) if platform_worker_limits else 1
    job.status_message = "Discovering pages..."
    job.messages.append(
        f"Worker distribution enabled: up to {per_platform_worker_limit} concurrent worker(s) per platform."
    )
    save_job_state(job)

    try:
        prepare_exports(job, reset=not resume)

        start_urls = job.start_urls or ([job.start_url] if job.start_url else [])
        if not start_urls:
            raise ExtractionError("Provide at least one website link.")

        async with httpx.AsyncClient(
            timeout=20.0, headers=REQUEST_HEADERS, follow_redirects=True
        ) as client:
            workers = [asyncio.create_task(worker(client)) for _ in range(job.concurrency)]

            if resume and (job.queue_items or job.in_progress_items):
                for item in list(job.queue_items):
                    await queue.put(item)
                for item in list(job.in_progress_items):
                    await queue.put(item)
            else:
                job.queue_items.clear()
                job.queued_urls.clear()
                job.in_progress_urls.clear()
                job.in_progress_items.clear()
                job.processed_urls.clear()
                job.discovered_pages = 0
                job.scraped_pages = 0
                save_job_state(job)

                for start_url in start_urls:
                    root = root_host(start_url)
                    source_name, verified, verified_from, tags = classify_source(start_url)
                    seed_item = CrawlQueueItem(
                        url=start_url,
                        depth=0,
                        platform_id=start_url,
                        root_host=root,
                        source_name=source_name,
                        verified=verified,
                        verified_from=verified_from,
                        tags=tags,
                    )
                    if enqueue_item(seed_item):
                        await queue.put(seed_item)

                    if job.include_sitemap:
                        platform = get_platform_state(job, seed_item.platform_id)
                        sitemap_limit = job.platform_max_pages or job.max_pages
                        if platform:
                            sitemap_limit = max(0, platform.max_pages - platform.discovered_pages)
                        if sitemap_limit <= 0:
                            continue
                        sitemap_pages = await discover_sitemap_pages(
                            client, start_url, root, sitemap_limit
                        )
                        await enqueue(sitemap_pages, 0, seed_item)

            discovery_complete = True
            await asyncio.gather(*workers)

        if job.cancel_requested:
            job.status = "cancelled"
            job.status_message = "Crawl was cancelled by user."
            job.messages.append("Crawl cancelled before completion.")
            clear_runtime_crawl_state(job)
            save_job_state(job, force=True)
            return

        if job.pause_requested:
            job.status = "paused"
            job.status_message = "Crawl paused by user."
            job.messages.append("Crawl paused before completion.")
            save_job_state(job, force=True)
            return

        if not job.accepted_pages:
            job.status = "completed"
            if any("429" in error or "rate-limited" in error.casefold() for error in job.errors):
                job.status_message = (
                    "Completed with 0 records. Some platforms were rate-limited (HTTP 429), "
                    "but crawl continued across all provided platforms."
                )
            elif job.failed_pages > 0:
                job.status_message = (
                    "Completed with 0 records. Some platforms could not be fetched, "
                    "and others had no accepted medical pages."
                )
            else:
                job.status_message = (
                    "Completed with 0 records. No pages passed the medical relevance filters "
                    "across the provided platforms."
                )
            for platform in job.platform_states:
                if platform.accepted_pages == 0:
                    platform.status = "empty"
                    if platform.discovered_pages == 0:
                        platform.status_message = "No pages found."
                        job.messages.append(f"{platform.label}: no pages found.")
                    else:
                        platform.status_message = "No accepted medical pages."
                        job.messages.append(f"{platform.label}: no accepted medical pages.")
            finalize_json_export(job)
            clear_runtime_crawl_state(job)
            save_job_state(job, force=True)
            return

        finalize_json_export(job)
        job.status = "completed"
        job.status_message = (
            f"Completed. {job.accepted_pages} structured records are ready for export."
        )
        for platform in job.platform_states:
            if platform.accepted_pages == 0:
                platform.status = "empty"
                platform.status_message = (
                    "No pages found." if platform.discovered_pages == 0 else "No accepted medical pages."
                )
                job.messages.append(f"{platform.label}: {platform.status_message}")
            elif platform.status not in {"cancelled", "paused"}:
                platform.status = "completed"
                platform.status_message = f"Completed with {platform.accepted_pages} records."
        clear_runtime_crawl_state(job)
        save_job_state(job, force=True)
    except Exception as exc:  # pragma: no cover - defensive job boundary.
        job.status = "failed"
        job.status_message = str(exc)
        add_error(job, str(exc))
        clear_runtime_crawl_state(job)
        save_job_state(job, force=True)


def prepare_exports(job: CrawlJob, reset: bool = True) -> None:
    export_dir = job_directory(job.export_root, job.job_id)
    export_dir.mkdir(parents=True, exist_ok=True)

    json_path = export_dir / "records.json"
    jsonl_path = export_dir / "records.jsonl"
    csv_path = export_dir / "records.csv"

    if reset or not json_path.exists():
        json_path.write_text("[]", encoding="utf-8")
    if reset or not jsonl_path.exists():
        jsonl_path.write_text("", encoding="utf-8")

    if reset or not csv_path.exists():
        with csv_path.open("w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(
                file,
                fieldnames=[*EXPORT_FIELDS, "language", "quality_score", "raw_text"],
            )
            writer.writeheader()

    ensure_export_paths(job)
    job.export_paths = {"json": json_path, "jsonl": jsonl_path, "csv": csv_path}


def jsonl_payload(record: CrawlRecord) -> dict:
    return {
        "data": model_to_dict(record.data),
        "raw_blocks": record.raw_blocks,
        "language": record.language,
        "quality_score": record.quality_score,
        "tags": record.tags,
    }


def append_record_export(job: CrawlJob, record: CrawlRecord) -> None:
    with job.export_paths["jsonl"].open("a", encoding="utf-8") as file:
        file.write(json.dumps(jsonl_payload(record), ensure_ascii=False) + "\n")

    with job.export_paths["csv"].open("a", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[*EXPORT_FIELDS, "language", "quality_score", "raw_text"],
        )
        row = model_to_dict(record.data)
        for field_name in (
            "symptoms",
            "common_symptoms",
            "rare_symptoms",
            "differential_questions",
            "home_care",
            "lifestyle_tips",
            "warning_signs",
            "prevention",
            "risk_groups",
            "possible_confusions",
        ):
            row[field_name] = " | ".join(row.get(field_name) or [])
        for field_name in ("severity_levels", "confidence_rules"):
            row[field_name] = json.dumps(row.get(field_name) or {}, ensure_ascii=False)
        row = {field_name: row.get(field_name, "") for field_name in EXPORT_FIELDS}
        row["language"] = record.language
        row["quality_score"] = record.quality_score
        row["raw_text"] = "\n\n".join(record.raw_blocks)
        writer.writerow(row)

    save_job_state(job)


def finalize_json_export(job: CrawlJob) -> None:
    with job.export_paths["json"].open("w", encoding="utf-8") as output_file:
        output_file.write("[\n")
        first_record = True
        with job.export_paths["jsonl"].open("r", encoding="utf-8") as input_file:
            for line in input_file:
                if not line.strip():
                    continue
                payload = json.loads(line)
                if not first_record:
                    output_file.write(",\n")
                output_file.write(json.dumps(payload["data"], ensure_ascii=False, indent=2))
                first_record = False
        output_file.write("\n]\n")
    save_job_state(job)
