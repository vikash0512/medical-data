import asyncio
import csv
import json
import uuid
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple
from urllib.parse import urlparse

import httpx

from ..models import CrawlJobResponse, CrawlRecord
from .cleaner import clean_blocks
from .exceptions import ExtractionError
from .filters import filter_medical_blocks, is_healthcare_relevant_url
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


EXPORT_FIELDS = (
    "title",
    "category",
    "symptoms",
    "description",
    "home_care",
    "warning_signs",
    "when_to_seek_doctor",
    "prevention",
    "source",
    "verified",
    "source_url",
)


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
class CrawlJob:
    job_id: str
    start_url: str
    max_pages: int
    max_depth: int
    include_sitemap: bool
    concurrency: int
    export_root: Path
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
            max_pages=self.max_pages,
            max_depth=self.max_depth,
            include_sitemap=self.include_sitemap,
            concurrency=self.concurrency,
            discovered_pages=self.discovered_pages,
            scraped_pages=self.scraped_pages,
            accepted_pages=self.accepted_pages,
            rejected_pages=self.rejected_pages,
            failed_pages=self.failed_pages,
            records_preview=self.records[:10],
            errors=self.errors[-20:],
            download_json_url=download_json_url,
            download_jsonl_url=download_jsonl_url,
            download_csv_url=download_csv_url,
        )


class CrawlJobManager:
    def __init__(self, export_root: Path) -> None:
        self.export_root = export_root
        self.jobs: Dict[str, CrawlJob] = {}

    def create_job(
        self,
        *,
        start_url: str,
        max_pages: int,
        max_depth: int,
        include_sitemap: bool,
        concurrency: int,
    ) -> CrawlJob:
        normalized_url = normalize_url(start_url)
        job_id = uuid.uuid4().hex[:12]
        job = CrawlJob(
            job_id=job_id,
            start_url=normalized_url,
            max_pages=max_pages,
            max_depth=max_depth,
            include_sitemap=include_sitemap,
            concurrency=concurrency,
            export_root=self.export_root,
        )
        self.jobs[job_id] = job
        return job

    def get_job(self, job_id: str) -> Optional[CrawlJob]:
        return self.jobs.get(job_id)


async def fetch_text(client: httpx.AsyncClient, url: str) -> str:
    response = await client.get(url)
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


def build_record(
    *,
    title: str,
    source_name: str,
    source_url: str,
    verified: bool,
    tags: List[str],
    blocks: List[str],
) -> Optional[CrawlRecord]:
    if not is_healthcare_relevant_url(source_url):
        return None

    cleaned_blocks = clean_blocks(blocks)
    accepted_blocks, _, quality_score = filter_medical_blocks(cleaned_blocks)
    if not accepted_blocks:
        return None

    language = detect_language(" ".join(accepted_blocks))
    data = structure_medical_data(
        accepted_blocks,
        source_title=title,
        source_name=source_name,
        source_url=source_url,
        verified=verified,
    )

    return CrawlRecord(
        data=data,
        raw_blocks=accepted_blocks,
        language=language,
        quality_score=quality_score,
        tags=tags,
    )


async def run_crawl_job(job: CrawlJob) -> None:
    base_host = root_host(job.start_url)
    source_name, verified, tags = classify_source(job.start_url)
    queued_urls: Set[str] = set()
    scraped_urls: Set[str] = set()
    queue: asyncio.Queue[Tuple[str, int]] = asyncio.Queue()
    lock = asyncio.Lock()

    async def enqueue(urls: Iterable[str], depth: int) -> None:
        for url in urls:
            url = canonicalize_url(url)
            if len(queued_urls) >= job.max_pages:
                job.discovered_pages = len(queued_urls)
                return
            if (
                url in queued_urls
                or not is_same_site(url, base_host)
                or not is_probably_html_url(url)
                or (depth > 0 and not is_healthcare_relevant_url(url))
            ):
                continue
            queued_urls.add(url)
            await queue.put((url, depth))
        job.discovered_pages = len(queued_urls)

    async def worker(client: httpx.AsyncClient) -> None:
        while True:
            try:
                url, depth = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                return

            async with lock:
                if len(scraped_urls) >= job.max_pages or url in scraped_urls:
                    queue.task_done()
                    continue
                scraped_urls.add(url)
                job.scraped_pages = len(scraped_urls)
                job.status_message = f"Scraping {job.scraped_pages}/{job.max_pages}: {url}"

            try:
                html = await fetch_html_with_client(client, url)
                title, blocks = parse_html(html)
                record = build_record(
                    title=title,
                    source_name=source_name,
                    source_url=url,
                    verified=verified,
                    tags=tags,
                    blocks=blocks,
                )

                if record:
                    async with lock:
                        append_record_export(job, record)
                        if len(job.records) < 10:
                            job.records.append(record)
                        job.accepted_pages += 1
                else:
                    job.rejected_pages += 1

                if depth < job.max_depth and len(queued_urls) < job.max_pages:
                    await enqueue(extract_page_links(html, url), depth + 1)
            except ExtractionError as exc:
                job.failed_pages += 1
                add_error(job, f"{url}: {exc}")
            finally:
                queue.task_done()

    job.status = "running"
    job.status_message = "Discovering pages..."

    try:
        prepare_exports(job)
        async with httpx.AsyncClient(
            timeout=20.0, headers=REQUEST_HEADERS, follow_redirects=True
        ) as client:
            sitemap_pages: List[str] = []
            if job.include_sitemap:
                sitemap_pages = await discover_sitemap_pages(
                    client, job.start_url, base_host, job.max_pages
                )

            await enqueue([job.start_url], 0)
            await enqueue(sitemap_pages, 0)

            workers = [worker(client) for _ in range(job.concurrency)]
            await asyncio.gather(*workers)

        if not job.records:
            job.status = "failed"
            job.status_message = "No pages passed the medical keyword filters."
            return

        finalize_json_export(job)
        job.status = "completed"
        job.status_message = (
            f"Completed. {job.accepted_pages} structured records are ready for export."
        )
    except Exception as exc:  # pragma: no cover - defensive job boundary.
        job.status = "failed"
        job.status_message = str(exc)
        add_error(job, str(exc))


def prepare_exports(job: CrawlJob) -> None:
    export_dir = job.export_root / job.job_id
    export_dir.mkdir(parents=True, exist_ok=True)

    json_path = export_dir / "records.json"
    jsonl_path = export_dir / "records.jsonl"
    csv_path = export_dir / "records.csv"

    json_path.write_text("[]", encoding="utf-8")
    jsonl_path.write_text("", encoding="utf-8")

    with csv_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[*EXPORT_FIELDS, "language", "quality_score", "raw_text"],
        )
        writer.writeheader()

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
        for field_name in ("symptoms", "home_care", "warning_signs", "prevention"):
            row[field_name] = " | ".join(row.get(field_name) or [])
        row["language"] = record.language
        row["quality_score"] = record.quality_score
        row["raw_text"] = "\n\n".join(record.raw_blocks)
        writer.writerow(row)


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
