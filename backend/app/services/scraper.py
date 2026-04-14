import asyncio
from dataclasses import dataclass, field
from html.parser import HTMLParser
from typing import List, Optional, Tuple
from urllib.parse import urldefrag, urljoin, urlparse, urlunparse

import httpx

from .cleaner import normalize_spacing
from .exceptions import ExtractionError

try:
    from bs4 import BeautifulSoup
except ImportError:  # pragma: no cover - exercised only when dependency is absent.
    BeautifulSoup = None


REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

REMOVABLE_SELECTORS = (
    "script",
    "style",
    "noscript",
    "nav",
    "header",
    "footer",
    "aside",
    "form",
    "button",
    "iframe",
    "svg",
    "canvas",
    "figure",
    "figcaption",
    "[aria-hidden='true']",
    ".advertisement",
    ".ads",
    ".cookie",
    ".breadcrumb",
    ".social",
    ".share",
    ".menu",
)

NON_HTML_EXTENSIONS = (
    ".7z",
    ".avi",
    ".css",
    ".csv",
    ".doc",
    ".docx",
    ".gif",
    ".gz",
    ".jpeg",
    ".jpg",
    ".js",
    ".json",
    ".mov",
    ".mp3",
    ".mp4",
    ".pdf",
    ".png",
    ".ppt",
    ".pptx",
    ".rar",
    ".svg",
    ".tar",
    ".webp",
    ".xls",
    ".xlsx",
    ".xml",
    ".zip",
)


@dataclass
class ScrapeResult:
    title: str
    blocks: List[str]
    source_url: str
    source_name: str
    verified: bool
    verified_from: str = ""
    tags: List[str] = field(default_factory=list)


class ParagraphFallbackParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._capture_tag: Optional[str] = None
        self._buffer: List[str] = []
        self.blocks: List[str] = []
        self.title = ""
        self._inside_title = False

    def handle_starttag(self, tag: str, attrs: List[Tuple[str, Optional[str]]]) -> None:
        if tag in {"p", "li"}:
            self._capture_tag = tag
            self._buffer = []
        elif tag == "title":
            self._inside_title = True

    def handle_endtag(self, tag: str) -> None:
        if self._capture_tag and tag == self._capture_tag:
            text = normalize_spacing(" ".join(self._buffer))
            if text:
                self.blocks.append(text)
            self._capture_tag = None
            self._buffer = []
        elif tag == "title":
            self._inside_title = False

    def handle_data(self, data: str) -> None:
        if self._capture_tag:
            self._buffer.append(data)
        elif self._inside_title:
            self.title = normalize_spacing(f"{self.title} {data}")


def normalize_url(url: str) -> str:
    url = url.strip()
    if not url:
        raise ExtractionError("A URL was not provided.")

    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"

    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ExtractionError("Only valid HTTP or HTTPS URLs are supported.")

    return canonicalize_url(url)


def canonicalize_url(url: str) -> str:
    clean_url = urldefrag(url.strip())[0]
    parsed = urlparse(clean_url)
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/") or "/"
    return urlunparse(
        (parsed.scheme, parsed.netloc.lower(), path, "", parsed.query, "")
    )


def is_probably_html_url(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.lower()
    return not path.endswith(NON_HTML_EXTENSIONS)


def platform_label(host: str) -> str:
    if host.endswith("who.int"):
        return "WHO"
    if host.endswith("medlineplus.gov") or host.endswith("medlineplus.org"):
        return "MedlinePlus"
    if host.endswith("cdc.gov"):
        return "CDC"
    if host.endswith("nih.gov"):
        return "NIH"
    if host.endswith("nhs.uk"):
        return "NHS"
    if host.endswith("gov.in"):
        return "Government of India"
    if host.endswith(".gov") or ".gov." in host:
        return "Government health source"
    return ""


def classify_source(url: str) -> Tuple[str, bool, str, List[str]]:
    host = urlparse(url).netloc.lower().removeprefix("www.")
    tags: List[str] = []
    source_name = host or "Web page"
    verified_from = platform_label(host)

    if verified_from == "WHO":
        tags.append("WHO verified")
        source_name = "World Health Organization"
    elif verified_from == "MedlinePlus":
        tags.append("MedlinePlus verified")
        source_name = "MedlinePlus"
    elif verified_from == "CDC":
        tags.append("CDC verified")
        source_name = "Centers for Disease Control and Prevention"
    elif verified_from == "NIH":
        tags.append("NIH verified")
        source_name = "National Institutes of Health"
    elif verified_from == "NHS":
        tags.append("NHS verified")
        source_name = "National Health Service"
    elif verified_from == "Government of India":
        tags.append("Govt verified")
        source_name = "Government of India"
    elif verified_from == "Government health source":
        tags.append("Govt verified")
        if source_name == host:
            source_name = "Government health source"

    return source_name, bool(tags), verified_from, tags


async def fetch_html(url: str) -> str:
    try:
        async with httpx.AsyncClient(
            timeout=20.0, headers=REQUEST_HEADERS, follow_redirects=True
        ) as client:
            response = await get_with_retries(client, url)
    except httpx.HTTPError as exc:
        raise ExtractionError(f"Unable to fetch URL: {exc}") from exc

    content_type = response.headers.get("content-type", "")
    if "text/html" not in content_type and "application/xhtml" not in content_type:
        raise ExtractionError("The URL did not return an HTML page.")

    return response.text


async def fetch_html_with_client(client: httpx.AsyncClient, url: str) -> str:
    try:
        response = await get_with_retries(client, url)
    except httpx.HTTPError as exc:
        raise ExtractionError(f"Unable to fetch URL: {exc}") from exc

    content_type = response.headers.get("content-type", "")
    if "text/html" not in content_type and "application/xhtml" not in content_type:
        raise ExtractionError("The URL did not return an HTML page.")

    return response.text


def parse_retry_after_seconds(header_value: str) -> Optional[int]:
    try:
        seconds = int(header_value.strip())
    except (TypeError, ValueError, AttributeError):
        return None
    return max(seconds, 0)


async def get_with_retries(
    client: httpx.AsyncClient,
    url: str,
    *,
    max_attempts: int = 3,
) -> httpx.Response:
    last_error: Optional[httpx.HTTPError] = None

    for attempt in range(1, max_attempts + 1):
        try:
            response = await client.get(url)
            if response.status_code in {429, 500, 502, 503, 504}:
                retry_after = parse_retry_after_seconds(response.headers.get("retry-after", ""))
                if response.status_code == 429 and retry_after and retry_after > 20:
                    raise ExtractionError(
                        f"Source is rate-limited (HTTP 429). Retry after about {retry_after} seconds."
                    )

                if attempt < max_attempts:
                    delay = retry_after if retry_after is not None else attempt
                    await asyncio.sleep(min(max(delay, 1), 5))
                    continue

            response.raise_for_status()
            return response
        except ExtractionError:
            raise
        except httpx.HTTPError as exc:
            last_error = exc
            if attempt < max_attempts:
                await asyncio.sleep(min(attempt, 3))
                continue
            raise

    if last_error is not None:
        raise last_error
    raise ExtractionError("Unable to fetch URL after retries.")


def parse_html(html: str) -> Tuple[str, List[str]]:
    if BeautifulSoup is None:
        parser = ParagraphFallbackParser()
        parser.feed(html)
        return parser.title or "Untitled medical page", parser.blocks

    soup = BeautifulSoup(html, "html.parser")

    for selector in REMOVABLE_SELECTORS:
        for element in soup.select(selector):
            element.decompose()

    title = ""
    heading = soup.find("h1")
    if heading:
        title = heading.get_text(" ", strip=True)
    elif soup.title and soup.title.string:
        title = soup.title.string
    title = normalize_spacing(title) or "Untitled medical page"

    containers = soup.select(
        "article, main, [role='main'], .content, .entry-content, .article-body"
    )
    search_roots = containers or [soup.body or soup]
    blocks: List[str] = []

    for root in search_roots:
        for element in root.find_all(["p", "li"], recursive=True):
            text = normalize_spacing(element.get_text(" ", strip=True))
            if len(text) >= 40:
                blocks.append(text)

    return title, blocks


def extract_page_links(html: str, base_url: str) -> List[str]:
    links: List[str] = []

    if BeautifulSoup is None:
        return links

    soup = BeautifulSoup(html, "html.parser")
    for anchor in soup.find_all("a", href=True):
        href = str(anchor.get("href", "")).strip()
        if not href or href.startswith(("mailto:", "tel:", "javascript:")):
            continue

        absolute_url = canonicalize_url(urljoin(base_url, href))
        parsed = urlparse(absolute_url)
        if parsed.scheme in {"http", "https"} and parsed.netloc and is_probably_html_url(
            absolute_url
        ):
            links.append(absolute_url)

    return links


async def scrape_url(url: str) -> ScrapeResult:
    normalized_url = normalize_url(url)
    html = await fetch_html(normalized_url)
    title, blocks = parse_html(html)
    source_name, verified, verified_from, tags = classify_source(normalized_url)

    if not blocks:
        raise ExtractionError("No paragraph-level medical content was found on the page.")

    return ScrapeResult(
        title=title,
        blocks=blocks,
        source_url=normalized_url,
        source_name=source_name,
        verified=verified,
        verified_from=verified_from,
        tags=tags,
    )
