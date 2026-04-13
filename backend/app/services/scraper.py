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
    "User-Agent": "MedicalDataExtractorPlatform/1.0 (+health-data-rag-builder)"
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


def classify_source(url: str) -> Tuple[str, bool, List[str]]:
    host = urlparse(url).netloc.lower().removeprefix("www.")
    tags: List[str] = []
    source_name = host or "Web page"

    if host.endswith("who.int"):
        tags.append("WHO verified")
        source_name = "World Health Organization"

    if (
        host.endswith(".gov")
        or ".gov." in host
        or host.endswith("gov.in")
        or host.endswith("nhs.uk")
        or host.endswith("cdc.gov")
        or host.endswith("nih.gov")
    ):
        tags.append("Govt verified")
        if source_name == host:
            source_name = "Government health source"

    return source_name, bool(tags), tags


async def fetch_html(url: str) -> str:
    try:
        async with httpx.AsyncClient(
            timeout=20.0, headers=REQUEST_HEADERS, follow_redirects=True
        ) as client:
            response = await client.get(url)
            response.raise_for_status()
    except httpx.HTTPError as exc:
        raise ExtractionError(f"Unable to fetch URL: {exc}") from exc

    content_type = response.headers.get("content-type", "")
    if "text/html" not in content_type and "application/xhtml" not in content_type:
        raise ExtractionError("The URL did not return an HTML page.")

    return response.text


async def fetch_html_with_client(client: httpx.AsyncClient, url: str) -> str:
    try:
        response = await client.get(url)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise ExtractionError(f"Unable to fetch URL: {exc}") from exc

    content_type = response.headers.get("content-type", "")
    if "text/html" not in content_type and "application/xhtml" not in content_type:
        raise ExtractionError("The URL did not return an HTML page.")

    return response.text


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
    source_name, verified, tags = classify_source(normalized_url)

    if not blocks:
        raise ExtractionError("No paragraph-level medical content was found on the page.")

    return ScrapeResult(
        title=title,
        blocks=blocks,
        source_url=normalized_url,
        source_name=source_name,
        verified=verified,
        tags=tags,
    )
