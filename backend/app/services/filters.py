from urllib.parse import parse_qsl, urlparse
from typing import Iterable, List, Tuple


ALLOWED_KEYWORDS = (
    "symptom",
    "symptoms",
    "treatment",
    "diagnosis",
    "management",
    "prevention",
    "prevent",
    "cause",
    "causes",
)

CONDITION_KEYWORDS = (
    "disease",
    "disorder",
    "syndrome",
    "infection",
    "covid",
    "influenza",
    "diabetes",
    "cancer",
    "tuberculosis",
    "malaria",
    "hypertension",
    "asthma",
)

MEDICAL_CONTEXT_KEYWORDS = (
    "disease",
    "infection",
    "health",
    "medical",
    "doctor",
    "patient",
    "diagnosis",
    "fever",
    "cough",
    "pain",
    "vaccine",
    "medicine",
    "clinic",
    "hospital",
    "care",
    "virus",
    "bacteria",
    "chronic",
    "acute",
    "therapy",
    "guideline",
    "public health",
)

EXCLUDED_CONTENT_KEYWORDS = (
    "campaign",
    "fundraising",
    "donate",
    "donation",
    "sponsor",
    "petition",
    "election",
    "merchandise",
    "shop now",
    "subscribe now",
    "all rights reserved",
    "newsletter",
    "media contact",
    "press release",
    "about us",
    "careers",
    "job openings",
    "section 508",
    "disclaimer",
    "trademark",
    "terms of use",
    "accessibility statement",
)

EXCLUDED_URL_KEYWORDS = (
    "campaign",
    "campaigns",
    "donate",
    "donation",
    "fundraising",
    "event",
    "events",
    "press",
    "newsroom",
    "media",
    "careers",
    "jobs",
    "privacy",
    "cookie",
    "terms",
    "contact",
    "about",
    "partner",
    "sponsor",
    "shop",
)

EXCLUDED_URL_SEGMENTS = {
    "campaign",
    "campaigns",
    "donate",
    "donation",
    "fundraising",
    "event",
    "events",
    "press",
    "news",
    "newsroom",
    "news-room",
    "media",
    "media-centre",
    "multimedia",
    "podcast",
    "video",
    "videos",
    "careers",
    "jobs",
    "privacy",
    "cookie",
    "cookies",
    "terms",
    "contact",
    "about",
    "shop",
    "store",
    "other",
    "accessibility",
    "agencymaterials",
    "agency-materials",
    "legal",
    "disclaimer",
}


def _keyword_hits(text: str, keywords: Iterable[str]) -> int:
    return sum(1 for keyword in keywords if keyword in text)


def contains_required_keyword(text: str) -> bool:
    lower_text = text.casefold()
    return any(keyword in lower_text for keyword in ALLOWED_KEYWORDS)


def has_medical_context(text: str, min_hits: int = 2) -> bool:
    lower_text = text.casefold()
    context_hits = _keyword_hits(lower_text, MEDICAL_CONTEXT_KEYWORDS)
    return context_hits >= min_hits


def contains_excluded_content(text: str) -> bool:
    lower_text = text.casefold()
    return any(keyword in lower_text for keyword in EXCLUDED_CONTENT_KEYWORDS)


def is_strong_medical_block(text: str) -> bool:
    lower_text = text.casefold()

    if contains_excluded_content(lower_text):
        return False

    required_hits = _keyword_hits(lower_text, ALLOWED_KEYWORDS)
    context_hits = _keyword_hits(lower_text, MEDICAL_CONTEXT_KEYWORDS)
    condition_hits = _keyword_hits(lower_text, CONDITION_KEYWORDS)

    if required_hits == 0:
        return False

    evidence_score = (required_hits * 2) + context_hits + (condition_hits * 2)

    # Keep only clinically meaningful blocks, not generic health messaging.
    return evidence_score >= 4 and (context_hits >= 1 or condition_hits >= 1)


def is_healthcare_relevant_url(url: str) -> bool:
    parsed = urlparse(url)
    path = parsed.path.casefold()
    query = parsed.query.casefold()
    path_and_query = f"{path} {query}"

    if not path_and_query.strip(" /"):
        return True

    segments = [segment for segment in path.split("/") if segment]
    if any(segment in EXCLUDED_URL_SEGMENTS for segment in segments):
        return False

    query_tokens = []
    for key, value in parse_qsl(query, keep_blank_values=True):
        query_tokens.append(key)
        query_tokens.append(value)

    if any(token in EXCLUDED_URL_SEGMENTS for token in query_tokens):
        return False

    return not any(keyword in path_and_query for keyword in EXCLUDED_URL_KEYWORDS)


def estimate_quality_score(blocks: Iterable[str]) -> float:
    joined = " ".join(blocks).casefold()
    if not joined:
        return 0.0

    required_hits = _keyword_hits(joined, ALLOWED_KEYWORDS)
    context_hits = _keyword_hits(joined, MEDICAL_CONTEXT_KEYWORDS)
    condition_hits = _keyword_hits(joined, CONDITION_KEYWORDS)
    length_bonus = min(len(joined) / 5000, 1.0)
    score = (required_hits * 0.14) + (context_hits * 0.04) + (condition_hits * 0.08) + (length_bonus * 0.2)
    if contains_excluded_content(joined):
        score -= 0.25
    return round(min(score, 1.0), 2)


def filter_medical_blocks(blocks: Iterable[str]) -> Tuple[List[str], List[str], float]:
    accepted: List[str] = []
    rejected: List[str] = []

    for block in blocks:
        if is_strong_medical_block(block):
            accepted.append(block)
        else:
            rejected.append(block)

    return accepted, rejected, estimate_quality_score(accepted)

