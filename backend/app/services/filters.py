from urllib.parse import urlparse
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


def contains_required_keyword(text: str) -> bool:
    lower_text = text.casefold()
    return any(keyword in lower_text for keyword in ALLOWED_KEYWORDS)


def has_medical_context(text: str, min_hits: int = 2) -> bool:
    lower_text = text.casefold()
    context_hits = sum(1 for keyword in MEDICAL_CONTEXT_KEYWORDS if keyword in lower_text)
    return context_hits >= min_hits


def contains_excluded_content(text: str) -> bool:
    lower_text = text.casefold()
    return any(keyword in lower_text for keyword in EXCLUDED_CONTENT_KEYWORDS)


def is_healthcare_relevant_url(url: str) -> bool:
    parsed = urlparse(url)
    path_and_query = f"{parsed.path} {parsed.query}".casefold()

    if not path_and_query.strip(" /"):
        return True

    return not any(keyword in path_and_query for keyword in EXCLUDED_URL_KEYWORDS)


def estimate_quality_score(blocks: Iterable[str]) -> float:
    joined = " ".join(blocks).casefold()
    if not joined:
        return 0.0

    required_hits = sum(1 for keyword in ALLOWED_KEYWORDS if keyword in joined)
    context_hits = sum(1 for keyword in MEDICAL_CONTEXT_KEYWORDS if keyword in joined)
    length_bonus = min(len(joined) / 5000, 1.0)
    score = (required_hits * 0.16) + (context_hits * 0.04) + (length_bonus * 0.2)
    if contains_excluded_content(joined):
        score -= 0.25
    return round(min(score, 1.0), 2)


def filter_medical_blocks(blocks: Iterable[str]) -> Tuple[List[str], List[str], float]:
    accepted: List[str] = []
    rejected: List[str] = []

    for block in blocks:
        if (
            contains_required_keyword(block)
            and has_medical_context(block)
            and not contains_excluded_content(block)
        ):
            accepted.append(block)
        else:
            rejected.append(block)

    return accepted, rejected, estimate_quality_score(accepted)

