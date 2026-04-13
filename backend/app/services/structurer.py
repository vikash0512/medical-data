import re
from typing import Iterable, List, Optional

from ..models import StructuredMedicalData
from .cleaner import normalize_spacing


SYMPTOM_TERMS = (
    "symptom",
    "fever",
    "cough",
    "pain",
    "rash",
    "fatigue",
    "nausea",
    "vomiting",
    "diarrhea",
    "breath",
    "headache",
    "sore throat",
    "weakness",
)

PREVENTION_TERMS = (
    "prevent",
    "prevention",
    "avoid",
    "vaccin",
    "wash",
    "hygiene",
    "mask",
    "screening",
    "protect",
    "sanitation",
)

WARNING_TERMS = (
    "warning",
    "danger",
    "emergency",
    "severe",
    "seek medical",
    "seek care",
    "doctor",
    "hospital",
    "chest pain",
    "confusion",
)

HOME_CARE_TERMS = (
    "home care",
    "self care",
    "self-care",
    "rest",
    "fluids",
    "drink",
    "hydration",
    "isolate",
    "medicine",
    "treatment",
)

WARNING_CONTEXT_TERMS = ("warning", "danger", "emergency", "severe", "seek medical", "seek care")


def split_sentences(text: str) -> List[str]:
    sentences = re.split(r"(?<=[.!?])\s+", text)
    return [normalize_spacing(sentence) for sentence in sentences if sentence.strip()]


def truncate(text: str, limit: int = 280) -> str:
    if len(text) <= limit:
        return text
    trimmed = text[: limit - 3].rsplit(" ", 1)[0]
    return f"{trimmed}..."


def unique_items(items: Iterable[str], limit: int = 10) -> List[str]:
    seen = set()
    result: List[str] = []

    for item in items:
        normalized = normalize_spacing(item).strip(" -:;")
        if not normalized:
            continue
        fingerprint = normalized.casefold()
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        result.append(truncate(normalized))
        if len(result) >= limit:
            break

    return result


def extract_matching_sentences(
    blocks: Iterable[str],
    terms: Iterable[str],
    limit: int = 8,
    exclude_terms: Iterable[str] = (),
) -> List[str]:
    lowered_terms = tuple(term.casefold() for term in terms)
    lowered_exclusions = tuple(term.casefold() for term in exclude_terms)
    matches: List[str] = []

    for block in blocks:
        for sentence in split_sentences(block):
            lower_sentence = sentence.casefold()
            if lowered_exclusions and any(term in lower_sentence for term in lowered_exclusions):
                continue
            if any(term in lower_sentence for term in lowered_terms):
                matches.append(sentence)

    return unique_items(matches, limit=limit)


def build_description(blocks: List[str]) -> str:
    if not blocks:
        return ""
    joined = " ".join(blocks[:3])
    return truncate(joined, limit=1200)


def infer_title(source_title: Optional[str], blocks: List[str]) -> str:
    if source_title:
        return truncate(source_title, limit=140)

    if not blocks:
        return "Untitled medical dataset entry"

    first_sentence = split_sentences(blocks[0])[0] if split_sentences(blocks[0]) else blocks[0]
    return truncate(first_sentence, limit=140)


def infer_when_to_seek_doctor(warnings: List[str], blocks: Iterable[str]) -> str:
    for sentence in split_sentences(" ".join(blocks)):
        lower_sentence = sentence.casefold()
        if "seek" in lower_sentence and (
            "doctor" in lower_sentence or "medical" in lower_sentence or "care" in lower_sentence
        ):
            return truncate(sentence, limit=280)

    if warnings:
        return warnings[0]

    return ""


def structure_medical_data(
    blocks: List[str],
    *,
    source_title: Optional[str] = None,
    source_name: str = "",
    source_url: str = "",
    verified: bool = False,
) -> StructuredMedicalData:
    symptoms = extract_matching_sentences(
        blocks, SYMPTOM_TERMS, exclude_terms=WARNING_CONTEXT_TERMS
    )
    prevention = extract_matching_sentences(blocks, PREVENTION_TERMS)
    warning_signs = extract_matching_sentences(blocks, WARNING_TERMS)
    home_care = extract_matching_sentences(blocks, HOME_CARE_TERMS)

    return StructuredMedicalData(
        title=infer_title(source_title, blocks),
        category="disease",
        symptoms=symptoms,
        description=build_description(blocks),
        home_care=home_care,
        warning_signs=warning_signs,
        when_to_seek_doctor=infer_when_to_seek_doctor(warning_signs, blocks),
        prevention=prevention,
        source=source_name,
        verified=verified,
        source_url=source_url,
    )
