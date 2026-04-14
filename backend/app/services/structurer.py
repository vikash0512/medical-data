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

DEFAULT_DIFFERENTIAL_QUESTIONS = [
    "Do you have high fever (>102°F)?",
    "Are you experiencing body pain?",
    "Since how many days do you have symptoms?",
]

DEFAULT_RISK_GROUPS = ["children", "pregnant women", "elderly"]

DEFAULT_POSSIBLE_CONFUSIONS = ["Common cold", "Flu", "COVID-19"]

LIFESTYLE_TERMS = (
    "sleep",
    "exercise",
    "diet",
    "hydration",
    "stress",
    "avoid smoking",
    "avoid alcohol",
)


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


def split_symptom_buckets(symptoms: List[str]) -> tuple[List[str], List[str]]:
    common = symptoms[:4]
    rare = symptoms[4:]
    return common, rare


def build_severity_levels(warning_signs: List[str], home_care: List[str]) -> dict:
    mild_conditions = [
        "No severe warning signs",
        "Mild and stable symptoms",
    ]
    moderate_conditions = [
        "Symptoms persist beyond a few days",
        "Symptoms interfere with daily activities",
    ]
    severe_conditions = warning_signs[:5] or ["Any emergency warning sign"]

    mild_advice = home_care[:5] or ["Monitor symptoms and continue basic supportive care"]
    moderate_advice = [
        "Consult a doctor soon for clinical evaluation",
        "Monitor progression closely and avoid self-medication escalation",
    ]
    severe_advice = [
        "Seek immediate medical care",
        "Visit the nearest emergency facility without delay",
    ]

    return {
        "mild": {"conditions": mild_conditions, "advice": mild_advice},
        "moderate": {"conditions": moderate_conditions, "advice": moderate_advice},
        "severe": {"conditions": severe_conditions, "advice": severe_advice},
    }


def has_minimum_required_fields(data: StructuredMedicalData) -> bool:
    return bool(
        data.title.strip()
        and data.category.strip()
        and data.symptoms
        and data.description.strip()
        and data.warning_signs
        and data.when_to_seek_doctor.strip()
        and data.prevention
    )


def infer_verified_from(source_name: str, verified_from: str) -> str:
    if verified_from:
        return verified_from

    lower_source = source_name.casefold()
    if "world health organization" in lower_source or "who" in lower_source:
        return "WHO"
    if any(term in lower_source for term in ("government", "cdc", "nih", "nhs")):
        return "Government health source"
    return ""


def structure_medical_data(
    blocks: List[str],
    *,
    source_title: Optional[str] = None,
    source_name: str = "",
    source_url: str = "",
    verified: bool = False,
    verified_from: str = "",
) -> StructuredMedicalData:
    symptoms = extract_matching_sentences(
        blocks, SYMPTOM_TERMS, exclude_terms=WARNING_CONTEXT_TERMS
    )
    common_symptoms, rare_symptoms = split_symptom_buckets(symptoms)
    prevention = extract_matching_sentences(blocks, PREVENTION_TERMS)
    warning_signs = extract_matching_sentences(blocks, WARNING_TERMS)
    home_care = extract_matching_sentences(blocks, HOME_CARE_TERMS)
    lifestyle_tips = extract_matching_sentences(blocks, LIFESTYLE_TERMS)
    when_to_seek_doctor = infer_when_to_seek_doctor(warning_signs, blocks)
    verified_from = infer_verified_from(source_name, verified_from) if verified else ""

    return StructuredMedicalData(
        title=infer_title(source_title, blocks),
        category="disease",
        symptoms=symptoms,
        common_symptoms=common_symptoms,
        rare_symptoms=rare_symptoms,
        description=build_description(blocks),
        differential_questions=DEFAULT_DIFFERENTIAL_QUESTIONS,
        severity_levels=build_severity_levels(warning_signs, home_care),
        home_care=home_care,
        lifestyle_tips=lifestyle_tips,
        warning_signs=warning_signs,
        when_to_seek_doctor=when_to_seek_doctor,
        prevention=prevention,
        risk_groups=DEFAULT_RISK_GROUPS,
        possible_confusions=DEFAULT_POSSIBLE_CONFUSIONS,
        confidence_rules={"min_symptoms_match": 2, "high_confidence_threshold": 0.7},
        source=source_name,
        verified=verified,
        verified_from=verified_from,
        source_url=source_url,
    )
