import json
from typing import Any, Iterable, List, Tuple

from ..models import StructuredMedicalData


MINIMUM_DUPLICATE_FIELDS = (
    "title",
    "source_url",
    "description",
)


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split()).casefold().strip()


def normalize_list(value: Any) -> Tuple[str, ...]:
    if not value:
        return tuple()
    if isinstance(value, str):
        value = [value]
    return tuple(sorted(normalize_text(item) for item in value if normalize_text(item)))


def stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False, separators=(",", ":"))


def record_fingerprint(record: StructuredMedicalData) -> str:
    """Create a stable identity for duplicate medical records."""
    payload = {
        "title": normalize_text(record.title),
        "source_url": normalize_text(record.source_url),
        "description": normalize_text(record.description),
        "symptoms": normalize_list(record.symptoms),
        "common_symptoms": normalize_list(record.common_symptoms),
        "rare_symptoms": normalize_list(record.rare_symptoms),
        "warning_signs": normalize_list(record.warning_signs),
        "prevention": normalize_list(record.prevention),
        "home_care": normalize_list(record.home_care),
        "lifestyle_tips": normalize_list(record.lifestyle_tips),
        "risk_groups": normalize_list(record.risk_groups),
    }
    return stable_json(payload)


def deduplicate_records(records: Iterable[StructuredMedicalData]) -> Tuple[List[StructuredMedicalData], int]:
    unique_records: List[StructuredMedicalData] = []
    seen = set()
    duplicate_count = 0

    for record in records:
        fingerprint = record_fingerprint(record)
        if fingerprint in seen:
            duplicate_count += 1
            continue
        seen.add(fingerprint)
        unique_records.append(record)

    return unique_records, duplicate_count
