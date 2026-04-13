import re
from typing import Iterable, List


MIN_TEXT_LENGTH = 100


def normalize_spacing(text: str) -> str:
    """Normalize whitespace while preserving readable medical prose."""
    text = text.replace("\xa0", " ")
    text = text.replace("\u200b", "")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def clean_blocks(blocks: Iterable[str], min_length: int = MIN_TEXT_LENGTH) -> List[str]:
    cleaned: List[str] = []
    seen = set()

    for block in blocks:
        normalized = normalize_spacing(block)
        if len(normalized) < min_length:
            continue

        fingerprint = normalized.casefold()
        if fingerprint in seen:
            continue

        seen.add(fingerprint)
        cleaned.append(normalized)

    return cleaned

