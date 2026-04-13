def detect_language(text: str) -> str:
    """Small dependency-free language detector for English/Hindi routing."""
    if not text:
        return "unknown"

    devanagari_chars = sum(1 for char in text if "\u0900" <= char <= "\u097f")
    alpha_chars = sum(1 for char in text if char.isalpha())

    if alpha_chars and devanagari_chars / alpha_chars > 0.2:
        return "hi"

    return "en"

