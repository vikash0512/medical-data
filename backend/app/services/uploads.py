import base64

from .exceptions import ExtractionError


MAX_UPLOAD_BYTES = 12 * 1024 * 1024


def decode_uploaded_file(content_base64: str) -> bytes:
    try:
        content = base64.b64decode(content_base64, validate=True)
    except ValueError as exc:
        raise ExtractionError("Uploaded file payload is not valid base64.") from exc

    if len(content) > MAX_UPLOAD_BYTES:
        raise ExtractionError("Uploaded file is larger than the 12 MB limit.")

    return content

