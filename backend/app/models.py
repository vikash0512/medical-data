from typing import List, Optional

from pydantic import BaseModel, Field


class UploadedFilePayload(BaseModel):
    filename: str
    content_base64: str


class ExtractRequest(BaseModel):
    url: Optional[str] = None
    file: Optional[UploadedFilePayload] = None


class StructuredMedicalData(BaseModel):
    title: str = ""
    category: str = "disease"
    symptoms: List[str] = Field(default_factory=list)
    description: str = ""
    home_care: List[str] = Field(default_factory=list)
    warning_signs: List[str] = Field(default_factory=list)
    when_to_seek_doctor: str = ""
    prevention: List[str] = Field(default_factory=list)
    source: str = ""
    verified: bool = False
    source_url: str = ""


class ExtractionMetadata(BaseModel):
    source_type: str
    language: str
    tags: List[str] = Field(default_factory=list)
    accepted_blocks: int
    rejected_blocks: int
    quality_score: float
    messages: List[str] = Field(default_factory=list)


class ExtractResponse(BaseModel):
    data: StructuredMedicalData
    raw_blocks: List[str] = Field(default_factory=list)
    metadata: ExtractionMetadata


class CrawlStartRequest(BaseModel):
    url: str
    max_pages: int = Field(default=1000, ge=1, le=50000)
    max_depth: int = Field(default=2, ge=0, le=20)
    include_sitemap: bool = True
    concurrency: int = Field(default=4, ge=1, le=10)


class CrawlRecord(BaseModel):
    data: StructuredMedicalData
    raw_blocks: List[str] = Field(default_factory=list)
    language: str = "unknown"
    quality_score: float = 0.0
    tags: List[str] = Field(default_factory=list)


class CrawlJobResponse(BaseModel):
    job_id: str
    status: str
    status_message: str
    start_url: str
    max_pages: int
    max_depth: int
    include_sitemap: bool
    concurrency: int
    discovered_pages: int
    scraped_pages: int
    accepted_pages: int
    rejected_pages: int
    failed_pages: int
    records_preview: List[CrawlRecord] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    download_json_url: Optional[str] = None
    download_jsonl_url: Optional[str] = None
    download_csv_url: Optional[str] = None
