from typing import List, Literal, Optional

from pydantic import BaseModel, Field


class UploadedFilePayload(BaseModel):
    filename: str
    content_base64: str


class ExtractRequest(BaseModel):
    url: Optional[str] = None
    file: Optional[UploadedFilePayload] = None


class MergeJsonRequest(BaseModel):
    files: List[UploadedFilePayload] = Field(default_factory=list)


class SeverityBand(BaseModel):
    conditions: List[str] = Field(default_factory=list)
    advice: List[str] = Field(default_factory=list)


class SeverityLevels(BaseModel):
    mild: SeverityBand = Field(default_factory=SeverityBand)
    moderate: SeverityBand = Field(default_factory=SeverityBand)
    severe: SeverityBand = Field(default_factory=SeverityBand)


class ConfidenceRules(BaseModel):
    min_symptoms_match: int = 2
    high_confidence_threshold: float = 0.7


class StructuredMedicalData(BaseModel):
    title: str = ""
    category: str = "disease"
    symptoms: List[str] = Field(default_factory=list)
    common_symptoms: List[str] = Field(default_factory=list)
    rare_symptoms: List[str] = Field(default_factory=list)
    description: str = ""
    differential_questions: List[str] = Field(default_factory=list)
    severity_levels: SeverityLevels = Field(default_factory=SeverityLevels)
    home_care: List[str] = Field(default_factory=list)
    lifestyle_tips: List[str] = Field(default_factory=list)
    warning_signs: List[str] = Field(default_factory=list)
    when_to_seek_doctor: str = ""
    prevention: List[str] = Field(default_factory=list)
    risk_groups: List[str] = Field(default_factory=list)
    possible_confusions: List[str] = Field(default_factory=list)
    confidence_rules: ConfidenceRules = Field(default_factory=ConfidenceRules)
    source: str = ""
    verified: bool = False
    verified_from: str = ""
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


class MergeJsonResponse(BaseModel):
    merged_count: int
    duplicate_count: int
    source_file_count: int
    records: List[StructuredMedicalData] = Field(default_factory=list)
    messages: List[str] = Field(default_factory=list)


class CrawlStartRequest(BaseModel):
    url: Optional[str] = None
    urls: List[str] = Field(default_factory=list)
    max_pages: int = Field(default=250, ge=1, le=25000)
    max_depth: int = Field(default=2, ge=0, le=8)
    include_sitemap: bool = True
    concurrency: int = Field(default=2, ge=1, le=6)


class CrawlControlRequest(BaseModel):
    action: Literal["pause", "resume", "cancel"]


class PlatformProgress(BaseModel):
    platform_id: str
    label: str
    start_url: str
    max_pages: int
    discovered_pages: int = 0
    scraped_pages: int = 0
    accepted_pages: int = 0
    rejected_pages: int = 0
    status: str = "queued"
    status_message: str = "Queued."


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
    start_urls: List[str] = Field(default_factory=list)
    max_pages: int
    max_depth: int
    include_sitemap: bool
    concurrency: int
    discovered_pages: int
    scraped_pages: int
    accepted_pages: int
    rejected_pages: int
    failed_pages: int
    platform_progress: List[PlatformProgress] = Field(default_factory=list)
    records_preview: List[CrawlRecord] = Field(default_factory=list)
    errors: List[str] = Field(default_factory=list)
    messages: List[str] = Field(default_factory=list)
    download_json_url: Optional[str] = None
    download_jsonl_url: Optional[str] = None
    download_csv_url: Optional[str] = None
