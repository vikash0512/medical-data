import base64
import json
import tempfile
import unittest
from pathlib import Path

from backend.app.services.cleaner import clean_blocks
from backend.app.services.crawler import (
    build_platform_worker_limits,
    CrawlJobManager,
    CrawlJob,
    append_record_export,
    build_record,
    finalize_json_export,
    is_same_site,
    parse_sitemap_xml,
    prepare_exports,
)
from backend.app.services.deduplicator import deduplicate_records, record_fingerprint
from backend.app.main import parse_uploaded_json_records
from backend.app.services.file_extractor import extract_file
from backend.app.services.filters import (
    filter_medical_blocks,
    is_condition_reference_page,
    is_healthcare_relevant_url,
)
from backend.app.services.language import detect_language
from backend.app.services.scraper import canonicalize_url
from backend.app.services.scraper import classify_source
from backend.app.services.structurer import structure_medical_data
from backend.app.services.uploads import decode_uploaded_file


class ServiceTests(unittest.TestCase):
    def test_build_platform_worker_limits_even_split(self):
        limits = build_platform_worker_limits(["p1", "p2", "p3"], 6)
        self.assertEqual(limits, {"p1": 2, "p2": 2, "p3": 2})

    def test_build_platform_worker_limits_with_remainder(self):
        limits = build_platform_worker_limits(["p1", "p2", "p3"], 5)
        self.assertEqual(limits, {"p1": 2, "p2": 2, "p3": 1})

    def test_build_platform_worker_limits_caps_at_three_per_platform(self):
        limits = build_platform_worker_limits(["p1", "p2", "p3"], 20)
        self.assertEqual(limits, {"p1": 3, "p2": 3, "p3": 3})

    def test_create_job_assigns_per_platform_page_budget(self):
        with tempfile.TemporaryDirectory() as directory:
            manager = CrawlJobManager(Path(directory))
            job = manager.create_job(
                start_urls=[
                    "https://www.who.int/",
                    "https://medlineplus.gov/",
                    "https://www.cdc.gov/",
                ],
                max_pages=1000,
                max_depth=1,
                include_sitemap=False,
                concurrency=8,
            )

            self.assertEqual(job.max_pages, 3000)
            self.assertEqual(job.platform_max_pages, 1000)
            self.assertEqual(job.concurrency, 8)
            self.assertEqual(len(job.platform_states), 3)
            self.assertTrue(all(platform.max_pages == 1000 for platform in job.platform_states))

    def test_create_job_caps_total_workers_to_three_per_platform(self):
        with tempfile.TemporaryDirectory() as directory:
            manager = CrawlJobManager(Path(directory))
            job = manager.create_job(
                start_urls=[
                    "https://www.who.int/",
                    "https://medlineplus.gov/",
                    "https://www.cdc.gov/",
                ],
                max_pages=1000,
                max_depth=1,
                include_sitemap=False,
                concurrency=20,
            )

            self.assertEqual(job.concurrency, 9)

    def test_clean_blocks_removes_short_text_and_duplicates(self):
        long_text = (
            "Symptoms may include fever and cough, and prevention depends on vaccination "
            "and hygiene practices recommended by trusted health authorities."
        )

        result = clean_blocks(["short", long_text, f"  {long_text}  "])

        self.assertEqual(result, [long_text])

    def test_filter_medical_blocks_keeps_required_keywords(self):
        accepted, rejected, score = filter_medical_blocks(
            [
                "Symptoms and prevention guidance for infection care with treatment details.",
                "A generic website footer with contact links and copyright text.",
            ]
        )

        self.assertEqual(len(accepted), 1)
        self.assertEqual(len(rejected), 1)
        self.assertGreater(score, 0)

    def test_filter_medical_blocks_rejects_campaign_content(self):
        accepted, rejected, _ = filter_medical_blocks(
            [
                (
                    "Our campaign asks supporters to donate now and sponsor awareness events "
                    "for better prevention messaging in communities."
                )
            ]
        )

        self.assertEqual(len(accepted), 0)
        self.assertEqual(len(rejected), 1)

    def test_filter_medical_blocks_rejects_weak_generic_health_message(self):
        accepted, rejected, _ = filter_medical_blocks(
            [
                (
                    "This campaign promotes better health in communities and asks people to "
                    "join events and donate for awareness programs."
                )
            ]
        )

        self.assertEqual(len(accepted), 0)
        self.assertEqual(len(rejected), 1)

    def test_filter_medical_blocks_accepts_strong_clinical_content(self):
        accepted, rejected, _ = filter_medical_blocks(
            [
                (
                    "Tuberculosis is an infectious disease. Symptoms include persistent cough, "
                    "fever, and weight loss. Diagnosis requires clinical evaluation and tests. "
                    "Treatment includes antibiotics and management under medical supervision."
                )
            ]
        )

        self.assertEqual(len(accepted), 1)
        self.assertEqual(len(rejected), 0)

    def test_healthcare_url_filter_blocks_campaign_paths(self):
        self.assertTrue(is_healthcare_relevant_url("https://www.who.int/health-topics/malaria"))
        self.assertFalse(is_healthcare_relevant_url("https://www.who.int/campaigns/world-health-day"))
        self.assertFalse(is_healthcare_relevant_url("https://www.who.int/news-room/releases"))
        self.assertFalse(is_healthcare_relevant_url("https://www.cdc.gov/other/accessibility.html"))
        self.assertFalse(is_healthcare_relevant_url("https://www.cdc.gov/other/agencymaterials.html"))

    def test_structure_medical_data_extracts_core_sections(self):
        blocks = [
            (
                "Symptoms of this disease may include fever, cough, headache, and fatigue. "
                "Treatment at home includes rest, fluids, and careful monitoring. Severe "
                "difficulty breathing is a warning sign and people should seek medical care. "
                "Prevention includes vaccination, hand washing, and avoiding close contact "
                "during outbreaks."
            )
        ]

        record = structure_medical_data(
            blocks,
            source_title="Example Disease",
            source_name="World Health Organization",
            source_url="https://www.who.int/example",
            verified=True,
        )

        self.assertEqual(record.title, "Example Disease")
        self.assertTrue(record.verified)
        self.assertEqual(record.verified_from, "WHO")
        self.assertTrue(record.symptoms)
        self.assertTrue(record.home_care)
        self.assertTrue(record.warning_signs)
        self.assertTrue(record.prevention)
        self.assertIn("who.int", record.source_url)

    def test_classify_source_sets_platform_specific_verified_from(self):
        source_name, verified, verified_from, tags = classify_source("https://medlineplus.gov/diabetes.html")

        self.assertTrue(verified)
        self.assertEqual(verified_from, "MedlinePlus")
        self.assertEqual(source_name, "MedlinePlus")
        self.assertTrue(any("MedlinePlus" in tag for tag in tags))

    def test_txt_file_extraction(self):
        content = (
            "Symptoms include fever and cough.\n\n"
            "Prevention includes vaccination and hand washing."
        ).encode("utf-8")

        result = extract_file("sample.txt", content)

        self.assertEqual(result.title, "sample")
        self.assertEqual(len(result.blocks), 2)

    def test_upload_base64_decoding(self):
        encoded = base64.b64encode(b"medical text").decode("ascii")
        self.assertEqual(decode_uploaded_file(encoded), b"medical text")

    def test_language_detection_for_hindi(self):
        self.assertEqual(detect_language("लक्षण और उपचार के बारे में जानकारी"), "hi")

    def test_sitemap_parser_reads_urlset(self):
        urls, nested = parse_sitemap_xml(
            """<?xml version="1.0" encoding="UTF-8"?>
            <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
              <url><loc>https://www.who.int/health-topics/malaria</loc></url>
              <url><loc>https://www.who.int/health-topics/tuberculosis</loc></url>
            </urlset>"""
        )

        self.assertEqual(nested, [])
        self.assertEqual(len(urls), 2)

    def test_same_site_allows_subdomains(self):
        self.assertTrue(is_same_site("https://www.who.int/news", "who.int"))
        self.assertTrue(is_same_site("https://data.who.int/page", "who.int"))
        self.assertFalse(is_same_site("https://example.com/page", "who.int"))

    def test_canonicalize_url_normalizes_homepage_variants(self):
        self.assertEqual(
            canonicalize_url("https://WHO.int/#main"),
            "https://who.int/",
        )

    def test_crawl_record_builder_rejects_irrelevant_content(self):
        record = build_record(
            title="Footer",
            source_name="example",
            source_url="https://example.com",
            verified=False,
            tags=[],
            blocks=["Contact links and copyright text with no useful healthcare signal."],
        )

        self.assertIsNone(record)

    def test_crawl_record_builder_rejects_incomplete_sections(self):
        record = build_record(
            title="Regional Update",
            source_name="example",
            source_url="https://example.com/health-update",
            verified=False,
            tags=[],
            blocks=[
                (
                    "Symptoms include fever and cough. Prevention includes hand washing and "
                    "vaccination during outbreaks."
                )
            ],
        )

        self.assertIsNone(record)

    def test_condition_reference_page_rejects_generic_explainer(self):
        self.assertFalse(
            is_condition_reference_page(
                "How the Wolbachia method works",
                [
                    (
                        "This method uses mosquitoes infected with Wolbachia bacteria to reduce the spread of dengue. "
                        "The campaign explains how the program works in local communities."
                    )
                ],
                "https://example.com/article",
            )
        )

    def test_crawl_record_builder_accepts_complete_sections(self):
        record = build_record(
            title="Example Disease",
            source_name="example",
            source_url="https://example.com/disease",
            verified=False,
            tags=[],
            blocks=[
                (
                    "Symptoms include fever and cough. Treatment at home includes rest and fluids. "
                    "Severe breathing difficulty is a warning sign, and people should seek medical care "
                    "from a doctor immediately. Prevention includes vaccination and hand washing."
                )
            ],
        )

        self.assertIsNotNone(record)

    def test_crawl_exports_stream_records_to_disk(self):
        record = build_record(
            title="Example Disease",
            source_name="example",
            source_url="https://example.com/disease",
            verified=False,
            tags=[],
            blocks=[
                (
                    "Symptoms include fever and cough. Treatment at home includes rest and fluids. "
                    "Severe breathing difficulty is a warning sign, and people should seek medical care "
                    "from a doctor immediately. Prevention includes vaccination and hand washing."
                )
            ],
        )
        self.assertIsNotNone(record)

        with tempfile.TemporaryDirectory() as directory:
            job = CrawlJob(
                job_id="testjob",
                start_url="https://example.com/",
                max_pages=10,
                max_depth=1,
                include_sitemap=True,
                concurrency=1,
                export_root=Path(directory),
            )
            prepare_exports(job)
            append_record_export(job, record)
            finalize_json_export(job)

            exported = json.loads(job.export_paths["json"].read_text(encoding="utf-8"))
            jsonl_text = job.export_paths["jsonl"].read_text(encoding="utf-8")
            self.assertEqual(exported[0]["title"], "Example Disease")
            self.assertIn("raw_blocks", jsonl_text)

    def test_record_fingerprint_matches_duplicates(self):
        first = structure_medical_data(
            [
                (
                    "Symptoms include fever and cough. Treatment at home includes rest and fluids. "
                    "Severe breathing difficulty is a warning sign, and people should seek medical care "
                    "from a doctor immediately. Prevention includes vaccination and hand washing."
                )
            ],
            source_title="Example Disease",
            source_name="example",
            source_url="https://example.com/disease",
            verified=False,
        )
        second = structure_medical_data(
            [
                (
                    "Symptoms include fever and cough. Treatment at home includes rest and fluids. "
                    "Severe breathing difficulty is a warning sign, and people should seek medical care "
                    "from a doctor immediately. Prevention includes vaccination and hand washing."
                )
            ],
            source_title="Example Disease",
            source_name="example",
            source_url="https://example.com/disease",
            verified=False,
        )

        self.assertEqual(record_fingerprint(first), record_fingerprint(second))

    def test_deduplicate_records_removes_duplicates(self):
        record_one = structure_medical_data(
            [
                (
                    "Symptoms include fever and cough. Treatment at home includes rest and fluids. "
                    "Severe breathing difficulty is a warning sign, and people should seek medical care "
                    "from a doctor immediately. Prevention includes vaccination and hand washing."
                )
            ],
            source_title="Example Disease",
            source_name="example",
            source_url="https://example.com/disease",
            verified=False,
        )
        record_two = structure_medical_data(
            [
                (
                    "Symptoms include fever and cough. Treatment at home includes rest and fluids. "
                    "Severe breathing difficulty is a warning sign, and people should seek medical care "
                    "from a doctor immediately. Prevention includes vaccination and hand washing."
                )
            ],
            source_title="Example Disease",
            source_name="example",
            source_url="https://example.com/disease",
            verified=False,
        )

        unique_records, duplicate_count = deduplicate_records([record_one, record_two])

        self.assertEqual(len(unique_records), 1)
        self.assertEqual(duplicate_count, 1)

    def test_parse_uploaded_json_records_supports_array(self):
        payload = json.dumps(
            [
                {
                    "title": "Example Disease",
                    "category": "disease",
                    "symptoms": ["fever"],
                    "common_symptoms": [],
                    "rare_symptoms": [],
                    "description": "Example description",
                    "differential_questions": [],
                    "severity_levels": {"mild": {"conditions": [], "advice": []}, "moderate": {"conditions": [], "advice": []}, "severe": {"conditions": [], "advice": []}},
                    "home_care": ["rest"],
                    "lifestyle_tips": [],
                    "warning_signs": ["seek care"],
                    "when_to_seek_doctor": "Seek care if severe",
                    "prevention": ["wash hands"],
                    "risk_groups": [],
                    "possible_confusions": [],
                    "confidence_rules": {"min_symptoms_match": 2, "high_confidence_threshold": 0.7},
                    "source": "example",
                    "verified": False,
                    "source_url": "https://example.com"
                }
            ]
        ).encode("utf-8")

        records = parse_uploaded_json_records("sample.json", payload)

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0].title, "Example Disease")

    def test_parse_uploaded_json_records_supports_records_object(self):
        payload = json.dumps(
            {
                "records": [
                    {
                        "title": "Example Disease",
                        "category": "disease",
                        "symptoms": ["fever"],
                        "common_symptoms": [],
                        "rare_symptoms": [],
                        "description": "Example description",
                        "differential_questions": [],
                        "severity_levels": {"mild": {"conditions": [], "advice": []}, "moderate": {"conditions": [], "advice": []}, "severe": {"conditions": [], "advice": []}},
                        "home_care": ["rest"],
                        "lifestyle_tips": [],
                        "warning_signs": ["seek care"],
                        "when_to_seek_doctor": "Seek care if severe",
                        "prevention": ["wash hands"],
                        "risk_groups": [],
                        "possible_confusions": [],
                        "confidence_rules": {"min_symptoms_match": 2, "high_confidence_threshold": 0.7},
                        "source": "example",
                        "verified": False,
                        "source_url": "https://example.com"
                    }
                ]
            }
        ).encode("utf-8")

        records = parse_uploaded_json_records("sample.json", payload)

        self.assertEqual(len(records), 1)


if __name__ == "__main__":
    unittest.main()
