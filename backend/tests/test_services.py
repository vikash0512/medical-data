import base64
import json
import tempfile
import unittest
from pathlib import Path

from backend.app.services.cleaner import clean_blocks
from backend.app.services.crawler import (
    CrawlJob,
    append_record_export,
    build_record,
    finalize_json_export,
    is_same_site,
    parse_sitemap_xml,
    prepare_exports,
)
from backend.app.services.file_extractor import extract_file
from backend.app.services.filters import filter_medical_blocks, is_healthcare_relevant_url
from backend.app.services.language import detect_language
from backend.app.services.scraper import canonicalize_url
from backend.app.services.structurer import structure_medical_data
from backend.app.services.uploads import decode_uploaded_file


class ServiceTests(unittest.TestCase):
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
        self.assertTrue(record.symptoms)
        self.assertTrue(record.home_care)
        self.assertTrue(record.warning_signs)
        self.assertTrue(record.prevention)
        self.assertIn("who.int", record.source_url)

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

    def test_crawl_exports_stream_records_to_disk(self):
        record = build_record(
            title="Example Disease",
            source_name="example",
            source_url="https://example.com/disease",
            verified=False,
            tags=[],
            blocks=[
                (
                    "Symptoms include fever and cough. Treatment includes rest and fluids. "
                    "Prevention includes vaccination and hand washing."
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


if __name__ == "__main__":
    unittest.main()
