import unittest
from unittest.mock import patch

import ingest
from extractor import CrawledLink


class IngestTestCase(unittest.TestCase):
    def test_filter_sources_applies_source_type_and_url(self):
        fake_sources = [
            {"url": "https://example.com/list-a", "department_name": "共通", "source_type": "proposal"},
            {"url": "https://example.com/list-b", "department_name": "広報広聴課", "source_type": "press_release"},
            {"url": "https://example.com/list-c", "department_name": "教育振興課", "source_type": "proposal"},
        ]

        with patch.object(ingest, "LIST_SOURCES", fake_sources):
            self.assertEqual(len(ingest.filter_sources()), 3)
            self.assertEqual(len(ingest.filter_sources(source_type="proposal")), 2)
            self.assertEqual(
                ingest.filter_sources(source_type="proposal", source_url="https://example.com/list-c"),
                [fake_sources[2]],
            )

    def test_choose_better_link_prefers_more_specific_source_department(self):
        generic = CrawledLink(
            title="test",
            url="https://example.com/kiji1",
            source_type="proposal",
            source_list_url="https://example.com/list-generic",
            source_department_name="共通",
        )
        specific = CrawledLink(
            title="test",
            url="https://example.com/kiji1",
            source_type="proposal",
            source_list_url="https://example.com/list-specific",
            source_department_name="教育振興課",
        )

        chosen = ingest.choose_better_link(generic, specific)

        self.assertEqual(chosen.source_list_url, "https://example.com/list-specific")
        self.assertEqual(chosen.source_department_name, "教育振興課")


if __name__ == "__main__":
    unittest.main()
