import unittest

from diagnose_source_coverage import build_source_report_row, extract_links_from_source_html


class DiagnoseSourceCoverageTestCase(unittest.TestCase):
    def test_extract_links_from_source_html_counts_raw_and_keyword_matched_links(self):
        html = """
        <html>
          <body>
            <a href="/kiji003100/index.html">令和8年度業務委託プロポーザルを実施します</a>
            <a href="/kiji003101/index.html">令和8年度審査結果を公表します</a>
            <a href="/kiji003102/index.html">連携協定を締結しました</a>
            <a href="/other003103/index.html">別ページ</a>
          </body>
        </html>
        """

        raw_links, matched_links = extract_links_from_source_html(html, "proposal")

        self.assertEqual(len(raw_links), 3)
        self.assertEqual(len(matched_links), 1)
        self.assertIn("https://www.pref.saga.lg.jp/kiji003100/index.html", matched_links)

    def test_extract_links_from_source_html_uses_broad_proposal_keywords(self):
        html = """
        <html>
          <body>
            <a href="/kiji003200/index.html">【佐賀城本丸歴史館】防蟻処理業務委託を行います</a>
            <a href="/kiji003201/index.html">観光イベントを開催します</a>
          </body>
        </html>
        """

        raw_links, matched_links = extract_links_from_source_html(html, "proposal", link_match_mode="broad")

        self.assertEqual(len(raw_links), 2)
        self.assertEqual(len(matched_links), 1)
        self.assertIn("https://www.pref.saga.lg.jp/kiji003200/index.html", matched_links)

    def test_build_source_report_row_counts_db_and_person_coverage(self):
        source = {
            "url": "https://www.pref.saga.lg.jp/list00001.html",
            "department_name": "広報広聴課",
            "source_type": "press_release",
            "link_match_mode": "",
        }
        diagnostics = {
            "pages_fetched": 1,
            "fetch_failed": False,
            "raw_links": {
                "https://www.pref.saga.lg.jp/kiji003100/index.html": object(),
                "https://www.pref.saga.lg.jp/kiji003101/index.html": object(),
                "https://www.pref.saga.lg.jp/kiji003102/index.html": object(),
            },
            "matched_links": {
                "https://www.pref.saga.lg.jp/kiji003100/index.html": type("Link", (), {"title": "A"})(),
                "https://www.pref.saga.lg.jp/kiji003101/index.html": type("Link", (), {"title": "B"})(),
            },
        }
        db_metrics = {
            "https://www.pref.saga.lg.jp/kiji003100/index.html": {
                "project_id": 1,
                "title": "A",
                "source_type": "press_release",
                "has_appearance": True,
                "has_person_name": False,
                "has_contact": True,
            },
            "https://www.pref.saga.lg.jp/kiji003101/index.html": {
                "project_id": 2,
                "title": "B",
                "source_type": "press_release",
                "has_appearance": True,
                "has_person_name": True,
                "has_contact": True,
            },
        }
        processed_urls = {
            "https://www.pref.saga.lg.jp/kiji003100/index.html",
            "https://www.pref.saga.lg.jp/kiji003101/index.html",
        }

        row = build_source_report_row(source, diagnostics, db_metrics, processed_urls, max_pages=3)

        self.assertEqual(row["all_kiji_links"], 3)
        self.assertEqual(row["link_match_mode"], "")
        self.assertEqual(row["keyword_matched_links"], 2)
        self.assertEqual(row["keyword_filtered_out"], 1)
        self.assertEqual(row["matched_in_db"], 2)
        self.assertEqual(row["matched_in_crawled_log"], 2)
        self.assertEqual(row["matched_with_appearance"], 2)
        self.assertEqual(row["matched_with_contact"], 2)
        self.assertEqual(row["matched_with_person_name"], 1)
        self.assertEqual(row["db_save_rate"], "100.0%")
        self.assertEqual(row["person_name_rate"], "50.0%")


if __name__ == "__main__":
    unittest.main()
