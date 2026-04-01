import sqlite3
import tempfile
import unittest
from pathlib import Path

from database import init_db
from rebuild_person_mentions import rebuild_person_mentions


class RebuildPersonMentionsTestCase(unittest.TestCase):
    def test_rebuild_uses_source_department_fallback(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = sqlite3.connect(db_path)
            conn.execute(
                """
                INSERT INTO projects (
                    title, url, source_type, source_list_url, source_department_name, summary, purpose, budget,
                    application_deadline, submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                    zip_urls_json, fetched_at
                )
                VALUES (?, ?, ?, ?, ?, '', '', '', '', '', '', '', '', '[]', '[]', '')
                """,
                (
                    "source fallback rebuild",
                    "https://example.com/rebuild-source-fallback",
                    "proposal",
                    "https://www.pref.saga.lg.jp/list00156.html",
                    "教育振興課",
                ),
            )
            conn.commit()
            conn.close()

            updated = rebuild_person_mentions(str(db_path))

            self.assertEqual(updated, 1)

            conn = sqlite3.connect(db_path)
            appearance = conn.execute(
                "select raw_department_name, raw_person_name, extracted_section from appearances where project_id = 1"
            ).fetchone()
            conn.close()

            self.assertEqual(appearance[0], "教育振興課")
            self.assertEqual(appearance[1], "")
            self.assertIn("Source list fallback", appearance[2])


if __name__ == "__main__":
    unittest.main()
