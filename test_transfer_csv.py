import tempfile
import unittest
from pathlib import Path

from import_transfer_directory import list_transfer_csvs
from validate_transfer_csv import is_transfer_data_csv, validate_transfer_csv


class TransferCsvToolingTestCase(unittest.TestCase):
    def test_list_transfer_csvs_ignores_template(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "official_2024.csv").write_text("a,b\n", encoding="utf-8")
            (root / "transfer_template.csv").write_text("a,b\n", encoding="utf-8")
            (root / "source_index.csv").write_text("a,b\n", encoding="utf-8")

            paths = list_transfer_csvs(root)

            self.assertEqual([path.name for path in paths], ["official_2024.csv"])

    def test_is_transfer_data_csv_ignores_source_index_and_template(self):
        self.assertTrue(is_transfer_data_csv(Path("official_2024.csv")))
        self.assertFalse(is_transfer_data_csv(Path("transfer_template.csv")))
        self.assertFalse(is_transfer_data_csv(Path("source_index.csv")))

    def test_validate_transfer_csv_accepts_header_only_file(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "official_2024.csv"
            csv_path.write_text(
                "source_type,title,url,published_at,effective_date,person_name,from_department,to_department,from_title,to_title,evidence_snippet,publisher,raw_text\n",
                encoding="utf-8",
            )

            issues = validate_transfer_csv(csv_path)

            self.assertEqual(issues, [])

    def test_validate_transfer_csv_reports_invalid_date_and_missing_person(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            csv_path = Path(tmpdir) / "official_2024.csv"
            csv_path.write_text(
                "\n".join(
                    [
                        "source_type,title,url,published_at,effective_date,person_name,from_department,to_department,from_title,to_title,evidence_snippet,publisher,raw_text",
                        "official_transfer_list,令和6年4月1日付人事異動,https://example.com/transfers,2024-04-01,2024-31-01,,広報広聴課,交通政策課,主査,主査,証拠,佐賀県,",
                    ]
                ),
                encoding="utf-8",
            )

            issues = validate_transfer_csv(csv_path)
            messages = [issue.message for issue in issues]

            self.assertIn("person_name is empty", messages)
            self.assertIn("invalid effective_date: 2024-31-01", messages)


if __name__ == "__main__":
    unittest.main()
