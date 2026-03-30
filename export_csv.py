import csv
import sqlite3
from collections import defaultdict
from pathlib import Path

from config import DB_PATH

EXPORT_DIR = Path("exports")
EXPORT_DIR.mkdir(parents=True, exist_ok=True)


def export_grouped_csv() -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        # Fetch all projects with their primary appearance
        rows = conn.execute(
            """
            SELECT p.id, p.title, p.source_type, p.summary, p.budget, 
                   p.application_deadline, p.submission_deadline, p.published_at, p.url,
                   a.raw_department_name, a.raw_person_name
            FROM projects p
            LEFT JOIN appearances a ON a.project_id = p.id
            ORDER BY p.published_at DESC, p.id DESC
            """
        ).fetchall()

        # Group by department (if none, "部署不明") and source_type
        groups = defaultdict(list)
        for row in rows:
            dept = row["raw_department_name"] or "部署不明"
            dept_safe = dept.replace("/", "_").replace(" ", "").replace("\n", "")
            source = row["source_type"] or "unknown"
            groups[(dept_safe, source)].append(row)

        print(f"Total projects to export: {len(rows)}")
        print(f"Total grouped files to create: {len(groups)}")

        # Write each group to a CSV file
        count_files = 0
        for (dept_safe, source), group_rows in groups.items():
            filename = EXPORT_DIR / f"{dept_safe}_{source}.csv"
            with filename.open("w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "ID", "タイトル", "要約", "抽出された部署", "抽出された担当者", 
                    "予算", "申込締切", "提出締切", "掲載日", "URL"
                ])
                for r in group_rows:
                    writer.writerow([
                        r["id"], r["title"], r["summary"], r["raw_department_name"], r["raw_person_name"],
                        r["budget"], r["application_deadline"], r["submission_deadline"], r["published_at"], r["url"]
                    ])
            count_files += 1

        print(f"Successfully generated {count_files} CSV files in '{EXPORT_DIR.resolve()}'")

    finally:
        conn.close()


if __name__ == "__main__":
    export_grouped_csv()
