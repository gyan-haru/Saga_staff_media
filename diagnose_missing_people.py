from __future__ import annotations

import argparse
import csv
import sqlite3
import re
from collections import Counter
from pathlib import Path

from config import DB_PATH, EXPORT_DIR
from extractor import normalize_text

CONTACT_SIGNAL_PATTERN = re.compile(r"(内線|直通|電話|TEL|E-mail|Email|Mail|メール|@)")
CONTACT_TRIGGER_PATTERN = re.compile(r"(問い合わせ先|問合せ先|提出先|申込先|連絡先|担当部署|担当課)")
RESULT_TITLE_PATTERN = re.compile(r"(結果をお知らせ|審査結果|選定結果|落札者|入札結果|質問回答|質問への回答|質問に対する回答|回答を掲載)")
EXPLICIT_PERSON_PATTERN = re.compile(r"^担当者[\s:：]*(?P<name>[^\s\d].{0,15})$")
INLINE_PERSON_PATTERN = re.compile(r"^(?:.+(?:課|部|局|室|所|班|係|チーム|担当|グループ))\s+(?P<name>[^\s\d]{1,8})$")


def classify_missing_person(title: str, raw_text: str) -> tuple[str, str]:
    lines = [line.strip() for line in normalize_text(raw_text).splitlines() if line.strip()]

    for index, line in enumerate(lines):
        if EXPLICIT_PERSON_PATTERN.search(line):
            window = " ".join(lines[index:index + 4])
            if CONTACT_SIGNAL_PATTERN.search(window):
                return "needs_reingest_explicit_person", window[:240]

        if INLINE_PERSON_PATTERN.search(line):
            window = " ".join(lines[index:index + 3])
            if CONTACT_SIGNAL_PATTERN.search(window):
                return "needs_reingest_inline_person", window[:240]

        if index + 1 < len(lines):
            next_line = lines[index + 1]
            if INLINE_PERSON_PATTERN.search(next_line):
                window = " ".join(lines[index:index + 5])
                if CONTACT_SIGNAL_PATTERN.search(window):
                    return "needs_reingest_inline_person", window[:240]

    for index, line in enumerate(lines):
        if not CONTACT_TRIGGER_PATTERN.search(line):
            continue
        window_lines = lines[index:index + 6]
        window = " ".join(window_lines)
        if not CONTACT_SIGNAL_PATTERN.search(window):
            continue
        if any(EXPLICIT_PERSON_PATTERN.search(candidate) or INLINE_PERSON_PATTERN.search(candidate) for candidate in window_lines):
            return "needs_reingest_contact_block", window[:240]
        return "source_has_department_only", window[:240]

    if RESULT_TITLE_PATTERN.search(title):
        return "result_or_answer_page_without_contact", title

    return "no_contact_section_detected", " / ".join(lines[:8])[:240]


def fetch_missing_people_rows(db_path: Path | str = DB_PATH) -> list[sqlite3.Row]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(
            """
            SELECT
                p.id AS project_id,
                p.source_type,
                p.title,
                p.url,
                p.published_at,
                a.raw_department_name,
                p.raw_text
            FROM appearances a
            JOIN projects p ON p.id = a.project_id
            WHERE IFNULL(TRIM(a.raw_person_name), '') = ''
            ORDER BY COALESCE(NULLIF(p.published_at, ''), substr(p.fetched_at, 1, 10)) DESC, p.id DESC
            """
        ).fetchall()
    finally:
        conn.close()


def write_report(rows: list[sqlite3.Row], output_path: Path) -> Counter:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    counts: Counter[str] = Counter()

    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "project_id",
                "source_type",
                "published_at",
                "title",
                "url",
                "raw_department_name",
                "cause_bucket",
                "evidence",
            ],
        )
        writer.writeheader()

        for row in rows:
            cause_bucket, evidence = classify_missing_person(row["title"] or "", row["raw_text"] or "")
            counts[cause_bucket] += 1
            writer.writerow(
                {
                    "project_id": row["project_id"],
                    "source_type": row["source_type"],
                    "published_at": row["published_at"],
                    "title": row["title"],
                    "url": row["url"],
                    "raw_department_name": row["raw_department_name"],
                    "cause_bucket": cause_bucket,
                    "evidence": evidence,
                }
            )

    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description="担当者が空欄のURLを原因別に診断してCSV化する")
    parser.add_argument("--db", default=str(DB_PATH), help="対象DBパス")
    parser.add_argument("--output", default=str(EXPORT_DIR / "missing_person_diagnosis.csv"), help="出力CSVパス")
    args = parser.parse_args()

    rows = fetch_missing_people_rows(args.db)
    counts = write_report(rows, Path(args.output))

    print(f"wrote {len(rows)} rows to {args.output}")
    for cause_bucket, count in counts.most_common():
        print(f"{cause_bucket}: {count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
