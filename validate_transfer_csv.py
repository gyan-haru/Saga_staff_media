from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

REQUIRED_COLUMNS = (
    "source_type",
    "title",
    "url",
    "published_at",
    "effective_date",
    "person_name",
    "from_department",
    "to_department",
    "from_title",
    "to_title",
    "evidence_snippet",
)
ALLOWED_SOURCE_TYPES = {"official_transfer_list", "newspaper_transfer_list", "manual_note"}


@dataclass
class ValidationIssue:
    level: str
    row_number: int
    message: str


def is_transfer_data_csv(path: Path) -> bool:
    name = path.name.lower()
    return path.suffix.lower() == ".csv" and "template" not in name and "source_index" not in name


def is_valid_date(text: str) -> bool:
    value = (text or "").strip()
    if not value:
        return True
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def validate_transfer_csv(csv_path: Path | str) -> list[ValidationIssue]:
    path = Path(csv_path)
    issues: list[ValidationIssue] = []
    seen_event_keys: set[tuple[str, str, str, str]] = set()

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        fieldnames = tuple(reader.fieldnames or ())
        missing = [column for column in REQUIRED_COLUMNS if column not in fieldnames]
        if missing:
            return [ValidationIssue("error", 1, f"missing columns: {', '.join(missing)}")]

        for row_index, row in enumerate(reader, start=2):
            source_type = (row.get("source_type") or "").strip()
            title = (row.get("title") or "").strip()
            published_at = (row.get("published_at") or "").strip()
            effective_date = (row.get("effective_date") or "").strip()
            person_name = (row.get("person_name") or "").strip()
            from_department = (row.get("from_department") or "").strip()
            to_department = (row.get("to_department") or "").strip()

            if not any((value or "").strip() for value in row.values()):
                continue

            if source_type not in ALLOWED_SOURCE_TYPES:
                issues.append(
                    ValidationIssue(
                        "warning",
                        row_index,
                        f"unknown source_type: {source_type or '(empty)'}",
                    )
                )

            if not title:
                issues.append(ValidationIssue("error", row_index, "title is empty"))
            if not person_name:
                issues.append(ValidationIssue("error", row_index, "person_name is empty"))
            if not effective_date:
                issues.append(ValidationIssue("error", row_index, "effective_date is empty"))
            if not (from_department or to_department):
                issues.append(ValidationIssue("error", row_index, "both from_department and to_department are empty"))
            if published_at and not is_valid_date(published_at):
                issues.append(ValidationIssue("error", row_index, f"invalid published_at: {published_at}"))
            if effective_date and not is_valid_date(effective_date):
                issues.append(ValidationIssue("error", row_index, f"invalid effective_date: {effective_date}"))

            event_key = (effective_date, person_name, from_department, to_department)
            if all(event_key):
                if event_key in seen_event_keys:
                    issues.append(
                        ValidationIssue(
                            "warning",
                            row_index,
                            "duplicate event row in file",
                        )
                    )
                seen_event_keys.add(event_key)

    return issues


def main() -> int:
    parser = argparse.ArgumentParser(description="人事異動CSVの列・日付・重複を事前検証する")
    parser.add_argument("csv_path", nargs="?", help="対象CSVパス")
    parser.add_argument("--dir", help="CSVディレクトリをまとめて検証する")
    args = parser.parse_args()

    csv_paths: list[Path] = []
    if args.dir:
        csv_paths.extend(sorted(path for path in Path(args.dir).glob("*.csv") if is_transfer_data_csv(path)))
    if args.csv_path:
        csv_paths.append(Path(args.csv_path))

    if not csv_paths:
        print("no csv files specified")
        return 0

    has_error = False
    for csv_path in csv_paths:
        issues = validate_transfer_csv(csv_path)
        if not issues:
            print(f"[OK] {csv_path}")
            continue
        print(f"[CHECK] {csv_path}")
        for issue in issues:
            print(f"  {issue.level.upper()} row {issue.row_number}: {issue.message}")
            if issue.level == "error":
                has_error = True

    return 1 if has_error else 0


if __name__ == "__main__":
    raise SystemExit(main())
