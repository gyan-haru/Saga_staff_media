from __future__ import annotations

import argparse

from database import DB_PATH, import_policy_sources_csv


def main() -> int:
    parser = argparse.ArgumentParser(description="重点施策ソースCSVを policy_topics に取り込む")
    parser.add_argument("csv_path", help="取り込み元CSVパス")
    parser.add_argument("--db", default=str(DB_PATH), help="対象DBパス")
    args = parser.parse_args()

    counts = import_policy_sources_csv(args.csv_path, args.db)
    for key, value in counts.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
