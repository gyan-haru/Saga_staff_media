from __future__ import annotations

import argparse
from pathlib import Path

from config import TRANSFER_DATA_DIR
from database import DB_PATH, import_transfers_csv


def list_transfer_csvs(directory: Path) -> list[Path]:
    return sorted(
        path
        for path in directory.glob("*.csv")
        if path.is_file()
        and "template" not in path.name.lower()
        and "source_index" not in path.name.lower()
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="人事異動CSVディレクトリ配下の年次CSVをまとめて取り込む")
    parser.add_argument(
        "--dir",
        default=str(TRANSFER_DATA_DIR),
        help="人事異動CSVを格納したディレクトリ",
    )
    parser.add_argument("--db", default=str(DB_PATH), help="対象DBパス")
    args = parser.parse_args()

    directory = Path(args.dir)
    csv_paths = list_transfer_csvs(directory)
    if not csv_paths:
        print(f"no csv files found in {directory}")
        return 0

    totals = {"files": 0, "sources": 0, "events": 0, "transfer_links": 0}
    for csv_path in csv_paths:
        counts = import_transfers_csv(csv_path, args.db)
        totals["files"] += 1
        for key in ("sources", "events", "transfer_links"):
            totals[key] += counts.get(key, 0)
        print(f"[{csv_path.name}] sources={counts['sources']} events={counts['events']} transfer_links={counts['transfer_links']}")

    print("---")
    for key, value in totals.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
