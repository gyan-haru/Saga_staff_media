from __future__ import annotations

import argparse

from database import DB_PATH, refresh_department_references


def main() -> int:
    parser = argparse.ArgumentParser(description="部署参照マスタに基づいて department_id を再正規化する")
    parser.add_argument("--db", default=str(DB_PATH), help="対象DBパス")
    args = parser.parse_args()

    counts = refresh_department_references(args.db)
    for key, value in counts.items():
        print(f"{key}: {value}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
