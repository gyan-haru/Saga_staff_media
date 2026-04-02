from __future__ import annotations

import argparse
import shutil
from pathlib import Path

from config import BUNDLED_DATA_DIR, DATA_DIR


SEED_FILES = (
    ("list_sources.csv", "list_sources.csv"),
    ("department_hierarchy.csv", "department_hierarchy.csv"),
    ("transfers/transfer_template.csv", "transfers/transfer_template.csv"),
    ("transfers/source_index.csv", "transfers/source_index.csv"),
    ("policy_sources/policy_source_template.csv", "policy_sources/policy_source_template.csv"),
)

OPTIONAL_COPY_GROUPS = {
    "db": (
        ("saga_media.db", "saga_media.db"),
        ("crawled_urls.txt", "crawled_urls.txt"),
    ),
    "transfers": (
        ("transfers/README.md", "transfers/README.md"),
        ("transfers/official_2022.csv", "transfers/official_2022.csv"),
        ("transfers/official_2023.csv", "transfers/official_2023.csv"),
        ("transfers/official_2024.csv", "transfers/official_2024.csv"),
        ("transfers/official_2025.csv", "transfers/official_2025.csv"),
        ("transfers/official_2026.csv", "transfers/official_2026.csv"),
        ("transfers/newspaper_2022.csv", "transfers/newspaper_2022.csv"),
        ("transfers/newspaper_2023.csv", "transfers/newspaper_2023.csv"),
        ("transfers/newspaper_2024.csv", "transfers/newspaper_2024.csv"),
        ("transfers/newspaper_2025.csv", "transfers/newspaper_2025.csv"),
        ("transfers/newspaper_2026.csv", "transfers/newspaper_2026.csv"),
    ),
    "policy_sources": (
        ("policy_sources/README.md", "policy_sources/README.md"),
        ("policy_sources/official_2017_starter.csv", "policy_sources/official_2017_starter.csv"),
        ("policy_sources/official_2018_starter.csv", "policy_sources/official_2018_starter.csv"),
        ("policy_sources/official_2019_starter.csv", "policy_sources/official_2019_starter.csv"),
        ("policy_sources/official_2020_starter.csv", "policy_sources/official_2020_starter.csv"),
        ("policy_sources/official_2021_starter.csv", "policy_sources/official_2021_starter.csv"),
        ("policy_sources/official_2022_starter.csv", "policy_sources/official_2022_starter.csv"),
        ("policy_sources/official_2023_starter.csv", "policy_sources/official_2023_starter.csv"),
        ("policy_sources/official_2024_starter.csv", "policy_sources/official_2024_starter.csv"),
        ("policy_sources/official_2025_starter.csv", "policy_sources/official_2025_starter.csv"),
        ("policy_sources/official_2026_starter.csv", "policy_sources/official_2026_starter.csv"),
    ),
}


def copy_file(source_root: Path, target_root: Path, relative_source: str, relative_target: str, force: bool) -> bool:
    source_path = source_root / relative_source
    target_path = target_root / relative_target
    if not source_path.exists():
        return False
    if target_path.exists() and not force:
        return False
    target_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, target_path)
    return True


def ensure_runtime_layout(runtime_dir: Path) -> None:
    runtime_dir.mkdir(parents=True, exist_ok=True)
    (runtime_dir / "exports").mkdir(parents=True, exist_ok=True)
    (runtime_dir / "transfers").mkdir(parents=True, exist_ok=True)
    (runtime_dir / "policy_sources").mkdir(parents=True, exist_ok=True)


def bootstrap_runtime(
    runtime_dir: Path,
    *,
    force: bool = False,
    copy_db: bool = False,
    copy_transfers: bool = False,
    copy_policy_sources: bool = False,
) -> dict[str, int]:
    ensure_runtime_layout(runtime_dir)
    copied = 0

    for relative_source, relative_target in SEED_FILES:
        copied += int(copy_file(BUNDLED_DATA_DIR, runtime_dir, relative_source, relative_target, force))

    if copy_db:
        for relative_source, relative_target in OPTIONAL_COPY_GROUPS["db"]:
            copied += int(copy_file(BUNDLED_DATA_DIR, runtime_dir, relative_source, relative_target, force))

    if copy_transfers:
        for relative_source, relative_target in OPTIONAL_COPY_GROUPS["transfers"]:
            copied += int(copy_file(BUNDLED_DATA_DIR, runtime_dir, relative_source, relative_target, force))

    if copy_policy_sources:
        for relative_source, relative_target in OPTIONAL_COPY_GROUPS["policy_sources"]:
            copied += int(copy_file(BUNDLED_DATA_DIR, runtime_dir, relative_source, relative_target, force))

    return {
        "copied_files": copied,
        "runtime_dir": str(runtime_dir),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Windows など別環境用の runtime データ置き場を初期化する")
    parser.add_argument("--runtime-dir", default=str(DATA_DIR), help="外部データ置き場のルート")
    parser.add_argument("--force", action="store_true", help="既存ファイルを上書きする")
    parser.add_argument("--copy-db", action="store_true", help="既存DBと crawled_urls.txt もコピーする")
    parser.add_argument("--copy-transfers", action="store_true", help="bundled transfers CSV もコピーする")
    parser.add_argument("--copy-policy-sources", action="store_true", help="bundled policy source CSV もコピーする")
    args = parser.parse_args()

    counts = bootstrap_runtime(
        Path(args.runtime_dir),
        force=args.force,
        copy_db=args.copy_db,
        copy_transfers=args.copy_transfers,
        copy_policy_sources=args.copy_policy_sources,
    )
    print(f"runtime_dir: {counts['runtime_dir']}")
    print(f"copied_files: {counts['copied_files']}")
    print("set SAGA_MEDIA_DATA_DIR to this directory when running on Windows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
