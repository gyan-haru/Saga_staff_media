from __future__ import annotations

import csv
import os
from pathlib import Path

BASE_URL = "https://www.pref.saga.lg.jp"

PROPOSAL_KEYWORDS = [
    "プロポーザル",
    "企画競争",
    "コンペ",
    "企画提案",
    "公募型プロポーザル",
    "入札",
]

PRESS_RELEASE_KEYWORDS = [
    "お知らせ",
    "発表",
    "開始",
    "開催",
    "決定",
    "連携",
    "実証",
    "募集",
    "公開",
    "発信",
]

PRESS_RELEASE_DEPARTMENT_KEYWORDS = [
    "お知らせ",
    "発表",
    "発表します",
    "開催",
    "開催します",
    "決定",
    "決定しました",
    "公表",
    "公開",
    "開始",
    "実施",
    "募集",
    "交付",
    "交付されます",
    "認定",
    "表彰",
    "設立",
    "連携",
    "締結",
    "受賞",
    "報告",
    "談話",
]

DATA_DIR = Path(os.getenv("SAGA_MEDIA_DATA_DIR", Path(__file__).resolve().parent / "data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)

DB_PATH = DATA_DIR / "saga_media.db"
CRAWLED_URL_LOG_PATH = DATA_DIR / "crawled_urls.txt"
LIST_SOURCES_CSV_PATH = Path(os.getenv("SAGA_MEDIA_SOURCES_CSV_PATH", DATA_DIR / "list_sources.csv"))

DISCORD_WEBHOOK_URL = os.getenv("PROPOSAL_WEBHOOK_URL", os.getenv("DISCORD_WEBHOOK_URL", ""))

VALID_SOURCE_TYPES = {"proposal", "press_release"}
GENERIC_PRESS_RELEASE_LABELS = {"", "記者発表", "報道発表", "プレスリリース", "共通"}


def _build_source_record(url: str, department_name: str, source_type: str) -> dict[str, object]:
    source: dict[str, object] = {
        "url": url,
        "department_name": department_name,
        "source_type": source_type,
    }
    if source_type == "press_release" and department_name not in GENERIC_PRESS_RELEASE_LABELS:
        source["link_keywords"] = PRESS_RELEASE_DEPARTMENT_KEYWORDS
    return source


def load_list_sources(csv_path: Path | str = LIST_SOURCES_CSV_PATH) -> list[dict[str, object]]:
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(f"取得元CSVが見つかりません: {path}")

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {"url", "department_name", "source_type"}
        actual = set(reader.fieldnames or [])
        missing = required - actual
        if missing:
            missing_cols = ", ".join(sorted(missing))
            raise ValueError(f"取得元CSVに必要な列がありません: {missing_cols}")

        sources: list[dict[str, object]] = []
        seen_source_keys: set[tuple[str, str]] = set()
        for lineno, row in enumerate(reader, start=2):
            url = (row.get("url") or "").strip()
            department_name = (row.get("department_name") or "").strip()
            source_type = (row.get("source_type") or "").strip()

            if not url:
                continue
            if source_type not in VALID_SOURCE_TYPES:
                raise ValueError(
                    f"{path}:{lineno} source_type は proposal / press_release のみ対応です: {source_type}"
                )
            source_key = (url, source_type)
            if source_key in seen_source_keys:
                continue

            sources.append(_build_source_record(url, department_name, source_type))
            seen_source_keys.add(source_key)

    if not sources:
        raise ValueError(f"取得元CSVに有効な行がありません: {path}")

    return sources


LIST_SOURCES = load_list_sources()
