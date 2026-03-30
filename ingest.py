from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

from config import CRAWLED_URL_LOG_PATH, DISCORD_WEBHOOK_URL, LIST_SOURCES
from database import init_db, save_project_record
from extractor import ProjectRecord, collect_links_from_list_page, extract_project_record
import requests


def load_processed_urls(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {line.strip() for line in path.read_text(encoding="utf-8").splitlines() if line.strip()}


def append_processed_urls(path: Path, urls: list[str]) -> None:
    if not urls:
        return
    with path.open("a", encoding="utf-8") as handle:
        for url in urls:
            handle.write(url + "\n")


def notify_discord(message: str) -> None:
    if not DISCORD_WEBHOOK_URL:
        return
    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json={"content": message}, timeout=10)
        response.raise_for_status()
    except Exception as exc:
        print(f"Discord通知の送信に失敗しました: {exc}")


def format_notification(record: ProjectRecord) -> str:
    deadline = record.submission_deadline or record.application_deadline or "締切不明"
    budget = record.budget or "金額不明"
    purpose = record.purpose or record.summary or "概要なし"
    if len(purpose) > 180:
        purpose = purpose[:180] + "..."

    person_line = record.person_name or "担当者不明"
    if record.person_key:
        person_line += f" (key: {record.person_key})"

    return (
        "【佐賀県企画アーカイブ 新着】\n"
        f"種別: {record.source_type}\n"
        f"タイトル: {record.title}\n"
        f"担当: {record.department_name or '部署不明'} / {person_line}\n"
        f"締切: {deadline}\n"
        f"予算: {budget}\n"
        f"概要: {purpose}\n"
        f"URL: {record.url}"
    )


def should_skip_expired(record: ProjectRecord) -> bool:
    if record.source_type != "proposal":
        return False
    if not record.submission_deadline:
        return False
    try:
        deadline = dt.date.fromisoformat(record.submission_deadline)
    except ValueError:
        return False
    return deadline < dt.date.today()


def run_ingest(force: bool = False, notify: bool = False, max_pages: int = 1) -> int:
    init_db()

    processed_urls = set() if force else load_processed_urls(CRAWLED_URL_LOG_PATH)
    all_links = []
    for source in LIST_SOURCES:
        all_links.extend(
            collect_links_from_list_page(
                source["url"],
                source["source_type"],
                max_pages=max_pages,
                link_keywords=source.get("link_keywords"),
            )
        )

    unique_links = list({item.url: item for item in all_links}.values())
    print(f"Collected {len(unique_links)} unique links")

    new_count = 0
    newly_processed: list[str] = []

    for link in unique_links:
        if not force and link.url in processed_urls:
            continue

        print(f"Processing: {link.title}\n  {link.url}")
        record = extract_project_record(link)
        newly_processed.append(link.url)
        if not record:
            continue
        if should_skip_expired(record):
            print("  -> expired proposal, skipped")
            continue

        save_project_record(record)
        new_count += 1
        if notify:
            notify_discord(format_notification(record))

    append_processed_urls(CRAWLED_URL_LOG_PATH, newly_processed)
    print(f"Saved {new_count} records")
    return new_count


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="佐賀県の企画・プレスリリースをDBに取り込む")
    parser.add_argument("--force", action="store_true", help="URLログを無視して再取得する")
    parser.add_argument("--notify", action="store_true", help="保存後にDiscordへ通知する")
    parser.add_argument("--max-pages", type=int, default=1, help="遡るページ数の上限（デフォルト1）")
    args = parser.parse_args()
    raise SystemExit(run_ingest(force=args.force, notify=args.notify, max_pages=args.max_pages))
