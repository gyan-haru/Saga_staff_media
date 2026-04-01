from __future__ import annotations

import argparse
import csv
import re
import sqlite3
import urllib.parse
from collections import Counter
from pathlib import Path
from typing import Any

from bs4 import BeautifulSoup

from config import CRAWLED_URL_LOG_PATH, DB_PATH, LIST_SOURCES
from extractor import CrawledLink, BASE_URL, get_html, link_keywords_for_source, normalize_text


def extract_links_from_source_html(
    html: str,
    source_type: str,
    link_keywords: list[str] | None = None,
    link_match_mode: str | None = None,
) -> tuple[dict[str, CrawledLink], dict[str, CrawledLink]]:
    soup = BeautifulSoup(html, "html.parser")
    keywords = link_keywords_for_source(source_type, link_keywords, link_match_mode)
    require_keyword_match = bool(link_keywords) or source_type == "proposal"

    raw_links: dict[str, CrawledLink] = {}
    matched_links: dict[str, CrawledLink] = {}

    for a_tag in soup.find_all("a", href=True):
        href = a_tag["href"]
        if "kiji" not in href:
            continue

        text = normalize_text(a_tag.get_text())
        if not text:
            continue

        full_url = urllib.parse.urljoin(BASE_URL, href)
        raw_links.setdefault(full_url, CrawledLink(title=text.strip() or full_url, url=full_url, source_type=source_type))

        if require_keyword_match and not any(keyword in text for keyword in keywords):
            continue
        matched_links.setdefault(full_url, CrawledLink(title=text.strip() or full_url, url=full_url, source_type=source_type))

    return raw_links, matched_links


def build_pager_url_template(current_html: str, current_url: str) -> str | None:
    soup = BeautifulSoup(current_html, "html.parser")
    next_url_template = None
    for a_tag in soup.find_all("a", rel=True):
        rel_vals = a_tag.get("rel", [])
        if "next1" in rel_vals or "next" in rel_vals:
            href = a_tag.get("href")
            if href and "hpkijilistpagerhandler.ashx" in href:
                next_url_template = urllib.parse.urljoin(current_url, href)
                break

    if not next_url_template:
        return None
    return re.sub(r"pg=\d+", "pg={pg}", next_url_template)


def collect_source_diagnostics(
    list_url: str,
    source_type: str,
    max_pages: int = 1,
    link_keywords: list[str] | None = None,
    link_match_mode: str | None = None,
) -> dict[str, Any]:
    html = get_html(list_url)
    if not html:
        return {
            "pages_fetched": 0,
            "fetch_failed": True,
            "raw_links": {},
            "matched_links": {},
        }

    raw_links, matched_links = extract_links_from_source_html(html, source_type, link_keywords, link_match_mode)
    pages_fetched = 1

    if max_pages > 1:
        pager_template = build_pager_url_template(html, list_url)
        if pager_template:
            for page_num in range(2, max_pages + 1):
                pager_url = pager_template.format(pg=page_num)
                pager_html = get_html(pager_url)
                if not pager_html:
                    break
                page_raw_links, page_matched_links = extract_links_from_source_html(
                    pager_html,
                    source_type,
                    link_keywords,
                    link_match_mode,
                )
                if not page_raw_links:
                    break
                raw_links.update(page_raw_links)
                matched_links.update(page_matched_links)
                pages_fetched += 1

    return {
        "pages_fetched": pages_fetched,
        "fetch_failed": False,
        "raw_links": raw_links,
        "matched_links": matched_links,
    }


def load_processed_urls(path: Path | str = CRAWLED_URL_LOG_PATH) -> set[str]:
    source_path = Path(path)
    if not source_path.exists():
        return set()
    return {line.strip() for line in source_path.read_text(encoding="utf-8").splitlines() if line.strip()}


def fetch_project_metrics(db_path: Path | str = DB_PATH) -> dict[str, dict[str, Any]]:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(
            """
            SELECT
                p.url,
                p.id,
                p.title,
                p.source_type,
                MAX(CASE WHEN a.id IS NOT NULL THEN 1 ELSE 0 END) AS has_appearance,
                MAX(CASE WHEN IFNULL(TRIM(a.raw_person_name), '') != '' THEN 1 ELSE 0 END) AS has_person_name,
                MAX(
                    CASE
                        WHEN IFNULL(TRIM(a.contact_email), '') != '' OR IFNULL(TRIM(a.contact_phone), '') != ''
                        THEN 1
                        ELSE 0
                    END
                ) AS has_contact
            FROM projects p
            LEFT JOIN appearances a ON a.project_id = p.id
            GROUP BY p.id, p.url, p.title, p.source_type
            """
        ).fetchall()
    finally:
        conn.close()

    metrics: dict[str, dict[str, Any]] = {}
    for row in rows:
        metrics[row["url"]] = {
            "project_id": int(row["id"]),
            "title": row["title"],
            "source_type": row["source_type"],
            "has_appearance": bool(row["has_appearance"]),
            "has_person_name": bool(row["has_person_name"]),
            "has_contact": bool(row["has_contact"]),
        }
    return metrics


def build_source_report_row(
    source: dict[str, object],
    diagnostics: dict[str, Any],
    db_metrics: dict[str, dict[str, Any]],
    processed_urls: set[str],
    max_pages: int,
) -> dict[str, Any]:
    raw_links: dict[str, CrawledLink] = diagnostics["raw_links"]
    matched_links: dict[str, CrawledLink] = diagnostics["matched_links"]
    matched_urls = list(matched_links.keys())

    matched_in_db = [url for url in matched_urls if url in db_metrics]
    matched_in_processed = [url for url in matched_urls if url in processed_urls]
    matched_with_appearance = [url for url in matched_in_db if db_metrics[url]["has_appearance"]]
    matched_with_contact = [url for url in matched_in_db if db_metrics[url]["has_contact"]]
    matched_with_person_name = [url for url in matched_in_db if db_metrics[url]["has_person_name"]]

    missing_titles = [
        matched_links[url].title
        for url in matched_urls
        if url not in db_metrics
    ][:3]
    saved_but_no_person_titles = [
        db_metrics[url]["title"]
        for url in matched_in_db
        if not db_metrics[url]["has_person_name"]
    ][:3]

    return {
        "source_url": source["url"],
        "department_name": source["department_name"],
        "source_type": source["source_type"],
        "link_match_mode": source.get("link_match_mode", ""),
        "max_pages": max_pages,
        "pages_fetched": diagnostics["pages_fetched"],
        "fetch_failed": int(bool(diagnostics["fetch_failed"])),
        "all_kiji_links": len(raw_links),
        "keyword_matched_links": len(matched_links),
        "keyword_filtered_out": max(0, len(raw_links) - len(matched_links)),
        "matched_in_crawled_log": len(matched_in_processed),
        "matched_in_db": len(matched_in_db),
        "matched_with_appearance": len(matched_with_appearance),
        "matched_with_contact": len(matched_with_contact),
        "matched_with_person_name": len(matched_with_person_name),
        "db_save_rate": format_ratio(len(matched_in_db), len(matched_links)),
        "person_name_rate": format_ratio(len(matched_with_person_name), len(matched_in_db)),
        "missing_titles_sample": " | ".join(missing_titles),
        "saved_but_no_person_sample": " | ".join(saved_but_no_person_titles),
    }


def format_ratio(numerator: int, denominator: int) -> str:
    if denominator <= 0:
        return ""
    return f"{(numerator / denominator) * 100:.1f}%"


def write_report(rows: list[dict[str, Any]], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "source_url",
                "department_name",
                "source_type",
                "link_match_mode",
                "max_pages",
                "pages_fetched",
                "fetch_failed",
                "all_kiji_links",
                "keyword_matched_links",
                "keyword_filtered_out",
                "matched_in_crawled_log",
                "matched_in_db",
                "matched_with_appearance",
                "matched_with_contact",
                "matched_with_person_name",
                "db_save_rate",
                "person_name_rate",
                "missing_titles_sample",
                "saved_but_no_person_sample",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def summarize_rows(rows: list[dict[str, Any]]) -> list[str]:
    fetch_failed = sum(int(row["fetch_failed"]) for row in rows)
    zero_matched = sum(1 for row in rows if int(row["keyword_matched_links"]) == 0)
    zero_saved = sum(1 for row in rows if int(row["matched_in_db"]) == 0)
    zero_person = sum(1 for row in rows if int(row["matched_with_person_name"]) == 0)
    lines = [
        f"sources: {len(rows)}",
        f"fetch_failed: {fetch_failed}",
        f"zero_keyword_matched: {zero_matched}",
        f"zero_saved_in_db: {zero_saved}",
        f"zero_person_name: {zero_person}",
        "top_keyword_filtered:",
    ]
    for row in sorted(rows, key=lambda item: int(item["keyword_filtered_out"]), reverse=True)[:10]:
        lines.append(
            f"  {row['source_type']} {row['department_name']} "
            f"filtered={row['keyword_filtered_out']} matched={row['keyword_matched_links']} url={row['source_url']}"
        )
    lines.append("top_saved_but_no_person:")
    for row in sorted(
        rows,
        key=lambda item: (int(item["matched_in_db"]) - int(item["matched_with_person_name"])),
        reverse=True,
    )[:10]:
        missing_person = int(row["matched_in_db"]) - int(row["matched_with_person_name"])
        lines.append(
            f"  {row['source_type']} {row['department_name']} "
            f"saved={row['matched_in_db']} person={row['matched_with_person_name']} missing_person={missing_person} "
            f"url={row['source_url']}"
        )
    return lines


def main() -> int:
    parser = argparse.ArgumentParser(description="list_sources ごとの収集・保存・担当者抽出の歩留まりを診断する")
    parser.add_argument("--db", default=str(DB_PATH), help="対象DBパス")
    parser.add_argument("--output", default="exports/source_coverage_diagnosis.csv", help="出力CSVパス")
    parser.add_argument("--max-pages", type=int, default=1, help="各一覧URLで遡るページ数")
    args = parser.parse_args()

    db_metrics = fetch_project_metrics(args.db)
    processed_urls = load_processed_urls()
    rows: list[dict[str, Any]] = []

    for index, source in enumerate(LIST_SOURCES, start=1):
        print(f"[{index}/{len(LIST_SOURCES)}] {source['department_name']} {source['url']}")
        diagnostics = collect_source_diagnostics(
            source["url"],
            str(source["source_type"]),
            max_pages=args.max_pages,
            link_keywords=source.get("link_keywords"),
            link_match_mode=source.get("link_match_mode"),
        )
        rows.append(build_source_report_row(source, diagnostics, db_metrics, processed_urls, args.max_pages))

    write_report(rows, Path(args.output))
    print(f"wrote {len(rows)} rows to {args.output}")
    for line in summarize_rows(rows):
        print(line)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
