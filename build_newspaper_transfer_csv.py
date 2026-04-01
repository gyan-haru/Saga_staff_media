from __future__ import annotations

import argparse
import csv
import re
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from extractor import (
    clean_department_name,
    clean_person_name,
    extract_article_title,
    format_date_iso,
    get_html,
    is_valid_person_name,
    normalize_text,
)

TRANSFER_FIELDNAMES = [
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
    "publisher",
    "raw_text",
]

NAME_PATTERN = re.compile(r"([^\s()（）=＝]{1,8}(?:\s+[^\s()（）=＝]{1,8})?)$")
DATE_PATTERN = re.compile(r"((?:令和|平成)\s*\d+\s*年\s*\d+\s*月\s*\d+\s*日|\d{4}\s*年\s*\d+\s*月\s*\d+\s*日)")
FEATURE_TITLE_PATTERN = re.compile(r"人事異動")
ARTICLE_URL_PATTERN = re.compile(r"^https://www\.saga-s\.co\.jp/articles/-/\d+/?$")

TOP_LEVEL_SUFFIXES = (
    "事務局",
    "委員会",
    "事務所",
    "センター",
    "学園",
    "学院",
    "大学校",
    "試験場",
    "博物館",
    "美術館",
    "図書館",
    "歴史館",
    "学校",
    "部",
    "局",
    "課",
    "室",
    "所",
    "館",
    "場",
)

DEPARTMENT_SUFFIXES = TOP_LEVEL_SUFFIXES + (
    "グループ",
    "チーム",
    "市",
    "町",
    "村",
)

TITLE_SUFFIX_TO_UNIT = (
    ("副事務局長", "事務局"),
    ("事務局長", "事務局"),
    ("副センター長", "センター"),
    ("センター長", "センター"),
    ("副部長", "部"),
    ("部長", "部"),
    ("副局長", "局"),
    ("局長", "局"),
    ("副課長", "課"),
    ("課長", "課"),
    ("副室長", "室"),
    ("室長", "室"),
    ("副所長", "所"),
    ("所長", "所"),
    ("統括副館長", "館"),
    ("副館長", "館"),
    ("館長", "館"),
    ("副校長", "校"),
    ("校長", "校"),
    ("副場長", "場"),
    ("場長", "場"),
)

TITLE_PATTERNS = [
    "主任児童自立支援専門員",
    "主任職業指導員",
    "副主任技術員",
    "主任技術員",
    "主任主査",
    "統括会計・監査専任監",
    "統括税務専任監",
    "統括副館長",
    "政策統括監",
    "政策企画監",
    "政策企画主幹",
    "副事務局長",
    "事務局長",
    "副センター長",
    "センター長",
    "副セン長",
    "副部長",
    "部長",
    "副局長",
    "局長",
    "副課長",
    "課長",
    "副室長",
    "室長",
    "副所長",
    "所長",
    "副館長",
    "館長",
    "副校長",
    "校長",
    "副場長",
    "場長",
    "副所長",
    "専門員",
    "指導員",
    "技師",
    "主査",
    "リーダー",
    "技術監",
    "保健監",
    "参事",
    "監査監",
    "副局長",
    "副館長",
    "副校長",
    "会計管理者",
]
TITLE_PATTERNS.sort(key=len, reverse=True)


def read_source_urls(source_index_path: Path, year: int) -> list[str]:
    urls: list[str] = []
    with source_index_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if int((row.get("year") or "0").strip() or 0) != year:
                continue
            if (row.get("source_type") or "").strip() != "newspaper_transfer_list":
                continue
            url = (row.get("url") or "").strip()
            if url:
                urls.append(url)
    return urls


def discover_article_links(feature_html: str, base_url: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(feature_html, "html.parser")
    seen: set[str] = set()
    results: list[dict[str, str]] = []
    article_body = soup.select_one(".article-body")
    heading = None
    if article_body:
        for candidate in article_body.select("h2, h3"):
            if "佐賀県職員人事" in normalize_text(candidate.get_text(" ", strip=True)):
                heading = candidate
                break

    if heading:
        sibling = heading.parent if heading.parent is not None else heading
        current = sibling.find_next_sibling()
        while current is not None:
            if current.name and current.name.lower() in {"div", "section"}:
                nested_heading = current.find(["h2", "h3"])
                if nested_heading and "佐賀県職員人事" not in normalize_text(nested_heading.get_text(" ", strip=True)):
                    break
            if current.name and current.name.lower() in {"h2", "h3"}:
                if "佐賀県職員人事" not in normalize_text(current.get_text(" ", strip=True)):
                    break
            for anchor in current.select("a[href]"):
                href = urljoin(base_url, anchor.get("href") or "")
                if not ARTICLE_URL_PATTERN.match(href):
                    continue
                if href in seen:
                    continue
                seen.add(href)
                results.append(
                    {
                        "url": href,
                        "title": normalize_text(anchor.get_text(" ", strip=True)),
                    }
                )
            current = current.find_next_sibling()

    if results:
        return results

    for anchor in soup.select("a[href]"):
        href = urljoin(base_url, anchor.get("href") or "")
        if not ARTICLE_URL_PATTERN.match(href):
            continue
        text = normalize_text(anchor.get_text(" ", strip=True))
        if not FEATURE_TITLE_PATTERN.search(text):
            continue
        if href in seen:
            continue
        seen.add(href)
        results.append({"url": href, "title": text})
    return results


def extract_article_body(soup: BeautifulSoup):
    body = soup.select_one(".article-body")
    if body:
        return body
    return soup.select_one("article") or soup.body or soup


def extract_published_at(soup: BeautifulSoup) -> str:
    meta = soup.select_one('meta[property="article:published_time"]')
    if meta:
        content = (meta.get("content") or "").strip()
        match = re.match(r"(\d{4}-\d{2}-\d{2})", content)
        if match:
            return match.group(1)
    return ""


def extract_effective_date(title: str, body_text: str, published_at: str) -> str:
    default_year = int(published_at[:4]) if re.match(r"\d{4}-\d{2}-\d{2}", published_at) else None
    for candidate in (title, body_text):
        match = DATE_PATTERN.search(candidate)
        if match:
            return format_date_iso(match.group(1), default_year=default_year)
    return published_at


def extract_same_prefix(resolved_assignment: str) -> str:
    normalized = normalize_text(resolved_assignment)
    for suffix in TOP_LEVEL_SUFFIXES:
        index = normalized.find(suffix)
        if index >= 0:
            return normalized[: index + len(suffix)]
    return normalized


def expand_same_reference(text: str, previous_assignment: str, same_prefix: str) -> str:
    normalized = normalize_text(text)
    if not normalized.startswith("同"):
        return normalized
    rest = normalized[1:]
    if not rest:
        return previous_assignment or same_prefix
    if rest.startswith(("=", "＝")):
        return f"{same_prefix}{rest}" if same_prefix else rest.lstrip("=＝")
    return f"{same_prefix}{rest}" if same_prefix else rest


def split_assignment_parts(assignment_text: str) -> tuple[str, str]:
    normalized = normalize_text(assignment_text)
    match = re.match(r"^(.*?)[（(]([^()（）]+)[)）]$", normalized)
    if not match:
        return normalized, ""
    return match.group(1).strip(), match.group(2).strip()


def guess_unit_from_title(title: str) -> str:
    for suffix, unit in TITLE_SUFFIX_TO_UNIT:
        if title.endswith(suffix):
            return unit
    return ""


def looks_like_department(text: str) -> bool:
    return any(text.endswith(suffix) for suffix in DEPARTMENT_SUFFIXES)


def find_last_department_split(text: str) -> int:
    best_index = -1
    best_length = -1
    for suffix in DEPARTMENT_SUFFIXES:
        index = text.rfind(suffix)
        if index < 0:
            continue
        end = index + len(suffix)
        if end >= len(text):
            continue
        if end > best_index or (end == best_index and len(suffix) > best_length):
            best_index = end
            best_length = len(suffix)
    return best_index


def split_department_and_title(text: str) -> tuple[str, str]:
    normalized = normalize_text(text).strip(" =＝")
    if not normalized:
        return "", ""

    first_assignment = re.split(r"[=＝]", normalized, maxsplit=1)[0].strip()
    for title in TITLE_PATTERNS:
        start = first_assignment.find(title)
        if start < 0:
            continue
        prefix = first_assignment[:start]
        unit = guess_unit_from_title(title)
        if looks_like_department(prefix):
            department = prefix
        elif unit:
            department = f"{prefix}{unit}"
        else:
            department = prefix
        cleaned_department = clean_department_name(department)
        if cleaned_department and looks_like_department(cleaned_department):
            return cleaned_department, first_assignment[start:].strip()

    cleaned_first = clean_department_name(first_assignment)
    if cleaned_first and looks_like_department(cleaned_first):
        return cleaned_first, normalized[len(first_assignment):].strip(" =＝")

    split_at = find_last_department_split(first_assignment)
    if split_at > 0:
        department = clean_department_name(first_assignment[:split_at])
        title = first_assignment[split_at:].strip()
        if department and title:
            return department, title
    return "", normalized


def parse_transfer_line(
    line: str,
    current_section: str,
    article_title: str,
    previous_assignment: str,
    same_prefix: str,
) -> tuple[dict[str, str] | None, str, str]:
    normalized = normalize_text(line)
    if not normalized or "人事異動" in normalized:
        return None, previous_assignment, same_prefix

    match = NAME_PATTERN.search(normalized)
    if not match:
        return None, previous_assignment, same_prefix

    person_name = clean_person_name(match.group(1))
    if not is_valid_person_name(person_name):
        return None, previous_assignment, same_prefix

    assignment_text = normalized[: match.start()].strip()
    to_part, from_part = split_assignment_parts(assignment_text)
    resolved_to = expand_same_reference(to_part, previous_assignment, same_prefix)
    resolved_from = expand_same_reference(from_part, previous_assignment, same_prefix)

    if "退職" in article_title and not from_part:
        resolved_from = resolved_to
        resolved_to = ""

    to_department, to_title = split_department_and_title(resolved_to)
    from_department, from_title = split_department_and_title(resolved_from)

    if resolved_to:
        updated_previous = resolved_to
        updated_prefix = extract_same_prefix(resolved_to)
    else:
        updated_previous = previous_assignment
        updated_prefix = same_prefix

    record = {
        "section": current_section,
        "person_name": person_name,
        "from_department": from_department,
        "to_department": to_department,
        "from_title": from_title,
        "to_title": to_title,
        "evidence_snippet": normalized,
    }
    return record, updated_previous, updated_prefix


def extract_article_rows(article_html: str, article_url: str) -> dict[str, Any]:
    soup = BeautifulSoup(article_html, "html.parser")
    title = extract_article_title(soup, fallback=article_url)
    body = extract_article_body(soup)
    body_text = normalize_text(body.get_text("\n", strip=True)) if body else ""
    published_at = extract_published_at(soup)
    effective_date = extract_effective_date(title, body_text, published_at)

    current_section = ""
    previous_assignment = ""
    same_prefix = ""
    rows: list[dict[str, str]] = []

    for element in body.find_all(["h3", "li"]):  # type: ignore[union-attr]
        text = normalize_text(element.get_text(" ", strip=True))
        if not text:
            continue
        if element.name == "h3":
            current_section = text
            previous_assignment = ""
            same_prefix = ""
            continue
        parsed, previous_assignment, same_prefix = parse_transfer_line(
            text,
            current_section,
            title,
            previous_assignment,
            same_prefix,
        )
        if not parsed:
            continue
        rows.append(parsed)

    return {
        "title": title,
        "published_at": published_at,
        "effective_date": effective_date,
        "raw_text": body_text,
        "rows": rows,
    }


def build_rows_for_year(year: int, source_index_path: Path) -> list[dict[str, str]]:
    feature_urls = read_source_urls(source_index_path, year)
    if not feature_urls:
        raise ValueError(f"{year}年の newspaper_transfer_list が {source_index_path} にありません")

    all_rows: list[dict[str, str]] = []
    for feature_url in feature_urls:
        feature_html = get_html(feature_url)
        if not feature_html:
            raise RuntimeError(f"failed to fetch feature page: {feature_url}")
        article_links = discover_article_links(feature_html, feature_url)
        for article in article_links:
            article_html = get_html(article["url"])
            if not article_html:
                continue
            parsed = extract_article_rows(article_html, article["url"])
            for row in parsed["rows"]:
                all_rows.append(
                    {
                        "source_type": "newspaper_transfer_list",
                        "title": parsed["title"],
                        "url": article["url"],
                        "published_at": parsed["published_at"],
                        "effective_date": parsed["effective_date"],
                        "person_name": row["person_name"],
                        "from_department": row["from_department"],
                        "to_department": row["to_department"],
                        "from_title": row["from_title"],
                        "to_title": row["to_title"],
                        "evidence_snippet": row["evidence_snippet"],
                        "publisher": "佐賀新聞",
                        "raw_text": parsed["raw_text"],
                    }
                )
    return all_rows


def write_rows(output_path: Path, rows: list[dict[str, str]]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=TRANSFER_FIELDNAMES)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> int:
    parser = argparse.ArgumentParser(description="佐賀新聞の人事異動特集から年次CSVを生成する")
    parser.add_argument("year", type=int, help="対象年")
    parser.add_argument(
        "--source-index",
        default="data/transfers/source_index.csv",
        help="人事異動ソース一覧CSV",
    )
    parser.add_argument(
        "--output",
        default="",
        help="出力先CSV。未指定なら data/transfers/newspaper_<year>.csv",
    )
    args = parser.parse_args()

    source_index_path = Path(args.source_index)
    output_path = Path(args.output) if args.output else Path(f"data/transfers/newspaper_{args.year}.csv")

    rows = build_rows_for_year(args.year, source_index_path)
    write_rows(output_path, rows)
    print(f"rows: {len(rows)}")
    print(f"output: {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
