from __future__ import annotations

import argparse
import csv
import sys
import urllib.request
from pathlib import Path

from bs4 import BeautifulSoup

from config import DEPARTMENT_HIERARCHY_CSV_PATH

DEFAULT_URL = "https://www.pref.saga.lg.jp/classset002.html"


def parse_department_hierarchy(html: str) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    root = soup.select_one("div.classArea")
    if not root:
        return []

    rows: list[dict[str, str]] = []
    for section in root.select("div.class"):
        heading = section.select_one("h2.midashi a")
        if not heading:
            continue
        top_name = heading.get_text(" ", strip=True)
        top_url = heading.get("href", "").strip()
        child_items = section.select("ul.child > li")
        if not child_items:
            rows.append(
                {
                    "top_unit": top_name,
                    "top_url": top_url,
                    "child_name": "",
                    "child_url": "",
                    "tel": "",
                    "fax": "",
                }
            )
            continue

        for item in child_items:
            link = item.select_one("a")
            tel = ""
            fax = ""
            tel_node = item.select_one(".renraku .tel")
            fax_node = item.select_one(".renraku .fax")
            if tel_node:
                tel = tel_node.get_text(" ", strip=True).replace("TEL：", "").strip()
            if fax_node:
                fax = fax_node.get_text(" ", strip=True).replace("FAX：", "").strip()
            rows.append(
                {
                    "top_unit": top_name,
                    "top_url": top_url,
                    "child_name": link.get_text(" ", strip=True) if link else "",
                    "child_url": (link.get("href", "") if link else "").strip(),
                    "tel": tel,
                    "fax": fax,
                }
            )
    return rows


def fetch_html(url: str) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as response:
        return response.read().decode("utf-8")


def write_department_hierarchy(rows: list[dict[str, str]], output_path: Path | str = DEPARTMENT_HIERARCHY_CSV_PATH) -> Path:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["top_unit", "top_url", "child_name", "child_url", "tel", "fax"])
        writer.writeheader()
        writer.writerows(rows)
    return path


def main() -> None:
    parser = argparse.ArgumentParser(description="Build department hierarchy CSV from Saga organization page.")
    parser.add_argument("--url", default=DEFAULT_URL)
    parser.add_argument("--output", default=str(DEPARTMENT_HIERARCHY_CSV_PATH))
    parser.add_argument("--stdin", action="store_true", help="Read HTML from stdin instead of fetching URL.")
    args = parser.parse_args()

    html = sys.stdin.read() if args.stdin else fetch_html(args.url)
    rows = parse_department_hierarchy(html)
    path = write_department_hierarchy(rows, args.output)
    print(f"department hierarchy written: rows={len(rows)} path={path}")


if __name__ == "__main__":
    main()
