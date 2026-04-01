from __future__ import annotations

import datetime as dt
import hashlib
import io
import re
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from functools import lru_cache
from typing import Iterable

import fitz  # PyMuPDF
import requests
from bs4 import BeautifulSoup

from config import (
    BASE_URL,
    GENERIC_PRESS_RELEASE_LABELS,
    GENERIC_PROPOSAL_LABELS,
    LIST_SOURCES,
    PRESS_RELEASE_KEYWORDS,
    PROPOSAL_BROAD_KEYWORDS,
    PROPOSAL_KEYWORDS,
)

try:
    fitz.TOOLS.mupdf_display_errors(False)
    fitz.TOOLS.mupdf_display_warnings(False)
except Exception:
    pass

KANJI_DIGITS = {
    "零": 0, "〇": 0,
    "一": 1, "二": 2, "三": 3, "四": 4, "五": 5,
    "六": 6, "七": 7, "八": 8, "九": 9,
}
SMALL_UNITS = {"十": 10, "百": 100, "千": 1000}
LARGE_UNITS = {"万": 10_000, "億": 100_000_000, "兆": 1_000_000_000_000}
DATE_PATTERN = r"((?:令和|平成)\s*\d+\s*年\s*\d+\s*月\s*\d+\s*日|\d{4}\s*年\s*\d+\s*月\s*\d+\s*日|\d{4}[/-]\d{1,2}[/-]\d{1,2})"
DEADLINE_DATE_PATTERN = r"((?:令和|平成)\s*\d+\s*年\s*\d+\s*月\s*\d+\s*日|\d{4}\s*年\s*\d+\s*月\s*\d+\s*日|\d{4}[/-]\d{1,2}[/-]\d{1,2}|\d{1,2}\s*月\s*\d{1,2}\s*日)"
DEPARTMENT_PATTERN = r"(?:県|市|庁|課|部|局|室|センター|所|班|係|チーム|事務局|担当|グループ|学科|事業所)"
DEPARTMENT_SUFFIX_PATTERN = r"(?:課|部|局|室|センター|所|班|係|チーム|事務局|担当|グループ|学科|事業所)"
CONTACT_TRIGGER_PATTERN = r"(?:提出先|提出場所|問合せ先|問い合わせ先|問合わせ先|お問合せ先|お問合わせ先|申込先|担当窓口|担当部署|担当課|連絡先|書類等提出先及び問い合わせ先|に関するお問い合わせ|発注者)"
KANJI_NUMBER_PATTERN = r"[〇零一二三四五六七八九十百千万億兆\d]+(?=\s*(?:年|月|日|時|分|秒|円|千円|万円|億円|件|人|回|部|社|校|丁目|番地?|号|年度|%))"
PERSON_LABEL_PATTERN = r"(?:担当者?|担当者名|氏名|連絡担当者|担当窓口|担当)"
PERSON_ANNOTATION_PATTERN = rf"[（(【\[]\s*{PERSON_LABEL_PATTERN}\s*[:：]\s*(.+?)\s*[)）】\]]"
NAME_CHAR_CLASS = r"\u3400-\u9FFF\uF900-\uFAFF々ぁ-んァ-ヶーA-Za-z"
NAME_PART_PATTERN = rf"[{NAME_CHAR_CLASS}]+"
ROLE_TITLE_SUFFIX_PATTERN = r"(?:部長|課長|室長|局長|班長|係長|主査|主幹|主任|次長|参事|監|担当|校長|副校長|教頭|教諭)"
ATTACHED_ROLE_TITLE_PATTERN = r"(?:班長|係長|主査|主幹|主任|次長|参事|監|担当|校長|副校長|教頭|教諭)"
ATTACHED_ROLE_SUFFIX_PATTERN = r"(?:長|主任|主査|主幹|次長|参事|監|担当|校長|副校長|教頭|教諭)"
BAD_PERSON_EXACT = {
    "問い合わせ先",
    "問合せ先",
    "提出書類",
    "提案書の提出",
    "担当者",
    "担当者名",
    "連絡先",
    "所在地",
    "氏名",
    "名",
    "部署",
    "部署名",
    "契約事項",
    "事業",
    "佐賀県",
    "号",
    "給与",
    "企画",
    "歴史",
    "以下",
    "定員",
    "Street",
    "Web開催",
    "玄海創生",
    "文化財保護",
    "時までの",
    "時までに書留",
    "海岸",
    "calogeras",
    "県下全域",
}
BAD_PERSON_SUBSTRINGS = (
    "問い合わせ",
    "問合せ",
    "提出",
    "様式",
    "仕様書",
    "資料",
    "メール",
    "E-mail",
    "Email",
    "電話",
    "直通",
    "内線",
    "担当",
    "事務局",
    "委員会",
    "協議会",
    "株式会社",
    "法人",
    "http",
    "https",
    "@",
    "業務",
    "リスト",
    "ページ",
    "ミュージアム",
    "シンポジウム",
    "Web",
    "開催",
    "試験場",
    "記名",
    "修了",
    "について",
    "書留",
    "グループ",
    "学科",
    "エリア",
    "周辺",
    "全域",
    "により",
    "ことにより",
    "主として",
)
BAD_PERSON_PREFIXES = (
    "佐賀県",
    "県立",
    "市立",
    "町立",
    "村立",
)
SAGA_MUNICIPALITY_PREFIXES = (
    "佐賀市",
    "唐津市",
    "鳥栖市",
    "多久市",
    "伊万里市",
    "武雄市",
    "鹿島市",
    "小城市",
    "嬉野市",
    "神埼市",
    "吉野ヶ里町",
    "基山町",
    "上峰町",
    "みやき町",
    "玄海町",
    "有田町",
    "大町町",
    "江北町",
    "白石町",
    "太良町",
)
BAD_PERSON_FACILITY_TERMS = (
    "博物館",
    "図書館",
    "美術館",
    "資料館",
    "記念館",
    "文学館",
    "科学館",
    "会館",
    "ホール",
    "庁舎",
    "県庁",
    "役場",
    "病院",
    "学校",
    "高校",
    "中学校",
    "小学校",
    "大学",
    "学園",
    "研究所",
    "体育館",
)
BAD_DEPARTMENT_EXACT = {
    "問い合わせ先",
    "問合せ先",
    "問合わせ先",
    "問い合せ先",
    "お問合せ先",
    "お問合わせ先",
    "問い合わせ",
    "問合せ",
    "問合わせ",
    "提出先",
    "提出場所",
    "担当課",
    "担当部署",
    "本件に係る問い合わせ先",
    "本件に係る問合せ先",
    "本件に係る問合わせ先",
    "本件に関する問い合わせ先",
    "本件に関する問合せ先",
    "本件に関する問合わせ先",
    "プレゼンテーションの日程及び場所",
    "履行場所",
    "開札場所",
    "納入場所",
    "実施場所",
    "交付場所",
    "開催場所",
    "会場",
    "業務場所",
}
BAD_DEPARTMENT_PREFIXES = (
    "本 文",
    "本文",
    "プレゼンテーション",
    "企画提案書",
    "工事成績",
    "なお、",
    "なお",
    "法人の場合",
    "受託者名",
    "代表者職",
    "代表者氏名",
    "氏名",
    "職・氏名",
    "な職氏名",
    "記録者",
    "申込書提出先",
    "申請書提出先",
    "提出書類提出先",
    "提出書類送付先",
    "送付先",
    "郵送先",
    "お問い合わせ先",
    "お問い合せ先",
    "事務所住所",
)
BAD_DEPARTMENT_SUBSTRINGS = (
    "担当部署名",
    "参加者氏名",
    "会社名等",
    "会社概要",
    "パンフレット",
    "実績書",
    "誓約書",
    "代表者",
    "責任者",
    "事務担当者",
    "担当者を明確",
    "担当者の範囲",
    "氏名欄",
    "判読不可能",
    "企画提案書の内容",
    "プレゼンテーションは参加者毎",
    "結果の通知",
    "工事成績",
    "記録簿",
    "丁目",
    "番地",
    "号室",
    "オフィス",
    "マンション",
    "ビル",
)


@dataclass
class CrawledLink:
    title: str
    url: str
    source_type: str
    source_list_url: str = ""
    source_department_name: str = ""


@dataclass
class PersonMention:
    department_name: str
    person_name: str
    person_key: str
    person_role: str
    contact_email: str
    contact_phone: str
    extracted_section: str
    name_quality: str
    source_confidence: float

    @classmethod
    def empty(cls) -> "PersonMention":
        return cls(
            department_name="",
            person_name="",
            person_key="",
            person_role="",
            contact_email="",
            contact_phone="",
            extracted_section="",
            name_quality="unknown",
            source_confidence=0.0,
        )


@dataclass
class ProjectRecord:
    title: str
    url: str
    source_type: str
    summary: str
    purpose: str
    budget: str
    application_deadline: str
    submission_deadline: str
    published_at: str
    raw_text: str
    html_text: str
    pdf_urls: list[str]
    zip_urls: list[str]
    person_mentions: list[PersonMention]
    fetched_at: str
    source_list_url: str = ""
    source_department_name: str = ""

    @property
    def primary_mention(self) -> PersonMention:
        if not self.person_mentions:
            return PersonMention.empty()
        return max(
            self.person_mentions,
            key=lambda mention: (
                1 if mention.person_name else 0,
                mention.source_confidence,
                1 if (mention.contact_email or mention.contact_phone) else 0,
                len(mention.department_name),
            ),
        )

    @property
    def department_name(self) -> str:
        return self.primary_mention.department_name

    @property
    def person_name(self) -> str:
        return self.primary_mention.person_name

    @property
    def person_key(self) -> str:
        return self.primary_mention.person_key

    @property
    def person_role(self) -> str:
        return self.primary_mention.person_role

    @property
    def contact_email(self) -> str:
        return self.primary_mention.contact_email

    @property
    def contact_phone(self) -> str:
        return self.primary_mention.contact_phone

    @property
    def extracted_section(self) -> str:
        return self.primary_mention.extracted_section


def get_html(url: str) -> str | None:
    try:
        return fetch_url_bytes(url).decode("utf-8")
    except Exception as exc:
        print(f"Error fetching {url}: {exc}")
        return None


def fetch_url_bytes(url: str, *, attempts: int = 3, timeout: int = 20) -> bytes:
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            last_exc = exc
            if 400 <= exc.code < 500 and exc.code not in {408, 429}:
                break
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            last_exc = exc

        if attempt < attempts:
            time.sleep(min(0.5 * attempt, 1.5))

    if last_exc:
        raise last_exc
    raise RuntimeError(f"Failed to fetch {url}")


def extract_pdf_text_from_bytes(pdf_data: bytes, source_label: str) -> str:
    if not pdf_data:
        return ""

    doc = None
    try:
        doc = fitz.open(stream=pdf_data, filetype="pdf")
        return "\n".join(page.get_text() for page in doc)
    except Exception as exc:
        print(f"Error extracting PDF {source_label}: {exc}")
        return ""
    finally:
        if doc is not None:
            doc.close()


def extract_pdf_text(pdf_url: str) -> str:
    try:
        pdf_data = fetch_url_bytes(pdf_url)
    except Exception as exc:
        print(f"Error fetching PDF {pdf_url}: {exc}")
        return ""
    return extract_pdf_text_from_bytes(pdf_data, pdf_url)


def extract_zip_pdfs_text(zip_url: str) -> str:
    combined_text = ""
    try:
        zip_data = fetch_url_bytes(zip_url)

        with zipfile.ZipFile(io.BytesIO(zip_data)) as archive:
            for file_info in archive.infolist():
                if not file_info.filename.lower().endswith(".pdf"):
                    continue

                pdf_bytes = archive.read(file_info.filename)
                pdf_text = extract_pdf_text_from_bytes(pdf_bytes, f"{file_info.filename} from ZIP")
                if pdf_text:
                    combined_text += pdf_text + "\n---\n"
    except Exception as exc:
        print(f"Error extracting ZIP {zip_url}: {exc}")

    return combined_text


def kanji_number_to_int(kanji: str) -> int | None:
    if not kanji:
        return None
    if re.fullmatch(r"\d+", kanji):
        return int(kanji)

    total = 0
    section = 0
    number = 0
    has_any = False

    for ch in kanji:
        if ch.isdigit():
            number = number * 10 + int(ch)
            has_any = True
        elif ch in KANJI_DIGITS:
            number = KANJI_DIGITS[ch]
            has_any = True
        elif ch in SMALL_UNITS:
            unit = SMALL_UNITS[ch]
            if number == 0:
                number = 1
            section += number * unit
            number = 0
            has_any = True
        elif ch in LARGE_UNITS:
            unit = LARGE_UNITS[ch]
            if number == 0 and section == 0:
                section = 1
            total += (section + number) * unit
            section = 0
            number = 0
            has_any = True
        else:
            return None

    if not has_any:
        return None
    return total + section + number


def replace_kanji_numbers_in_text(text: str) -> str:
    def repl(match: re.Match[str]) -> str:
        raw = match.group(0)
        value = kanji_number_to_int(raw)
        return str(value) if value is not None else raw

    return re.sub(KANJI_NUMBER_PATTERN, repl, text)


def normalize_text(text: str) -> str:
    if not text:
        return ""

    text = unicodedata.normalize("NFKC", text)
    text = text.replace("〆切", "締切")
    text = text.replace("締め切り", "締切")
    text = text.replace("しめきり", "締切")
    text = text.replace("提出期限", "提出締切")
    text = text.replace("参加期限", "参加申込締切")
    text = text.replace("〆", "締")
    text = text.replace("令和元年", "令和1年")
    text = text.replace("平成元年", "平成1年")
    text = replace_kanji_numbers_in_text(text)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n+", "\n", text)
    return text.strip()


def compact_text(text: str) -> str:
    return normalize_text(text).replace(" ", "")


@lru_cache(maxsize=1)
def known_department_labels() -> tuple[str, ...]:
    labels: set[str] = set()
    generic_labels = GENERIC_PRESS_RELEASE_LABELS | GENERIC_PROPOSAL_LABELS
    for source in LIST_SOURCES:
        label = normalize_text((source.get("department_name") or "").strip())
        if not label or label in generic_labels:
            continue
        labels.add(label)
        labels.add(label.replace(" ", ""))
    return tuple(sorted(labels, key=len, reverse=True))


def contains_known_department_label(text: str) -> bool:
    compact = compact_text(text)
    if not compact:
        return False
    return any(label.replace(" ", "") in compact for label in known_department_labels())


def normalize_person_name(name: str) -> str:
    name = normalize_text(name)
    name = re.sub(r"(様|さん|氏|殿|担当)$", "", name).strip()
    name = name.replace(" ", "")
    return name


def build_person_key(name: str) -> str:
    normalized = normalize_person_name(name)
    if not normalized:
        return ""
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]


def extract_content_root(soup: BeautifulSoup):
    for selector in ("article", "#tmp_contents", "main", "#main"):
        node = soup.select_one(selector)
        if node:
            return node
    return soup.body or soup


def extract_article_title(soup: BeautifulSoup, fallback: str = "") -> str:
    root = extract_content_root(soup)
    title_tag = root.select_one("h1") if root else None
    if not title_tag:
        title_tag = soup.select_one("h1.title, h1")
    return normalize_text(title_tag.get_text(" ", strip=True)) if title_tag else normalize_text(fallback)


def extract_main_text(soup: BeautifulSoup) -> str:
    root = extract_content_root(soup)
    return normalize_text(root.get_text(separator="\n", strip=True))


def extract_published_at_from_soup(soup: BeautifulSoup) -> str:
    root = extract_content_root(soup)
    time_tag = root.select_one(".updDate time[datetime], time[datetime]") if root else None
    if not time_tag:
        return ""
    datetime_attr = (time_tag.get("datetime") or "").strip()
    match = re.match(r"(\d{4}-\d{2}-\d{2})", datetime_attr)
    return match.group(1) if match else ""


def _safe_datetime(year: int, month: int, day: int) -> dt.datetime | None:
    try:
        return dt.datetime(year, month, day)
    except ValueError:
        return None


def parse_japanese_date(date_str: str, default_year: int | None = None) -> dt.datetime | None:
    if not date_str:
        return None

    date_str = re.sub(r"\s+", "", normalize_text(date_str))

    match = re.search(r"令和(\d+)年(\d+)月(\d+)日", date_str)
    if match:
        y, m, d = int(match.group(1)), int(match.group(2)), int(match.group(3))
        return _safe_datetime(2018 + y, m, d)

    match = re.search(r"平成(\d+)年(\d+)月(\d+)日", date_str)
    if match:
        y, m, d = int(match.group(1)), int(match.group(2)), int(match.group(3))
        return _safe_datetime(1988 + y, m, d)

    match = re.search(r"(\d{4})年(\d+)月(\d+)日", date_str)
    if match:
        y, m, d = int(match.group(1)), int(match.group(2)), int(match.group(3))
        return _safe_datetime(y, m, d)

    match = re.search(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", date_str)
    if match:
        y, m, d = int(match.group(1)), int(match.group(2)), int(match.group(3))
        return _safe_datetime(y, m, d)

    match = re.search(r"(\d{1,2})月(\d{1,2})日", date_str)
    if match and default_year:
        m, d = int(match.group(1)), int(match.group(2))
        inferred_year = default_year
        parsed = _safe_datetime(inferred_year, m, d)
        if parsed:
            return parsed
        return None

    return None


def format_date_iso(date_str: str, default_year: int | None = None) -> str:
    parsed = parse_japanese_date(date_str, default_year=default_year)
    return parsed.date().isoformat() if parsed else ""


def find_deadline(text: str, keywords: Iterable[str]) -> str:
    lines = text.split("\n")
    for i, line in enumerate(lines):
        for keyword in keywords:
            if keyword in line:
                for candidate_line in lines[i:i + 8]:
                    match = re.search(DEADLINE_DATE_PATTERN, candidate_line)
                    if match:
                        return match.group(1)
    return ""


def find_budget(text: str) -> str:
    text = normalize_text(text)
    keyword_pattern = r"(委託上限額|上限額|限度額|上限金額|予定価格|委託料上限額)"
    amount_pattern = r"([\d,]+)\s*(円|千円|万円|億円)"

    match = re.search(keyword_pattern + r".{0,40}?" + amount_pattern, text)
    if match:
        amount = match.group(2).replace(",", "")
        unit = match.group(3)
        return f"{amount}{unit}"

    match = re.search(r"金\s*([\d,]+)\s*(円|千円|万円|億円)", text)
    if match:
        return f"{match.group(1).replace(',', '')}{match.group(2)}"

    match = re.search(r"([\d,]+)\s*(円|千円|万円|億円)\s*[(（]消費税", text)
    if match:
        return f"{match.group(1).replace(',', '')}{match.group(2)}"

    return ""


def find_purpose(text: str) -> str:
    lines = text.split("\n")
    purpose_lines: list[str] = []
    capture = False

    for line in lines:
        stripped = line.strip()
        if re.search(r"^(【?目的】?|【?事業の趣旨】?|【?趣旨】?|1\s+目的|1\.目的|1 目的)$", stripped):
            capture = True
            continue

        if capture:
            if re.search(r"^([2-9２-９]\s|[2-9２-９]\.|\(|【)", stripped) and purpose_lines:
                break
            if stripped:
                purpose_lines.append(stripped)
            if len(purpose_lines) > 6:
                break

    return " ".join(purpose_lines) if purpose_lines else ""


def detect_published_at(text: str, html_published_at: str = "") -> str:
    if html_published_at:
        return html_published_at

    lines = [line.strip() for line in normalize_text(text).split("\n") if line.strip()]
    labels = ("最終更新日", "更新日", "掲載日", "公開日", "公表日", "発表日")

    for i, line in enumerate(lines[:20]):
        if any(label in line for label in labels):
            match = re.search(DATE_PATTERN, " ".join(lines[i:i + 3]))
            if match:
                return format_date_iso(match.group(1))

    for line in lines[:12]:
        match = re.search(DATE_PATTERN, line)
        if match:
            return format_date_iso(match.group(1))

    return ""


def extract_contact_info(text: str) -> tuple[str, str]:
    normalized = normalize_text(text)
    email_match = re.search(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", normalized)
    phone_match = re.search(r"\d{2,4}-\d{2,4}-\d{3,4}", normalized)
    return (email_match.group(0) if email_match else "", phone_match.group(0) if phone_match else "")


def strip_wrapping_brackets(text: str) -> str:
    stripped = text.strip()
    if re.match(r"^[（(【\[]\d+[\)）】\]]", stripped):
        return stripped

    pairs = {"(": ")", "（": "）", "[": "]", "【": "】"}
    while len(stripped) >= 2 and pairs.get(stripped[:1]) == stripped[-1:]:
        stripped = stripped[1:-1].strip()

    return stripped


def extract_person_from_annotation(line: str) -> str:
    match = re.search(PERSON_ANNOTATION_PATTERN, normalize_text(line))
    if not match:
        return ""
    person = clean_person_name(match.group(1))
    return person if is_valid_person_name(person) else ""


def extract_people_from_annotation(line: str) -> list[str]:
    match = re.search(PERSON_ANNOTATION_PATTERN, normalize_text(line))
    if not match:
        return []
    return extract_person_names(match.group(1))


def strip_person_role_prefix(text: str) -> str:
    normalized = normalize_text(text).strip()
    if not normalized:
        return ""
    match = re.match(rf"^(?:[^\s]{{0,24}}?{ROLE_TITLE_SUFFIX_PATTERN})\s+(.+)$", normalized)
    if match:
        return match.group(1).strip()
    return normalized


def clean_person_name(text: str) -> str:
    if not text:
        return ""
    text = normalize_text(text)
    text = re.sub(r"^(担当者?|担当者名|氏名|連絡担当者|担当窓口|担当|連絡先)[\s:：]*", "", text)
    text = re.sub(r"[（(【\[].*?[)）】\]]", "", text)
    text = re.split(r"(?:内線|直通|電話|TEL|Tel|Fax|FAX|メール|E-mail|Email|Mail|〒|https?://|@)", text, maxsplit=1)[0]
    text = re.sub(r"\s+\d+$", "", text)
    text = re.sub(r"\s+", " ", text).strip(" 　:：,、/／-")
    text = strip_person_role_prefix(text)
    text = re.sub(r"\s*(?:氏|様|さん)$", "", text)

    for separator in ("、", ",", "，", "/", "／"):
        if separator in text:
            parts = [part.strip() for part in text.split(separator) if part.strip()]
            if len(parts) > 1:
                text = parts[0]
                break

    if "・" in text and " " not in text:
        parts = [part.strip() for part in text.split("・") if part.strip()]
        if len(parts) > 1 and all(looks_like_name_token(part) for part in parts):
            text = parts[0]

    return text.strip()


def extract_person_names(text: str) -> list[str]:
    if not text:
        return []

    text = normalize_text(text)
    text = re.sub(rf"^(?:{PERSON_LABEL_PATTERN}|連絡先)[\s:：]*", "", text)
    text = re.split(r"(?:内線|直通|電話|TEL|Tel|Fax|FAX|メール|E-mail|Email|Mail|〒|https?://|@)", text, maxsplit=1)[0]
    text = text.strip(" 　:：")

    raw_parts = [text]
    for separator in ("、", ",", "，", "/", "／"):
        if separator in text:
            raw_parts = [part.strip() for part in text.split(separator) if part.strip()]
            break

    if raw_parts == [text] and "・" in text and " " not in text:
        candidate_parts = [part.strip() for part in text.split("・") if part.strip()]
        if len(candidate_parts) > 1 and all(looks_like_name_token(part) for part in candidate_parts):
            raw_parts = candidate_parts

    people: list[str] = []
    for part in raw_parts:
        candidate = clean_person_name(part)
        if is_valid_person_name(candidate) and candidate not in people:
            people.append(candidate)
    return people


def classify_person_name_quality(name: str) -> str:
    candidate = clean_person_name(name)
    if not candidate:
        return "unknown"
    if " " in candidate:
        return "full_name"
    if len(candidate) <= 2:
        return "surname_only"
    return "unknown"


def looks_like_name_token(text: str) -> bool:
    candidate = normalize_text(text).strip(" 　:：,、/／-")
    if not candidate or len(candidate) > 6:
        return False
    if any(char.isdigit() for char in candidate):
        return False
    if re.search(DEPARTMENT_SUFFIX_PATTERN, candidate):
        return False
    if candidate in BAD_PERSON_EXACT:
        return False
    if any(token in candidate for token in BAD_PERSON_SUBSTRINGS):
        return False
    if looks_like_non_person_entity(candidate):
        return False
    return bool(re.fullmatch(NAME_PART_PATTERN, candidate))


def looks_like_non_person_entity(text: str) -> bool:
    candidate = clean_person_name(text)
    if not candidate:
        return False
    if candidate.startswith(("その", "主として", "県下")):
        return True
    if candidate.startswith(BAD_PERSON_PREFIXES):
        return True
    if candidate.startswith(SAGA_MUNICIPALITY_PREFIXES):
        return True
    if candidate.endswith(("県内", "市内", "町内", "村内")):
        return True
    if candidate.endswith(("グループ", "学科", "エリア", "全域")):
        return True
    if any(token in candidate for token in BAD_PERSON_FACILITY_TERMS):
        return True
    if re.search(r"(?:都|道|府|県)[^\s]{0,16}(?:市|区|町|村)", candidate):
        return True
    if re.search(r"(?:市|区|町|村)[^\s]{0,12}(?:丁目|番地?|番|号|他)$", candidate):
        return True
    return False


def is_valid_person_name(text: str) -> bool:
    candidate = clean_person_name(text)
    if not candidate or len(candidate) > 12:
        return False
    if any(char.isdigit() for char in candidate):
        return False
    if re.search(DEPARTMENT_SUFFIX_PATTERN, candidate):
        return False
    if candidate in BAD_PERSON_EXACT:
        return False
    if any(token in candidate for token in BAD_PERSON_SUBSTRINGS):
        return False
    if looks_like_non_person_entity(candidate):
        return False
    if candidate.endswith(("する", "ます", "まで", "こと", "先")):
        return False
    if len(candidate.split()) > 2:
        return False
    return bool(re.fullmatch(rf"{NAME_PART_PATTERN}(?: {NAME_PART_PATTERN})?", candidate))


def clean_department_name(text: str) -> str:
    if not text:
        return ""
    text = normalize_text(text)
    text = re.sub(r"^[〇○●◎■□◆◇]\s*", "", text)
    text = re.sub(r"^\(?\d+[.)．]?\)?\s*", "", text)
    text = re.sub(rf"^(?:{CONTACT_TRIGGER_PATTERN})[\s:：]*", "", text)
    text = re.sub(
        r"^(?:本件に(?:係る|関する)\s*)?(?:問い合わせ先|問合せ先|問合わせ先|問い合せ先|問い合わせ|問合せ|問合わせ)[\s:：]*",
        "",
        text,
    )
    text = re.sub(r"^[【\[]\s*(?:問い合わせ先|問合せ先|問合わせ先|お問合せ先|お問合わせ先)\s*[】\]]\s*", "", text)
    text = re.sub(r"^(?:問い合わせ先|問合せ先|問合わせ先|お問合せ先|お問合わせ先)】\s*", "", text)
    text = re.sub(
        r"^(?:申込書提出先|申請書提出先|提出書類提出先|提出書類送付先|送付先|郵送先|お問い合わせ先|お問い合せ先|担当部局|担当部門|担当所属|事務所住所|所在地|住所)[\s:：]*",
        "",
        text,
    )
    if re.match(r"^[^:：]{1,20}[:：]", text):
        label, rest = re.split(r"[:：]", text, maxsplit=1)
        if not re.search(DEPARTMENT_PATTERN, label) and (
            contains_known_department_label(rest) or re.search(DEPARTMENT_PATTERN, rest)
        ):
            text = rest
    text = re.sub(r"^(?:履行場所|開札場所|業務場所|開催場所|会場)[\s:：]*", "", text)
    text = re.sub(r"^担当[\s:：]+", "", text)
    text = re.sub(r"[［\[]担当[］\]]", " ", text)
    text = re.sub(PERSON_ANNOTATION_PATTERN, "", text)
    text = re.sub(
        r"(?<=\S)\s*[（(【\[](?=[^()（）\[\]【】]{0,40}(?:階|号室|県庁|新館|旧館|庁舎|会議室|〒|佐賀市|城内|丁目|番地?|番|号))[^()（）\[\]【】]{0,40}[)）】\]]$",
        "",
        text,
    )
    text = re.sub(r"\s+担当者[\s:：]+.+$", "", text)
    text = re.split(r"(?:電話|TEL|Tel|Fax|FAX|メール|E-mail|Email|Mail|〒|https?://|@)", text, maxsplit=1)[0]
    text = re.sub(r"(課|部|局|室|班|係|担当|チーム|センター)\1+", r"\1", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip(" 　:：,、・()（）[]【】")


def is_valid_department_name(text: str) -> bool:
    candidate = clean_department_name(text)
    if not candidate or len(candidate) > 80:
        return False
    if re.fullmatch(r"[\d()（）.\- ]+", candidate):
        return False
    if candidate in {"所在地", "連絡先", "場所", "部署", "担当", "担当者", "担当部署", "問い合わせ先", "問合せ先"}:
        return False
    if candidate in BAD_DEPARTMENT_EXACT:
        return False
    if candidate.startswith(BAD_DEPARTMENT_PREFIXES):
        return False
    if re.match(r"^[アイウエオ]\s+", candidate):
        return False
    if candidate.startswith(("場所 ", "会場 ")):
        return False
    if re.search(r"(?:丁目|番地?|番|号)(?:\D|$)", candidate) and not contains_known_department_label(candidate):
        return False
    if re.search(r"(?:号室|オフィス|マンション|ビル)$", candidate):
        return False
    if re.search(r"[0-9０-９]+\s*(?:部|枚|式|件|人)$", candidate):
        return False
    if re.search(r"[。!！?？「」『』【】]", candidate):
        return False
    if any(token in candidate for token in ("メール", "E-mail", "Email", "電話", "直通", "内線", "http", "@", "仕様書", "様式", "Web開催", "オンライン開催", "/", "|", "※")):
        return False
    if any(token in candidate for token in BAD_DEPARTMENT_SUBSTRINGS):
        return False
    has_known_department = contains_known_department_label(candidate)
    if re.search(r"(?:場合|こと|もの|ため|について|により|しなければ|できる|される)$", candidate) and not has_known_department:
        return False
    if re.search(rf"{DEPARTMENT_SUFFIX_PATTERN}$", candidate):
        return True
    if not re.search(r"(?:課|室|センター|所|事務局|部|局|班|係|チーム)\s+[^\s]{1,8}$", candidate):
        return False
    if not (has_known_department or re.search(r"(?:課|室|センター|所|事務局|部|局|班|係|チーム)", candidate)):
            return False
    return True


def extract_person_from_labeled_line(line: str) -> str:
    normalized = normalize_text(line)
    match = re.match(rf"^{PERSON_LABEL_PATTERN}[\s:：]*(.+)$", normalized)
    if not match:
        return extract_person_from_annotation(normalized)
    person = clean_person_name(match.group(1))
    return person if is_valid_person_name(person) else ""


def extract_people_from_labeled_line(line: str) -> list[str]:
    normalized = normalize_text(line)
    match = re.match(rf"^{PERSON_LABEL_PATTERN}[\s:：]*(.+)$", normalized)
    if not match:
        return extract_people_from_annotation(normalized)
    return extract_person_names(match.group(1))


def extract_people_from_role_line(line: str) -> list[str]:
    normalized = normalize_text(line)
    match = re.match(rf"^(?:[^\s]{{0,20}}?{ROLE_TITLE_SUFFIX_PATTERN})\s+(.+)$", normalized)
    if not match:
        return []
    return extract_person_names(match.group(1))


def extract_department_and_person_from_compound_line(line: str) -> tuple[str, str]:
    department, people = extract_department_and_people_from_compound_line(line)
    return department, people[0] if people else ""


def extract_department_and_people_from_compound_line(line: str) -> tuple[str, list[str]]:
    normalized = strip_wrapping_brackets(normalize_text(line))

    attached_role_title_match = re.match(
        rf"^(?P<department>.+?(?:課|部|局|室|センター|所|班|係|チーム|事務局|担当|グループ))(?P<role>{ATTACHED_ROLE_TITLE_PATTERN})\s+(?P<person>.+)$",
        normalized,
    )
    if attached_role_title_match:
        department = clean_department_name(attached_role_title_match.group("department"))
        people = extract_person_names(attached_role_title_match.group("person"))
        if is_valid_department_name(department) and people:
            return department, people

    attached_role_match = re.match(
        rf"^(?P<department>.+?(?:課|部|局|室|センター|所|班|係|チーム|事務局|担当|グループ))(?P<role>{ATTACHED_ROLE_SUFFIX_PATTERN})\s+(?P<person>.+)$",
        normalized,
    )
    if attached_role_match:
        department = clean_department_name(attached_role_match.group("department"))
        people = extract_person_names(attached_role_match.group("person"))
        if is_valid_department_name(department) and people:
            return department, people

    annotated_people = extract_people_from_annotation(normalized)
    if annotated_people:
        department = clean_department_name(re.sub(PERSON_ANNOTATION_PATTERN, "", normalized))
        if is_valid_department_name(department):
            return department, annotated_people

    parts = normalized.split()
    if len(parts) < 2:
        return "", []

    candidates: list[tuple[str, list[str]]] = []
    for person_token_count in (2, 1):
        if len(parts) <= person_token_count:
            continue
        people = extract_person_names(" ".join(parts[-person_token_count:]))
        department = clean_department_name(" ".join(parts[:-person_token_count]))
        if people and is_valid_department_name(department):
            candidates.append((department, people))
    if not candidates:
        return "", []
    department, people = max(candidates, key=lambda item: (len(item[1]), len(item[0])))
    return department, people


def extract_department_candidate(line: str) -> str:
    inline_department, inline_people = extract_department_and_people_from_compound_line(line)
    if inline_department and inline_people:
        return ""
    for inner in re.findall(r"[（(【\[]([^()（）\[\]【】]{1,80})[)）】\]]", normalize_text(line)):
        department = clean_department_name(inner)
        if is_valid_department_name(department):
            return department
    department = clean_department_name(line)
    if not is_valid_department_name(department):
        return ""
    return department


def build_person_mentions(
    department: str,
    people: list[str],
    role: str,
    snippet: str,
    source_confidence: float,
    fallback_text: str = "",
) -> list[PersonMention]:
    if not department and not people:
        return []

    email, phone = extract_contact_info(snippet or fallback_text)
    if not people:
        return [
            PersonMention(
                department_name=department,
                person_name="",
                person_key="",
                person_role=role,
                contact_email=email,
                contact_phone=phone,
                extracted_section=snippet,
                name_quality="unknown",
                source_confidence=source_confidence,
            )
        ]

    mentions: list[PersonMention] = []
    for person in people:
        mentions.append(
            PersonMention(
                department_name=department,
                person_name=person,
                person_key=build_person_key(person),
                person_role=role,
                contact_email=email,
                contact_phone=phone,
                extracted_section=snippet,
                name_quality=classify_person_name_quality(person),
                source_confidence=source_confidence,
            )
        )
    return mentions


def dedupe_person_mentions(mentions: list[PersonMention]) -> list[PersonMention]:
    unique_mentions: list[PersonMention] = []
    seen: set[tuple[str, str, str, str]] = set()
    for mention in mentions:
        key = (
            mention.department_name,
            mention.person_name,
            mention.person_role,
            mention.extracted_section,
        )
        if key in seen:
            continue
        seen.add(key)
        unique_mentions.append(mention)
    return unique_mentions


def department_specificity_score(text: str) -> int:
    candidate = clean_department_name(text)
    if not candidate:
        return -10_000
    score = len(candidate)
    score += 12 * len(re.findall(DEPARTMENT_PATTERN, candidate))
    if re.search(r"(?:佐賀県|県|市|庁)", candidate):
        score += 10
    if re.search(r"(?:部長|課長|室長|局長|班長|係長|主査|主幹|主任)$", candidate):
        score -= 18
    if candidate in {"担当", "担当者"}:
        score -= 30
    return score


def choose_better_department(current: str, candidate: str) -> str:
    current = clean_department_name(current)
    candidate = clean_department_name(candidate)
    if not current:
        return candidate
    if not candidate:
        return current
    if current == candidate:
        return current
    if candidate in current:
        return current
    if current in candidate:
        return candidate
    return candidate if department_specificity_score(candidate) > department_specificity_score(current) else current


def is_contact_block_boundary(line: str, contact_seen: bool) -> bool:
    stripped = normalize_text(line).strip()
    boundary_token = stripped.strip("()（）")
    if not stripped:
        return False
    if re.match(r"^(?:添付(?:ファイル|資料)?|別表|関連ファイル|参考資料)$", boundary_token):
        return True
    if stripped in {"No", "名称"}:
        return True
    if contact_seen and re.match(r"^[0-9０-９]+\s*[.)．]?\s*[^0-9０-９].{0,40}$", stripped):
        return True
    if contact_seen and re.match(r"^[【\[].+[】\]]$", stripped):
        return True
    return False


def collect_contact_block(lines: list[str], start_index: int, max_lines: int = 8) -> list[str]:
    block: list[str] = []
    contact_seen = False
    contact_signal_pattern = r"(内線|直通|電話|TEL|Tel|メール|E-mail|Email|Mail|@)"
    for line in lines[start_index:start_index + max_lines]:
        if block and is_contact_block_boundary(line, contact_seen):
            break
        block.append(line)
        if re.search(contact_signal_pattern, line):
            contact_seen = True
    return block


def find_person_mentions(text: str) -> list[PersonMention]:
    text = normalize_text(text)
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    contact_signal_pattern = r"(内線|直通|電話|TEL|Tel|メール|E-mail|Email|Mail|@)"
    candidate_mentions: list[list[PersonMention]] = []

    def add_candidate(mentions: list[PersonMention]) -> None:
        if mentions:
            candidate_mentions.append(dedupe_person_mentions(mentions))

    # 1. 同一行に部署と担当者がある場合
    for i, line in enumerate(lines):
        match = re.match(
            rf"^(?P<department>.+?{DEPARTMENT_PATTERN}.*?)\s+担当者?[\s:：]*(?P<person>.+)$",
            line,
        )
        if not match:
            continue
        department = clean_department_name(match.group("department"))
        people = extract_person_names(match.group("person"))
        if is_valid_department_name(department) and people:
            candidate_block = collect_contact_block(lines, i, 5)
            snippet = "\n".join(candidate_block) if any(re.search(contact_signal_pattern, block_line) for block_line in candidate_block[1:]) else line
            add_candidate(build_person_mentions(department, people, "contact", snippet, 0.95, text))

    # 2. 記者発表ヘッダ型を優先して探す
    for i, line in enumerate(lines):
        department, people = extract_department_and_people_from_compound_line(line)
        if department and people:
            candidate_block = collect_contact_block(lines, i, 5)
            snippet = "\n".join(candidate_block) if any(re.search(contact_signal_pattern, block_line) for block_line in candidate_block[1:]) else line
            add_candidate(build_person_mentions(department, people, "contact", snippet, 0.75, text))

        department = extract_department_candidate(line)
        if not department:
            continue

        candidate_block = collect_contact_block(lines, i, 5)
        department_index = i
        people: list[str] = []
        people_index: int | None = None
        for offset, next_line in enumerate(candidate_block[1:], start=1):
            line_index = i + offset
            people = extract_people_from_labeled_line(next_line)
            if not people:
                people = extract_people_from_role_line(next_line)
            if people:
                people_index = line_index
                break

            inline_department, inline_people = extract_department_and_people_from_compound_line(next_line)
            if inline_people:
                people = inline_people
                if inline_department:
                    chosen_department = choose_better_department(department, inline_department)
                    if chosen_department == inline_department:
                        department_index = line_index
                    department = chosen_department
                people_index = line_index
                break

        if people or any(re.search(contact_signal_pattern, block_line) for block_line in candidate_block):
            snippet_start = department_index if people_index is None else min(department_index, people_index)
            snippet = "\n".join(collect_contact_block(lines, snippet_start, 5))
            confidence = 0.9 if people else 0.55
            add_candidate(build_person_mentions(department, people, "contact", snippet, confidence, text))

    # 3. 問い合わせ先ブロック
    for i, line in enumerate(lines):
        if not re.search(CONTACT_TRIGGER_PATTERN, line):
            continue

        candidate_block = collect_contact_block(lines, i, 8)
        department = ""
        department_index: int | None = None
        people: list[str] = []
        people_index: int | None = None

        for offset, block_line in enumerate(candidate_block):
            line_index = i + offset
            if not people:
                people = extract_people_from_labeled_line(block_line)
                if not people:
                    people = extract_people_from_role_line(block_line)
                if people:
                    people_index = line_index

            if not department:
                department = extract_department_candidate(block_line)
                if department:
                    department_index = line_index

            if not (department and people):
                inline_department, inline_people = extract_department_and_people_from_compound_line(block_line)
                if inline_people and not people:
                    people = inline_people
                    people_index = line_index
                    if inline_department:
                        chosen_department = choose_better_department(department, inline_department)
                        if chosen_department == inline_department:
                            department_index = line_index
                        department = chosen_department
                elif inline_department and not department:
                    department = inline_department
                    department_index = line_index

        if department or people:
            snippet_start_candidates = [index for index in (department_index, people_index) if index is not None]
            snippet_start = min(snippet_start_candidates) if snippet_start_candidates else i
            snippet = "\n".join(collect_contact_block(lines, snippet_start, 6))
            confidence = 0.9 if people else 0.5
            add_candidate(build_person_mentions(department, people, "contact", snippet, confidence, text))

    # 4. 担当者だけ明示される場合は直前の部署行を拾う
    for i, line in enumerate(lines):
        people = extract_people_from_labeled_line(line)
        if not people:
            continue

        department = ""
        for previous_line in reversed(lines[max(0, i - 3):i]):
            department = extract_department_candidate(previous_line)
            if department:
                break
            department, inline_people = extract_department_and_people_from_compound_line(previous_line)
            if department:
                break

        snippet = "\n".join(lines[max(0, i - 2):i + 3])
        add_candidate(build_person_mentions(department, people, "contact", snippet, 0.9, text))

    if not candidate_mentions:
        return []

    best_mentions = max(
        candidate_mentions,
        key=lambda mentions: (
            1 if any(mention.person_name for mention in mentions) else 0,
            sum(1 for mention in mentions if mention.person_name),
            1 if any(mention.department_name for mention in mentions) else 0,
            sum(1 for mention in mentions if mention.department_name),
            max(mention.source_confidence for mention in mentions),
            1 if any(mention.contact_email or mention.contact_phone for mention in mentions) else 0,
            max(len(mention.department_name) for mention in mentions),
        ),
    )
    return best_mentions


def find_department_and_person(text: str) -> tuple[str, str, str, str]:
    mentions = find_person_mentions(text)
    if not mentions:
        return "", "", "", ""
    primary = max(
        mentions,
        key=lambda mention: (
            1 if mention.person_name else 0,
            mention.source_confidence,
            1 if (mention.contact_email or mention.contact_phone) else 0,
            len(mention.department_name),
        ),
    )
    return primary.department_name, primary.person_name, primary.person_role, primary.extracted_section


def summarize_text(text: str, max_length: int = 220, title: str = "") -> str:
    text = normalize_text(text)
    if not text:
        return ""

    normalized_title = normalize_text(title)
    for line in text.split("\n"):
        stripped = line.strip(" 　●■・")
        if not stripped:
            continue
        if normalized_title and stripped == normalized_title:
            continue
        if re.search(r"^(最終更新日|更新日|公開日|掲載日|公表日|発表日|担当者|内線|直通|電話|TEL|メール|E-mail|Email)", stripped):
            continue
        if re.fullmatch(DATE_PATTERN, stripped):
            continue
        if len(stripped) < 12:
            continue
        return stripped[:max_length] + "..." if len(stripped) > max_length else stripped

    fallback = text.split("\n")[0].strip()
    return fallback[:max_length] + "..." if len(fallback) > max_length else fallback


def link_keywords_for_source(
    source_type: str,
    link_keywords: list[str] | None = None,
    link_match_mode: str | None = None,
) -> list[str]:
    if link_keywords:
        return link_keywords
    if source_type == "proposal":
        if link_match_mode == "broad":
            return PROPOSAL_BROAD_KEYWORDS
        return PROPOSAL_KEYWORDS
    return PRESS_RELEASE_KEYWORDS


def collect_links_from_list_page(
    list_url: str,
    source_type: str,
    max_pages: int = 1,
    link_keywords: list[str] | None = None,
    link_match_mode: str | None = None,
    source_department_name: str = "",
) -> list[CrawledLink]:
    html = get_html(list_url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    keywords = link_keywords_for_source(source_type, link_keywords, link_match_mode)
    require_keyword_match = bool(link_keywords) or source_type == "proposal"
    links: dict[str, CrawledLink] = {}

    def extract_links_from_html(current_html: str) -> bool:
        current_soup = BeautifulSoup(current_html, "html.parser")
        found_any = False
        for a_tag in current_soup.find_all("a", href=True):
            href = a_tag["href"]
            if "kiji" not in href:
                continue

            text = normalize_text(a_tag.get_text())
            if not text:
                continue
            if require_keyword_match and not any(keyword in text for keyword in keywords):
                continue

            full_url = urllib.parse.urljoin(BASE_URL, href)
            if full_url not in links:
                links[full_url] = CrawledLink(
                    title=text.strip() or full_url,
                    url=full_url,
                    source_type=source_type,
                    source_list_url=list_url,
                    source_department_name=source_department_name,
                )
                found_any = True
        return found_any

    extract_links_from_html(html)

    if max_pages <= 1:
        return list(links.values())

    next_url_template = None
    for a in soup.find_all("a", rel=True):
        rel_vals = a.get("rel", [])
        if "next1" in rel_vals or "next" in rel_vals:
            href = a.get("href")
            if href and "hpkijilistpagerhandler.ashx" in href:
                next_url_template = urllib.parse.urljoin(BASE_URL, href)
                break

    if next_url_template:
        base_pager_url = re.sub(r"pg=\d+", "pg={pg}", next_url_template)
        for page_num in range(2, max_pages + 1):
            pager_url = base_pager_url.format(pg=page_num)
            print(f"  Fetching older page {page_num} for {list_url}")
            pager_html = get_html(pager_url)
            if not pager_html:
                break
            found_new = extract_links_from_html(pager_html)
            if not found_new:
                break
            time.sleep(0.5)

    return list(links.values())


def extract_project_record(link: CrawledLink) -> ProjectRecord | None:
    page_html = get_html(link.url)
    if not page_html:
        return None

    soup = BeautifulSoup(page_html, "html.parser")
    article_title = extract_article_title(soup, link.title)
    html_text = extract_main_text(soup)

    pdf_urls: list[str] = []
    zip_urls: list[str] = []
    for a_tag in soup.find_all("a", href=True):
        href_lower = a_tag["href"].lower()
        if href_lower.endswith(".pdf"):
            pdf_urls.append(urllib.parse.urljoin(link.url, a_tag["href"]))
        elif href_lower.endswith(".zip"):
            zip_urls.append(urllib.parse.urljoin(link.url, a_tag["href"]))

    pdf_urls = list(dict.fromkeys(pdf_urls))
    zip_urls = list(dict.fromkeys(zip_urls))

    combined_text = html_text + "\n---\n"
    for pdf_url in pdf_urls:
        combined_text += extract_pdf_text(pdf_url) + "\n---\n"
        time.sleep(0.3)
    for zip_url in zip_urls:
        combined_text += extract_zip_pdfs_text(zip_url) + "\n---\n"
        time.sleep(0.3)

    combined_text = normalize_text(combined_text)

    published_at = detect_published_at(html_text, extract_published_at_from_soup(soup))
    published_year = int(published_at[:4]) if re.match(r"^\d{4}-\d{2}-\d{2}$", published_at) else None

    application_deadline = find_deadline(
        combined_text,
        ["参加申込締切", "参加申込書", "参加資格確認申請書", "入札参加"],
    )
    submission_deadline = find_deadline(
        combined_text,
        ["企画提案書等提出締切", "企画提案書提出締切", "提案書提出", "企画書提出", "入札書提出", "開札"],
    )
    purpose = find_purpose(combined_text)
    budget = find_budget(combined_text)
    person_mentions = dedupe_person_mentions(find_person_mentions(combined_text))
    if not person_mentions:
        fallback_department, fallback_person, fallback_role, fallback_section = find_department_and_person(combined_text)
        fallback_email, fallback_phone = extract_contact_info(fallback_section or combined_text)
        person_mentions = build_person_mentions(
            fallback_department,
            [fallback_person] if fallback_person else [],
            fallback_role,
            fallback_section,
            0.0,
            combined_text,
        )
        if not person_mentions and (fallback_department or fallback_email or fallback_phone):
            person_mentions = [
                PersonMention(
                    department_name=fallback_department,
                    person_name="",
                    person_key="",
                    person_role=fallback_role,
                    contact_email=fallback_email,
                    contact_phone=fallback_phone,
                    extracted_section=fallback_section,
                    name_quality="unknown",
                    source_confidence=0.0,
                )
            ]

    summary = summarize_text(purpose or html_text, title=article_title)
    fetched_at = dt.datetime.now().isoformat(timespec="seconds")

    return ProjectRecord(
        title=article_title or link.title,
        url=link.url,
        source_type=link.source_type,
        summary=summary,
        purpose=purpose,
        budget=budget,
        application_deadline=format_date_iso(application_deadline, default_year=published_year),
        submission_deadline=format_date_iso(submission_deadline, default_year=published_year),
        published_at=published_at,
        raw_text=combined_text,
        html_text=html_text,
        pdf_urls=pdf_urls,
        zip_urls=zip_urls,
        person_mentions=person_mentions,
        fetched_at=fetched_at,
        source_list_url=link.source_list_url,
        source_department_name=link.source_department_name,
    )
