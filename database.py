from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Any

from config import DB_PATH
from extractor import (
    PersonMention,
    ProjectRecord,
    build_person_key,
    clean_department_name,
    is_valid_person_name,
    normalize_text,
    normalize_person_name,
)


def get_connection(db_path: Path | str = DB_PATH) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(db_path: Path | str = DB_PATH) -> None:
    conn = get_connection(db_path)
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS departments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                normalized_name TEXT NOT NULL UNIQUE,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS people (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                person_key TEXT NOT NULL UNIQUE,
                display_name TEXT NOT NULL,
                normalized_name TEXT NOT NULL,
                bio TEXT DEFAULT '',
                note TEXT DEFAULT '',
                is_verified INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS projects (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                url TEXT NOT NULL UNIQUE,
                source_type TEXT NOT NULL,
                summary TEXT DEFAULT '',
                purpose TEXT DEFAULT '',
                budget TEXT DEFAULT '',
                application_deadline TEXT DEFAULT '',
                submission_deadline TEXT DEFAULT '',
                published_at TEXT DEFAULT '',
                raw_text TEXT DEFAULT '',
                html_text TEXT DEFAULT '',
                pdf_urls_json TEXT DEFAULT '[]',
                zip_urls_json TEXT DEFAULT '[]',
                fetched_at TEXT DEFAULT '',
                review_status TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS appearances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                department_id INTEGER,
                person_id INTEGER,
                raw_department_name TEXT DEFAULT '',
                raw_person_name TEXT DEFAULT '',
                role TEXT DEFAULT '',
                contact_email TEXT DEFAULT '',
                contact_phone TEXT DEFAULT '',
                extracted_section TEXT DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE,
                FOREIGN KEY(department_id) REFERENCES departments(id) ON DELETE SET NULL,
                FOREIGN KEY(person_id) REFERENCES people(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS person_mentions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                mention_index INTEGER NOT NULL DEFAULT 0,
                department_id INTEGER,
                raw_department_name TEXT DEFAULT '',
                raw_person_name TEXT DEFAULT '',
                normalized_person_name TEXT DEFAULT '',
                name_quality TEXT DEFAULT 'unknown',
                role TEXT DEFAULT '',
                contact_email TEXT DEFAULT '',
                contact_phone TEXT DEFAULT '',
                extracted_section TEXT DEFAULT '',
                source_confidence REAL DEFAULT 0,
                review_status TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE,
                FOREIGN KEY(department_id) REFERENCES departments(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS person_identity_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                person_mention_id INTEGER NOT NULL UNIQUE,
                person_id INTEGER,
                link_status TEXT DEFAULT 'review_pending',
                confidence REAL DEFAULT 0,
                matched_by TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(person_mention_id) REFERENCES person_mentions(id) ON DELETE CASCADE,
                FOREIGN KEY(person_id) REFERENCES people(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS transfer_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type TEXT NOT NULL,
                source_key TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                url TEXT DEFAULT '',
                publisher TEXT DEFAULT '',
                published_at TEXT DEFAULT '',
                effective_date TEXT DEFAULT '',
                raw_text TEXT DEFAULT '',
                source_hash TEXT DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS transfer_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transfer_source_id INTEGER NOT NULL,
                event_index INTEGER NOT NULL DEFAULT 0,
                effective_date TEXT DEFAULT '',
                raw_person_name TEXT NOT NULL,
                normalized_person_name TEXT NOT NULL,
                name_quality TEXT DEFAULT 'unknown',
                from_department_raw TEXT DEFAULT '',
                from_department_id INTEGER,
                to_department_raw TEXT DEFAULT '',
                to_department_id INTEGER,
                from_title_raw TEXT DEFAULT '',
                to_title_raw TEXT DEFAULT '',
                evidence_snippet TEXT DEFAULT '',
                review_status TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(transfer_source_id, event_index),
                FOREIGN KEY(transfer_source_id) REFERENCES transfer_sources(id) ON DELETE CASCADE,
                FOREIGN KEY(from_department_id) REFERENCES departments(id) ON DELETE SET NULL,
                FOREIGN KEY(to_department_id) REFERENCES departments(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS transfer_identity_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transfer_event_id INTEGER NOT NULL UNIQUE,
                person_id INTEGER,
                link_status TEXT DEFAULT 'review_pending',
                confidence REAL DEFAULT 0,
                matched_by TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(transfer_event_id) REFERENCES transfer_events(id) ON DELETE CASCADE,
                FOREIGN KEY(person_id) REFERENCES people(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS project_tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                tag_name TEXT NOT NULL,
                UNIQUE(project_id, tag_name),
                FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_projects_source_type ON projects(source_type);
            CREATE INDEX IF NOT EXISTS idx_projects_published_at ON projects(published_at);
            CREATE INDEX IF NOT EXISTS idx_appearances_project_id ON appearances(project_id);
            CREATE INDEX IF NOT EXISTS idx_appearances_person_id ON appearances(person_id);
            CREATE INDEX IF NOT EXISTS idx_person_mentions_project_id ON person_mentions(project_id);
            CREATE INDEX IF NOT EXISTS idx_person_mentions_name ON person_mentions(normalized_person_name);
            CREATE INDEX IF NOT EXISTS idx_person_identity_links_person_id ON person_identity_links(person_id);
            CREATE INDEX IF NOT EXISTS idx_person_identity_links_status ON person_identity_links(link_status);
            CREATE INDEX IF NOT EXISTS idx_transfer_sources_type ON transfer_sources(source_type);
            CREATE INDEX IF NOT EXISTS idx_transfer_sources_effective_date ON transfer_sources(effective_date);
            CREATE INDEX IF NOT EXISTS idx_transfer_events_name ON transfer_events(normalized_person_name);
            CREATE INDEX IF NOT EXISTS idx_transfer_events_effective_date ON transfer_events(effective_date);
            CREATE INDEX IF NOT EXISTS idx_transfer_events_to_department_id ON transfer_events(to_department_id);
            CREATE INDEX IF NOT EXISTS idx_transfer_identity_links_person_id ON transfer_identity_links(person_id);
            CREATE INDEX IF NOT EXISTS idx_transfer_identity_links_status ON transfer_identity_links(link_status);
            """
        )
        conn.commit()

        # Schema migrations
        try:
            conn.execute("ALTER TABLE projects ADD COLUMN review_status TEXT DEFAULT 'pending'")
        except sqlite3.OperationalError:
            pass  # duplicate column name

        conn.execute(
            """
            INSERT INTO person_mentions (
                project_id, mention_index, department_id, raw_department_name, raw_person_name,
                normalized_person_name, name_quality, role, contact_email, contact_phone,
                extracted_section, source_confidence, review_status
            )
            SELECT
                a.project_id,
                0,
                a.department_id,
                a.raw_department_name,
                a.raw_person_name,
                CASE WHEN IFNULL(TRIM(a.raw_person_name), '') = '' THEN '' ELSE REPLACE(TRIM(a.raw_person_name), ' ', '') END,
                CASE
                    WHEN LENGTH(TRIM(a.raw_person_name)) = 0 THEN 'unknown'
                    WHEN LENGTH(REPLACE(TRIM(a.raw_person_name), ' ', '')) <= 2 THEN 'surname_only'
                    ELSE 'unknown'
                END,
                a.role,
                a.contact_email,
                a.contact_phone,
                a.extracted_section,
                CASE
                    WHEN IFNULL(TRIM(a.contact_email), '') != '' OR IFNULL(TRIM(a.contact_phone), '') != '' THEN 0.9
                    WHEN IFNULL(TRIM(a.raw_person_name), '') != '' THEN 0.7
                    WHEN IFNULL(TRIM(a.raw_department_name), '') != '' THEN 0.5
                    ELSE 0.0
                END,
                'pending'
            FROM appearances a
            WHERE NOT EXISTS (
                SELECT 1 FROM person_mentions pm WHERE pm.project_id = a.project_id
            )
            """
        )

        conn.execute(
            """
            INSERT INTO person_identity_links (
                person_mention_id, person_id, link_status, confidence, matched_by, notes
            )
            SELECT
                pm.id,
                a.person_id,
                'auto_matched',
                0.8,
                'legacy_appearance',
                'Migrated from legacy appearances.person_id'
            FROM person_mentions pm
            JOIN appearances a
              ON a.project_id = pm.project_id
             AND IFNULL(TRIM(a.raw_person_name), '') = IFNULL(TRIM(pm.raw_person_name), '')
            WHERE a.person_id IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM person_identity_links pil
                  WHERE pil.person_mention_id = pm.id
              )
            """
        )
        conn.commit()

    finally:
        conn.close()


def get_or_create_department(conn: sqlite3.Connection, name: str) -> int | None:
    name = (name or "").strip()
    if not name:
        return None
    normalized_name = name.replace(" ", "")
    conn.execute(
        """
        INSERT INTO departments (name, normalized_name)
        VALUES (?, ?)
        ON CONFLICT(normalized_name) DO UPDATE SET name = excluded.name
        """,
        (name, normalized_name),
    )
    row = conn.execute("SELECT id FROM departments WHERE normalized_name = ?", (normalized_name,)).fetchone()
    return int(row["id"]) if row else None


def get_or_create_person(conn: sqlite3.Connection, person_key: str, display_name: str) -> int | None:
    if not person_key or not display_name or not is_valid_person_name(display_name):
        return None
    normalized_name = normalize_person_name(display_name)
    conn.execute(
        """
        INSERT INTO people (person_key, display_name, normalized_name)
        VALUES (?, ?, ?)
        ON CONFLICT(person_key) DO UPDATE SET
            display_name = excluded.display_name,
            normalized_name = excluded.normalized_name,
            updated_at = CURRENT_TIMESTAMP
        """,
        (person_key, display_name.strip(), normalized_name),
    )
    row = conn.execute("SELECT id FROM people WHERE person_key = ?", (person_key,)).fetchone()
    return int(row["id"]) if row else None


def generate_distinct_person_key(conn: sqlite3.Connection, display_name: str, preferred_key: str = "") -> str:
    base_key = preferred_key or build_person_key(display_name)
    candidate = base_key
    suffix = 2
    while conn.execute("SELECT 1 FROM people WHERE person_key = ?", (candidate,)).fetchone():
        candidate = f"{base_key}-{suffix}"
        suffix += 1
    return candidate


def create_distinct_person(conn: sqlite3.Connection, display_name: str, preferred_key: str = "") -> int | None:
    if not display_name or not is_valid_person_name(display_name):
        return None
    normalized_name = normalize_person_name(display_name)
    person_key = generate_distinct_person_key(conn, display_name, preferred_key)
    conn.execute(
        """
        INSERT INTO people (person_key, display_name, normalized_name)
        VALUES (?, ?, ?)
        """,
        (person_key, display_name.strip(), normalized_name),
    )
    row = conn.execute("SELECT id FROM people WHERE person_key = ?", (person_key,)).fetchone()
    return int(row["id"]) if row else None


def should_create_person_from_mention(mention: PersonMention) -> bool:
    if not mention.person_key or not is_valid_person_name(mention.person_name):
        return False
    if mention.name_quality == "full_name":
        return True
    if mention.contact_email or mention.contact_phone:
        return True
    return mention.source_confidence >= 0.9


def get_person_id_for_mention(conn: sqlite3.Connection, mention: PersonMention) -> int | None:
    if not should_create_person_from_mention(mention):
        return None
    return get_or_create_person(conn, mention.person_key, mention.person_name)


def person_mention_from_row(row: sqlite3.Row | dict[str, Any]) -> PersonMention:
    data = dict(row)
    person_name = data.get("raw_person_name", "") or ""
    return PersonMention(
        department_name=data.get("raw_department_name", "") or "",
        person_name=person_name,
        person_key=build_person_key(person_name),
        person_role=data.get("role", "") or "",
        contact_email=data.get("contact_email", "") or "",
        contact_phone=data.get("contact_phone", "") or "",
        extracted_section=data.get("extracted_section", "") or "",
        name_quality=data.get("name_quality", "unknown") or "unknown",
        source_confidence=float(data.get("source_confidence", 0) or 0),
    )


def mention_link_rank(link_status: str, has_person_name: bool, confidence: float) -> tuple[int, float, int]:
    status_priority = {
        "reviewed_match": 4,
        "auto_matched": 3,
        "review_pending": 2,
        "reviewed_distinct": 1,
    }
    return (status_priority.get(link_status or "", 0), confidence, 1 if has_person_name else 0)


def normalize_contact_email(value: str) -> str:
    return (value or "").strip().lower()


def normalize_contact_phone(value: str) -> str:
    digits = re.sub(r"\D", "", value or "")
    if not digits:
        return ""
    if len(digits) in {9, 10} and not digits.startswith("0"):
        digits = "0" + digits
    return digits


def normalize_department_for_match(name: str) -> str:
    return clean_department_name(name).replace(" ", "")


def fetch_person_identity_context(conn: sqlite3.Connection, person_id: int) -> dict[str, Any]:
    mention_rows = conn.execute(
        """
        SELECT pm.raw_department_name, pm.contact_email, pm.contact_phone
        FROM person_identity_links pil
        JOIN person_mentions pm ON pm.id = pil.person_mention_id
        WHERE pil.person_id = ?
          AND pil.link_status IN ('reviewed_match', 'auto_matched')
        """,
        (person_id,),
    ).fetchall()
    appearance_rows = conn.execute(
        """
        SELECT raw_department_name, contact_email, contact_phone
        FROM appearances
        WHERE person_id = ?
        """,
        (person_id,),
    ).fetchall()

    emails: set[str] = set()
    phones: set[str] = set()
    departments: set[str] = set()

    for row in [*mention_rows, *appearance_rows]:
        if row["contact_email"]:
            emails.add(normalize_contact_email(row["contact_email"]))
        if row["contact_phone"]:
            phones.add(normalize_contact_phone(row["contact_phone"]))
        department = normalize_department_for_match(row["raw_department_name"] or "")
        if department:
            departments.add(department)

    project_count = conn.execute(
        """
        SELECT COUNT(DISTINCT project_id)
        FROM appearances
        WHERE person_id = ?
        """,
        (person_id,),
    ).fetchone()[0]

    return {
        "emails": emails,
        "phones": phones,
        "departments": departments,
        "project_count": int(project_count or 0),
    }


def score_person_identity_candidate(
    mention_row: sqlite3.Row | dict[str, Any],
    person_row: sqlite3.Row | dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    mention = dict(mention_row)
    person = dict(person_row)

    mention_email = normalize_contact_email(mention.get("contact_email") or "")
    mention_phone = normalize_contact_phone(mention.get("contact_phone") or "")
    mention_department = normalize_department_for_match(mention.get("raw_department_name", "") or "")

    contact_match = bool(
        (mention_email and mention_email in context["emails"])
        or (mention_phone and mention_phone in context["phones"])
    )
    department_exact = bool(mention_department and mention_department in context["departments"])
    department_overlap = bool(
        mention_department
        and not department_exact
        and any(
            mention_department in department_name or department_name in mention_department
            for department_name in context["departments"]
        )
    )

    score = 0.2
    if person.get("normalized_name") == mention.get("normalized_person_name"):
        score += 0.35
    if (person.get("display_name") or "").strip() == (mention.get("raw_person_name") or "").strip():
        score += 0.05
    if contact_match:
        score += 0.25
    if department_exact:
        score += 0.15
    elif department_overlap:
        score += 0.08
    if context["project_count"] >= 2:
        score += min(context["project_count"] * 0.02, 0.08)

    return {
        "score": round(min(score, 0.98), 2),
        "contact_match": contact_match,
        "department_match": department_exact or department_overlap,
        "project_count": context["project_count"],
    }


RESULT_LIKE_TITLE_TOKENS = (
    "審査結果",
    "結果",
    "質問",
    "回答",
    "最優秀提案事業者",
    "決定",
)


def is_status_like_title(title: str) -> bool:
    normalized = normalize_text(title)
    return any(token in normalized for token in RESULT_LIKE_TITLE_TOKENS)


def normalize_project_title_for_pairing(title: str) -> str:
    normalized = normalize_text(title)
    normalized = re.sub(r"【[^】]*】", " ", normalized)
    normalized = re.sub(r"\[[^\]]*\]", " ", normalized)
    replacements = (
        "質問書に対する回答を公表します",
        "質問書に対する回答について",
        "質問の回答を掲載します",
        "質問への回答について",
        "質問への回答",
        "質問回答掲載",
        "質問回答追加",
        "審査結果をお知らせします",
        "審査結果について",
        "審査結果",
        "結果をお知らせします",
        "結果について",
        "最優秀提案事業者を決定しました",
        "最優秀提案事業者を決定",
        "参加者を募集します",
        "企画提案を募集します",
        "提案を募集します",
        "募集を行います",
        "募集します",
        "を実施します",
        "を行います",
        "を掲載します",
        "を公表します",
        "について",
        "に関する",
        "に係る",
        "に対する",
        "における",
        "の",
    )
    for phrase in replacements:
        normalized = normalized.replace(phrase, " ")
    normalized = re.sub(r"への\s*質問書?\s*に?対する\s*回答.*$", " ", normalized)
    normalized = re.sub(r"質問.*?回答.*$", " ", normalized)
    normalized = re.sub(r"審査結果.*$", " ", normalized)
    normalized = re.sub(r"結果.*$", " ", normalized)
    normalized = re.sub(r"最優秀提案事業者.*$", " ", normalized)
    normalized = re.sub(r"を\s*実施します|を\s*行います|を\s*掲載します|を\s*公表します|を\s*募集します", " ", normalized)
    normalized = re.sub(r"プロポーザル\s*へ", "プロポーザル ", normalized)
    normalized = re.sub(r"[「」\"'【】\[\]（）()・,，:：/／~〜\-]", " ", normalized)
    normalized = re.sub(r"\s+", "", normalized)
    return normalized


def titles_look_related(left: str, right: str) -> bool:
    left_key = normalize_project_title_for_pairing(left)
    right_key = normalize_project_title_for_pairing(right)
    if not left_key or not right_key:
        return False
    if left_key == right_key:
        return True
    shorter, longer = sorted((left_key, right_key), key=len)
    return len(shorter) >= 10 and shorter in longer and (len(shorter) / len(longer)) >= 0.75


def build_public_appearance_payload(appearance_row: sqlite3.Row | dict[str, Any] | None) -> dict[str, Any]:
    appearance = dict(appearance_row) if appearance_row else {}
    raw_person_name = appearance.get("raw_person_name", "") or ""
    observed_person_key = appearance.get("person_key", "") if is_valid_person_name(raw_person_name) else ""
    return {
        **appearance,
        "raw_department_name": appearance.get("raw_department_name", "") or "",
        "raw_person_name": raw_person_name,
        "contact_email": appearance.get("contact_email", "") or "",
        "contact_phone": appearance.get("contact_phone", "") or "",
        "role": appearance.get("role", "") or "",
        "extracted_section": appearance.get("extracted_section", "") or "",
        "person_id": appearance.get("person_id"),
        "person_key": appearance.get("person_key", "") or "",
        "display_department_name": appearance.get("raw_department_name", "") or "",
        "display_person_name": raw_person_name,
        "display_person_key": observed_person_key,
        "department_status": "observed" if (appearance.get("raw_department_name", "") or "").strip() else "missing",
        "person_status": "observed" if raw_person_name.strip() else "missing",
        "department_status_label": "",
        "person_status_label": "",
        "source_project_id": None,
        "source_project_title": "",
    }


def infer_person_from_contact(
    conn: sqlite3.Connection,
    appearance_payload: dict[str, Any],
    exclude_project_id: int,
) -> dict[str, Any] | None:
    email = normalize_contact_email(appearance_payload.get("contact_email", ""))
    phone = normalize_contact_phone(appearance_payload.get("contact_phone", ""))
    if not email and not phone:
        return None

    rows = conn.execute(
        """
        SELECT a.project_id, a.person_id, a.raw_person_name, a.raw_department_name,
               a.contact_email, a.contact_phone, pe.person_key, pe.display_name
        FROM appearances a
        LEFT JOIN people pe ON pe.id = a.person_id
        WHERE a.project_id != ?
          AND IFNULL(TRIM(a.raw_person_name), '') != ''
        """,
        (exclude_project_id,),
    ).fetchall()

    candidates: dict[str, dict[str, Any]] = {}
    for row in rows:
        row_email = normalize_contact_email(row["contact_email"] or "")
        row_phone = normalize_contact_phone(row["contact_phone"] or "")
        if not ((email and email == row_email) or (phone and phone == row_phone)):
            continue

        signature = (
            f"id:{row['person_id']}"
            if row["person_id"]
            else f"name:{normalize_person_name(row['raw_person_name'])}"
        )
        if signature.endswith(":"):
            continue

        candidate = candidates.setdefault(
            signature,
            {
                "person_name": row["display_name"] or row["raw_person_name"],
                "person_key": row["person_key"] or "",
                "project_id": row["project_id"],
                "department_name": row["raw_department_name"] or "",
                "matched_on_email": False,
                "matched_on_phone": False,
                "match_count": 0,
            },
        )
        candidate["match_count"] += 1
        candidate["matched_on_email"] = candidate["matched_on_email"] or bool(email and email == row_email)
        candidate["matched_on_phone"] = candidate["matched_on_phone"] or bool(phone and phone == row_phone)

    if len(candidates) != 1:
        return None

    candidate = next(iter(candidates.values()))
    return candidate if is_valid_person_name(candidate["person_name"]) else None


def infer_related_project_appearance(
    conn: sqlite3.Connection,
    project_row: sqlite3.Row | dict[str, Any],
    exclude_project_id: int,
) -> dict[str, Any] | None:
    title = project_row["title"] if isinstance(project_row, sqlite3.Row) else project_row.get("title", "")
    if not is_status_like_title(title):
        return None

    rows = conn.execute(
        """
        SELECT
            p.id,
            p.title,
            p.source_type,
            p.published_at,
            a.raw_department_name,
            a.raw_person_name,
            a.contact_email,
            a.contact_phone,
            a.role,
            a.extracted_section,
            a.person_id,
            pe.person_key
        FROM projects p
        JOIN appearances a ON a.project_id = p.id
        LEFT JOIN people pe ON pe.id = a.person_id
        WHERE p.id != ?
          AND p.review_status != 'rejected'
        ORDER BY p.id DESC
        """,
        (exclude_project_id,),
    ).fetchall()

    candidates: list[sqlite3.Row] = [row for row in rows if titles_look_related(title, row["title"] or "")]
    if not candidates:
        return None

    best = max(
        candidates,
        key=lambda row: (
            1 if not is_status_like_title(row["title"] or "") else 0,
            1 if (row["raw_person_name"] or "").strip() else 0,
            1 if (row["raw_department_name"] or "").strip() else 0,
            1 if row["person_id"] else 0,
            row["id"],
        ),
    )
    return dict(best)


def resolve_public_appearance(
    conn: sqlite3.Connection,
    project_row: sqlite3.Row | dict[str, Any],
    appearance_row: sqlite3.Row | dict[str, Any] | None,
) -> dict[str, Any] | None:
    project_id = int(project_row["id"] if isinstance(project_row, sqlite3.Row) else project_row["id"])
    resolved = build_public_appearance_payload(appearance_row)

    if resolved["person_status"] == "missing":
        contact_candidate = infer_person_from_contact(conn, resolved, exclude_project_id=project_id)
        if contact_candidate:
            resolved["display_person_name"] = contact_candidate["person_name"]
            resolved["display_person_key"] = contact_candidate["person_key"]
            resolved["person_status"] = "inferred_same_contact"
            resolved["person_status_label"] = "推定: 同じ連絡先"

    if resolved["department_status"] == "missing" or resolved["person_status"] == "missing":
        related = infer_related_project_appearance(conn, project_row, exclude_project_id=project_id)
        if related:
            if resolved["department_status"] == "missing" and (related.get("raw_department_name") or "").strip():
                resolved["display_department_name"] = related["raw_department_name"]
                resolved["department_status"] = "inherited_related_project"
                resolved["department_status_label"] = "継承: 関連ページ"
            if resolved["person_status"] == "missing" and is_valid_person_name(related.get("raw_person_name", "") or ""):
                resolved["display_person_name"] = related["raw_person_name"]
                resolved["display_person_key"] = related.get("person_key", "") or ""
                resolved["person_status"] = "inherited_related_project"
                resolved["person_status_label"] = "継承: 関連ページ"
            if (
                resolved["department_status"] == "inherited_related_project"
                or resolved["person_status"] == "inherited_related_project"
            ):
                resolved["source_project_id"] = related["id"]
                resolved["source_project_title"] = related["title"]

    if not (
        resolved["display_department_name"]
        or resolved["display_person_name"]
        or resolved["contact_email"]
        or resolved["contact_phone"]
    ):
        return None

    return resolved


def upsert_project(conn: sqlite3.Connection, record: ProjectRecord) -> int:
    import json

    conn.execute(
        """
        INSERT INTO projects (
            title, url, source_type, summary, purpose, budget,
            application_deadline, submission_deadline, published_at,
            raw_text, html_text, pdf_urls_json, zip_urls_json, fetched_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(url) DO UPDATE SET
            title = excluded.title,
            source_type = excluded.source_type,
            summary = excluded.summary,
            purpose = excluded.purpose,
            budget = excluded.budget,
            application_deadline = excluded.application_deadline,
            submission_deadline = excluded.submission_deadline,
            published_at = excluded.published_at,
            raw_text = excluded.raw_text,
            html_text = excluded.html_text,
            pdf_urls_json = excluded.pdf_urls_json,
            zip_urls_json = excluded.zip_urls_json,
            fetched_at = excluded.fetched_at,
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            record.title,
            record.url,
            record.source_type,
            record.summary,
            record.purpose,
            record.budget,
            record.application_deadline,
            record.submission_deadline,
            record.published_at,
            record.raw_text,
            record.html_text,
            json.dumps(record.pdf_urls, ensure_ascii=False),
            json.dumps(record.zip_urls, ensure_ascii=False),
            record.fetched_at,
        ),
    )
    row = conn.execute("SELECT id FROM projects WHERE url = ?", (record.url,)).fetchone()
    return int(row["id"])


def replace_project_person_mentions(
    conn: sqlite3.Connection,
    project_id: int,
    record: ProjectRecord,
    allow_empty_replace: bool = False,
) -> None:
    if not record.person_mentions and not allow_empty_replace:
        return

    conn.execute("DELETE FROM person_mentions WHERE project_id = ?", (project_id,))

    for mention_index, mention in enumerate(record.person_mentions):
        department_id = get_or_create_department(conn, mention.department_name)
        conn.execute(
            """
            INSERT INTO person_mentions (
                project_id, mention_index, department_id, raw_department_name, raw_person_name,
                normalized_person_name, name_quality, role, contact_email, contact_phone,
                extracted_section, source_confidence, review_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                project_id,
                mention_index,
                department_id,
                mention.department_name,
                mention.person_name,
                normalize_person_name(mention.person_name) if mention.person_name else "",
                mention.name_quality,
                mention.person_role,
                mention.contact_email,
                mention.contact_phone,
                mention.extracted_section,
                mention.source_confidence,
                "pending",
            ),
        )


def upsert_person_identity_link(
    conn: sqlite3.Connection,
    person_mention_id: int,
    person_id: int | None,
    link_status: str,
    confidence: float,
    matched_by: str,
    notes: str = "",
) -> None:
    conn.execute(
        """
        INSERT INTO person_identity_links (
            person_mention_id, person_id, link_status, confidence, matched_by, notes, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(person_mention_id) DO UPDATE SET
            person_id = excluded.person_id,
            link_status = excluded.link_status,
            confidence = excluded.confidence,
            matched_by = excluded.matched_by,
            notes = excluded.notes,
            updated_at = CURRENT_TIMESTAMP
        """,
        (person_mention_id, person_id, link_status, confidence, matched_by, notes),
    )


def auto_link_person_mentions_for_project(conn: sqlite3.Connection, project_id: int) -> None:
    rows = conn.execute(
        """
        SELECT
            pm.*,
            pil.id AS link_id,
            pil.link_status AS existing_link_status
        FROM person_mentions pm
        LEFT JOIN person_identity_links pil ON pil.person_mention_id = pm.id
        WHERE pm.project_id = ?
        ORDER BY pm.mention_index ASC, pm.id ASC
        """,
        (project_id,),
    ).fetchall()

    for row in rows:
        mention = person_mention_from_row(row)
        existing_status = row["existing_link_status"] or ""
        if existing_status in {"reviewed_match", "reviewed_distinct"}:
            continue

        if not mention.person_name:
            continue

        exact_people = [
            person
            for person in conn.execute(
                """
                SELECT *
                FROM people
                WHERE normalized_name = ?
                   OR display_name = ?
                ORDER BY id ASC
                """,
                (normalize_person_name(mention.person_name), mention.person_name),
            ).fetchall()
            if is_valid_person_name(person["display_name"])
        ]

        person_id: int | None = None
        link_status = "review_pending"
        confidence = min(mention.source_confidence, 0.55)
        matched_by = "same_name"
        notes = ""

        scored_candidates: list[tuple[sqlite3.Row, dict[str, Any]]] = []
        for person in exact_people:
            context = fetch_person_identity_context(conn, int(person["id"]))
            metrics = score_person_identity_candidate(row, person, context)
            scored_candidates.append((person, metrics))

        scored_candidates.sort(
            key=lambda item: (
                item[1]["score"],
                1 if item[1]["contact_match"] else 0,
                item[1]["project_count"],
                -int(item[0]["id"]),
            ),
            reverse=True,
        )

        top_candidate = scored_candidates[0] if scored_candidates else None
        second_score = scored_candidates[1][1]["score"] if len(scored_candidates) > 1 else 0.0
        top_gap = top_candidate[1]["score"] - second_score if top_candidate else 0.0

        if len(scored_candidates) == 1 and top_candidate:
            person_id = int(top_candidate[0]["id"])
            if mention.name_quality == "full_name" and top_candidate[1]["score"] >= 0.9:
                link_status = "auto_matched"
                confidence = max(top_candidate[1]["score"], mention.source_confidence)
                matched_by = "full_name_exact"
            elif top_candidate[1]["contact_match"] and top_candidate[1]["score"] >= 0.8:
                link_status = "auto_matched"
                confidence = max(top_candidate[1]["score"], mention.source_confidence)
                matched_by = "same_name_contact_exact"
            else:
                link_status = "review_pending"
                confidence = max(top_candidate[1]["score"], mention.source_confidence, 0.6)
                matched_by = "same_name_only"
        elif len(scored_candidates) > 1 and top_candidate:
            if top_candidate[1]["contact_match"] and top_gap >= 0.12:
                person_id = int(top_candidate[0]["id"])
                link_status = "review_pending"
                confidence = max(top_candidate[1]["score"], mention.source_confidence, 0.78)
                matched_by = "same_name_contact_exact"
                notes = "Best contact match among people sharing this normalized name"
            else:
                person_id = None
                link_status = "review_pending"
                confidence = max(top_candidate[1]["score"], 0.45)
                matched_by = "multiple_same_name"
                notes = "Multiple people share this normalized name"
        else:
            person_id = get_person_id_for_mention(conn, mention)
            if person_id:
                if mention.name_quality == "full_name":
                    link_status = "auto_matched"
                    confidence = max(0.9, mention.source_confidence)
                    matched_by = "new_full_name"
                elif mention.contact_email or mention.contact_phone:
                    link_status = "review_pending"
                    confidence = max(0.72, mention.source_confidence)
                    matched_by = "new_name_with_contact"
                else:
                    link_status = "review_pending"
                    confidence = max(0.6, mention.source_confidence)
                    matched_by = "new_name"

        if person_id or link_status == "review_pending":
            upsert_person_identity_link(
                conn,
                int(row["id"]),
                person_id,
                link_status,
                confidence,
                matched_by,
                notes,
            )


def refresh_project_appearance_from_mentions(conn: sqlite3.Connection, project_id: int) -> None:
    rows = conn.execute(
        """
        SELECT
            pm.*,
            pil.person_id,
            pil.link_status,
            pil.confidence
        FROM person_mentions pm
        LEFT JOIN person_identity_links pil ON pil.person_mention_id = pm.id
        WHERE pm.project_id = ?
        ORDER BY pm.mention_index ASC, pm.id ASC
        """,
        (project_id,),
    ).fetchall()

    if not rows:
        return

    best_row = max(
        rows,
        key=lambda row: (
            mention_link_rank(
                row["link_status"] or "",
                bool((row["raw_person_name"] or "").strip()),
                float(row["confidence"] or row["source_confidence"] or 0),
            ),
            1 if (row["contact_email"] or row["contact_phone"]) else 0,
            len(row["raw_department_name"] or ""),
        ),
    )

    department_id = get_or_create_department(conn, best_row["raw_department_name"] or "")
    conn.execute("DELETE FROM appearances WHERE project_id = ?", (project_id,))
    conn.execute(
        """
        INSERT INTO appearances (
            project_id, department_id, person_id, raw_department_name, raw_person_name,
            role, contact_email, contact_phone, extracted_section
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            project_id,
            department_id,
            best_row["person_id"],
            best_row["raw_department_name"] or "",
            best_row["raw_person_name"] or "",
            best_row["role"] or "",
            best_row["contact_email"] or "",
            best_row["contact_phone"] or "",
            best_row["extracted_section"] or "",
        ),
    )


def replace_project_appearance(
    conn: sqlite3.Connection,
    project_id: int,
    record: ProjectRecord,
    allow_empty_replace: bool = False,
) -> None:
    if not record.person_mentions:
        if allow_empty_replace:
            conn.execute("DELETE FROM appearances WHERE project_id = ?", (project_id,))
        return
    auto_link_person_mentions_for_project(conn, project_id)
    refresh_project_appearance_from_mentions(conn, project_id)


def save_project_record(record: ProjectRecord, db_path: Path | str = DB_PATH) -> int:
    conn = get_connection(db_path)
    try:
        project_id = upsert_project(conn, record)
        replace_project_person_mentions(conn, project_id, record)
        replace_project_appearance(conn, project_id, record)
        conn.commit()
        return project_id
    finally:
        conn.close()


def fetch_projects(limit: int = 100, source_type: str | None = None) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        if source_type:
            rows = conn.execute(
                """
                SELECT p.*, a.raw_department_name, a.raw_person_name, pe.person_key
                FROM projects p
                LEFT JOIN appearances a ON a.project_id = p.id
                LEFT JOIN people pe ON pe.id = a.person_id
                WHERE p.source_type = ? AND p.review_status != 'rejected'
                ORDER BY COALESCE(NULLIF(p.published_at, ''), substr(p.fetched_at, 1, 10)) DESC, p.id DESC
                LIMIT ?
                """,
                (source_type, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT p.*, a.raw_department_name, a.raw_person_name, pe.person_key
                FROM projects p
                LEFT JOIN appearances a ON a.project_id = p.id
                LEFT JOIN people pe ON pe.id = a.person_id
                WHERE p.review_status != 'rejected'
                ORDER BY COALESCE(NULLIF(p.published_at, ''), substr(p.fetched_at, 1, 10)) DESC, p.id DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        payload = [dict(row) for row in rows]
        for item in payload:
            resolved = resolve_public_appearance(conn, item, item)
            item["display_department_name"] = resolved["display_department_name"] if resolved else ""
            item["display_person_name"] = resolved["display_person_name"] if resolved else ""
            item["display_person_key"] = resolved["display_person_key"] if resolved else ""
            item["person_status"] = resolved["person_status"] if resolved else "missing"
            item["person_status_label"] = resolved["person_status_label"] if resolved else ""
            item["department_status"] = resolved["department_status"] if resolved else "missing"
            item["department_status_label"] = resolved["department_status_label"] if resolved else ""
            item["source_project_id"] = resolved["source_project_id"] if resolved else None
            item["source_project_title"] = resolved["source_project_title"] if resolved else ""
            if not is_valid_person_name(item.get("raw_person_name", "")):
                item["person_key"] = ""
        return payload
    finally:
        conn.close()


def fetch_project_detail(project_id: int) -> dict[str, Any] | None:
    import json

    conn = get_connection()
    try:
        row = conn.execute("SELECT * FROM projects WHERE id = ?", (project_id,)).fetchone()
        if not row:
            return None

        appearance = conn.execute(
            """
            SELECT a.*, pe.person_key
            FROM appearances a
            LEFT JOIN people pe ON pe.id = a.person_id
            WHERE a.project_id = ?
            LIMIT 1
            """,
            (project_id,),
        ).fetchone()

        related = conn.execute(
            """
            SELECT p.id, p.title, p.source_type, p.published_at
            FROM projects p
            JOIN appearances a2 ON a2.project_id = p.id
            WHERE a2.person_id = ? AND p.id != ?
            ORDER BY COALESCE(NULLIF(p.published_at, ''), substr(p.fetched_at, 1, 10)) DESC, p.id DESC
            LIMIT 10
            """,
            (appearance["person_id"], project_id),
        ).fetchall() if appearance and appearance["person_id"] else []

        payload = dict(row)
        payload["pdf_urls"] = json.loads(payload.pop("pdf_urls_json") or "[]")
        payload["zip_urls"] = json.loads(payload.pop("zip_urls_json") or "[]")
        payload["appearance"] = dict(appearance) if appearance else None
        payload["resolved_appearance"] = resolve_public_appearance(conn, row, appearance)
        payload["related_projects"] = [dict(item) for item in related]
        if payload["appearance"] and not is_valid_person_name(payload["appearance"].get("raw_person_name", "")):
            payload["appearance"]["person_key"] = ""
            payload["related_projects"] = []
        return payload
    finally:
        conn.close()


def fetch_people(limit: int = 100, db_path: Path | str = DB_PATH) -> list[sqlite3.Row]:
    conn = get_connection(db_path)
    try:
        rows = conn.execute(
            """
            WITH linked_projects AS (
                SELECT DISTINCT a.person_id, a.project_id, a.raw_department_name
                FROM appearances a
                WHERE a.person_id IS NOT NULL
                UNION
                SELECT DISTINCT pil.person_id, pm.project_id, pm.raw_department_name
                FROM person_identity_links pil
                JOIN person_mentions pm ON pm.id = pil.person_mention_id
                WHERE pil.person_id IS NOT NULL
                  AND pil.link_status IN ('reviewed_match', 'auto_matched')
            )
            SELECT pe.*, COUNT(DISTINCT lp.project_id) AS project_count,
                   GROUP_CONCAT(DISTINCT lp.raw_department_name) AS departments
            FROM people pe
            LEFT JOIN linked_projects lp ON lp.person_id = pe.id
            GROUP BY pe.id
            HAVING project_count > 0
            ORDER BY project_count DESC, pe.display_name ASC
            """,
        ).fetchall()
        filtered = [row for row in rows if is_valid_person_name(row["display_name"])]
        return filtered[:limit]
    finally:
        conn.close()


def fetch_person_detail(person_key: str, db_path: Path | str = DB_PATH) -> dict[str, Any] | None:
    conn = get_connection(db_path)
    try:
        person = conn.execute("SELECT * FROM people WHERE person_key = ?", (person_key,)).fetchone()
        if not person:
            return None

        projects = conn.execute(
            """
            WITH linked_projects AS (
                SELECT DISTINCT a.person_id, a.project_id, a.raw_department_name, a.role
                FROM appearances a
                WHERE a.person_id = ?
                UNION
                SELECT DISTINCT pil.person_id, pm.project_id, pm.raw_department_name, pm.role
                FROM person_identity_links pil
                JOIN person_mentions pm ON pm.id = pil.person_mention_id
                WHERE pil.person_id = ?
                  AND pil.link_status IN ('reviewed_match', 'auto_matched')
            )
            SELECT p.id, p.title, p.summary, p.source_type, p.published_at,
                   MAX(COALESCE(NULLIF(lp.raw_department_name, ''), '')) AS raw_department_name,
                   MAX(COALESCE(NULLIF(lp.role, ''), '')) AS role
            FROM linked_projects lp
            JOIN projects p ON p.id = lp.project_id
            GROUP BY p.id, p.title, p.summary, p.source_type, p.published_at
            ORDER BY COALESCE(NULLIF(p.published_at, ''), substr(p.fetched_at, 1, 10)) DESC, p.id DESC
            """,
            (person["id"], person["id"]),
        ).fetchall()

        departments = conn.execute(
            """
            WITH linked_projects AS (
                SELECT DISTINCT a.person_id, a.project_id, a.raw_department_name
                FROM appearances a
                WHERE a.person_id = ?
                UNION
                SELECT DISTINCT pil.person_id, pm.project_id, pm.raw_department_name
                FROM person_identity_links pil
                JOIN person_mentions pm ON pm.id = pil.person_mention_id
                WHERE pil.person_id = ?
                  AND pil.link_status IN ('reviewed_match', 'auto_matched')
            )
            SELECT DISTINCT raw_department_name
            FROM linked_projects
            WHERE raw_department_name != ''
            ORDER BY raw_department_name ASC
            """,
            (person["id"], person["id"]),
        ).fetchall()

        department_history = conn.execute(
            """
            WITH linked_projects AS (
                SELECT DISTINCT a.person_id, a.project_id, a.raw_department_name
                FROM appearances a
                WHERE a.person_id = ?
                UNION
                SELECT DISTINCT pil.person_id, pm.project_id, pm.raw_department_name
                FROM person_identity_links pil
                JOIN person_mentions pm ON pm.id = pil.person_mention_id
                WHERE pil.person_id = ?
                  AND pil.link_status IN ('reviewed_match', 'auto_matched')
            )
            SELECT
                COALESCE(NULLIF(lp.raw_department_name, ''), '部署不明') AS department_name,
                COUNT(DISTINCT lp.project_id) AS project_count,
                MIN(COALESCE(NULLIF(p.published_at, ''), substr(p.fetched_at, 1, 10))) AS first_seen_at,
                MAX(COALESCE(NULLIF(p.published_at, ''), substr(p.fetched_at, 1, 10))) AS last_seen_at
            FROM linked_projects lp
            JOIN projects p ON p.id = lp.project_id
            GROUP BY COALESCE(NULLIF(lp.raw_department_name, ''), '部署不明')
            ORDER BY last_seen_at DESC, project_count DESC, department_name ASC
            """,
            (person["id"], person["id"]),
        ).fetchall()

        return {
            "person": dict(person),
            "projects": [dict(item) for item in projects],
            "departments": [row["raw_department_name"] for row in departments],
            "department_history": [dict(item) for item in department_history],
        }
    finally:
        conn.close()


def cleanup_orphan_people(db_path: Path | str = DB_PATH) -> int:
    conn = get_connection(db_path)
    try:
        orphan_ids = [
            int(row["id"])
            for row in conn.execute(
                """
                SELECT pe.id
                FROM people pe
                LEFT JOIN appearances a ON a.person_id = pe.id
                LEFT JOIN person_identity_links pil ON pil.person_id = pe.id
                WHERE pe.is_verified = 0
                  AND IFNULL(TRIM(pe.bio), '') = ''
                  AND IFNULL(TRIM(pe.note), '') = ''
                GROUP BY pe.id
                HAVING COUNT(DISTINCT a.project_id) = 0
                   AND COUNT(DISTINCT pil.id) = 0
                """
            ).fetchall()
        ]
        if orphan_ids:
            conn.executemany("DELETE FROM people WHERE id = ?", [(person_id,) for person_id in orphan_ids])
            conn.commit()
        return len(orphan_ids)
    finally:
        conn.close()


def fetch_pending_reviews(limit: int = 100) -> list[sqlite3.Row]:
    conn = get_connection()
    try:
        return conn.execute(
            """
            SELECT p.*, a.id as appearance_id, a.raw_department_name, a.raw_person_name, a.role, pe.person_key
            FROM projects p
            LEFT JOIN appearances a ON a.project_id = p.id
            LEFT JOIN people pe ON pe.id = a.person_id
            WHERE p.review_status = 'pending'
            ORDER BY COALESCE(p.published_at, ''), p.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    finally:
        conn.close()


def fetch_identity_candidates_for_mention(conn: sqlite3.Connection, mention_id: int, limit: int = 10) -> list[dict[str, Any]]:
    mention = conn.execute(
        """
        SELECT *
        FROM person_mentions
        WHERE id = ?
        """,
        (mention_id,),
    ).fetchone()
    if not mention or not (mention["normalized_person_name"] or "").strip():
        return []

    people = conn.execute(
        """
        SELECT
            pe.*,
            COUNT(DISTINCT a.project_id) AS project_count,
            GROUP_CONCAT(DISTINCT a.raw_department_name) AS departments
        FROM people pe
        LEFT JOIN appearances a ON a.person_id = pe.id
        WHERE pe.normalized_name = ?
           OR pe.display_name = ?
        GROUP BY pe.id
        ORDER BY project_count DESC, pe.display_name ASC
        LIMIT ?
        """,
        (mention["normalized_person_name"], mention["raw_person_name"], limit * 3),
    ).fetchall()

    payload: list[dict[str, Any]] = []
    for person in people:
        if not is_valid_person_name(person["display_name"]):
            continue
        context = fetch_person_identity_context(conn, int(person["id"]))
        metrics = score_person_identity_candidate(mention, person, context)
        payload.append(
            {
                **dict(person),
                "candidate_score": metrics["score"],
                "contact_match": metrics["contact_match"],
                "department_match": metrics["department_match"],
            }
        )
    payload.sort(
        key=lambda item: (
            item["candidate_score"],
            1 if item["contact_match"] else 0,
            item["project_count"],
            item["display_name"],
        ),
        reverse=True,
    )
    return payload[:limit]


def fetch_pending_identity_reviews(limit: int = 100) -> list[dict[str, Any]]:
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT
                pm.*,
                p.title,
                p.url,
                p.published_at,
                p.source_type,
                pil.person_id,
                pil.link_status,
                pil.confidence,
                pil.matched_by,
                pe.display_name AS linked_person_name,
                pe.person_key AS linked_person_key
            FROM person_mentions pm
            JOIN projects p ON p.id = pm.project_id
            LEFT JOIN person_identity_links pil ON pil.person_mention_id = pm.id
            LEFT JOIN people pe ON pe.id = pil.person_id
            WHERE IFNULL(TRIM(pm.raw_person_name), '') != ''
              AND (
                    pil.id IS NULL
                    OR pil.link_status = 'review_pending'
                  )
            ORDER BY
                COALESCE(pil.confidence, pm.source_confidence) DESC,
                COALESCE(NULLIF(p.published_at, ''), substr(p.fetched_at, 1, 10)) DESC,
                pm.id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()

        payload: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["candidates"] = fetch_identity_candidates_for_mention(conn, int(row["id"]))
            payload.append(item)
        return payload
    finally:
        conn.close()


def link_person_mention_to_person(
    mention_id: int,
    person_id: int,
    link_status: str = "reviewed_match",
    confidence: float = 1.0,
    matched_by: str = "manual",
    notes: str = "",
    db_path: Path | str = DB_PATH,
) -> None:
    conn = get_connection(db_path)
    try:
        mention = conn.execute("SELECT project_id FROM person_mentions WHERE id = ?", (mention_id,)).fetchone()
        if not mention:
            return
        upsert_person_identity_link(conn, mention_id, person_id, link_status, confidence, matched_by, notes)
        refresh_project_appearance_from_mentions(conn, int(mention["project_id"]))
        conn.commit()
    finally:
        conn.close()


def create_person_for_mention(mention_id: int, db_path: Path | str = DB_PATH) -> int | None:
    conn = get_connection(db_path)
    try:
        row = conn.execute("SELECT * FROM person_mentions WHERE id = ?", (mention_id,)).fetchone()
        if not row:
            return None
        mention = person_mention_from_row(row)
        if not mention.person_name or not is_valid_person_name(mention.person_name):
            return None
        person_id = create_distinct_person(conn, mention.person_name, mention.person_key)
        upsert_person_identity_link(conn, mention_id, person_id, "reviewed_match", 1.0, "manual_create", "")
        refresh_project_appearance_from_mentions(conn, int(row["project_id"]))
        conn.commit()
        return person_id
    finally:
        conn.close()


def mark_person_mention_distinct(mention_id: int, notes: str = "", db_path: Path | str = DB_PATH) -> None:
    conn = get_connection(db_path)
    try:
        row = conn.execute("SELECT project_id FROM person_mentions WHERE id = ?", (mention_id,)).fetchone()
        if not row:
            return
        upsert_person_identity_link(conn, mention_id, None, "reviewed_distinct", 1.0, "manual_distinct", notes)
        refresh_project_appearance_from_mentions(conn, int(row["project_id"]))
        conn.commit()
    finally:
        conn.close()


def update_project_review(project_id: int, review_status: str, title: str, summary: str, purpose: str, budget: str, app_deadline: str, sub_deadline: str, department_name: str, person_name: str, person_role: str) -> None:
    conn = get_connection()
    try:
        conn.execute(
            """
            UPDATE projects SET
                review_status = ?,
                title = ?, summary = ?, purpose = ?, budget = ?,
                application_deadline = ?, submission_deadline = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE id = ?
            """,
            (review_status, title, summary, purpose, budget, app_deadline, sub_deadline, project_id)
        )

        department_id = get_or_create_department(conn, department_name)
        person_key = build_person_key(person_name) if is_valid_person_name(person_name) else ""
        mention = PersonMention(
            department_name=department_name,
            person_name=person_name,
            person_key=person_key,
            person_role=person_role,
            contact_email="",
            contact_phone="",
            extracted_section="",
            name_quality="full_name" if " " in person_name else ("surname_only" if len(person_name.strip()) <= 2 else "unknown"),
            source_confidence=1.0,
        )
        person_id = get_person_id_for_mention(conn, mention)

        app = conn.execute("SELECT id FROM appearances WHERE project_id = ? LIMIT 1", (project_id,)).fetchone()
        if app:
            conn.execute(
                """
                UPDATE appearances SET
                    department_id = ?, person_id = ?,
                    raw_department_name = ?, raw_person_name = ?, role = ?
                WHERE id = ?
                """,
                (department_id, person_id, department_name, person_name, person_role, app["id"])
            )
        else:
            conn.execute(
                """
                INSERT INTO appearances (project_id, department_id, person_id, raw_department_name, raw_person_name, role)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (project_id, department_id, person_id, department_name, person_name, person_role)
            )

        conn.execute("DELETE FROM person_mentions WHERE project_id = ?", (project_id,))
        conn.execute(
            """
            INSERT INTO person_mentions (
                project_id, mention_index, department_id, raw_department_name, raw_person_name,
                normalized_person_name, name_quality, role, contact_email, contact_phone,
                extracted_section, source_confidence, review_status
            )
            VALUES (?, 0, ?, ?, ?, ?, ?, ?, '', '', '', 1.0, ?)
            """,
            (
                project_id,
                department_id,
                department_name,
                person_name,
                normalize_person_name(person_name) if person_name else "",
                mention.name_quality,
                person_role,
                review_status,
            ),
        )
        mention_id = conn.execute(
            "SELECT id FROM person_mentions WHERE project_id = ? ORDER BY mention_index ASC, id ASC LIMIT 1",
            (project_id,),
        ).fetchone()["id"]
        if person_id:
            upsert_person_identity_link(conn, int(mention_id), person_id, "reviewed_match", 1.0, "manual_review")
        else:
            upsert_person_identity_link(conn, int(mention_id), None, "reviewed_distinct", 1.0, "manual_review")
        refresh_project_appearance_from_mentions(conn, project_id)

        conn.commit()
    finally:
        conn.close()

def merge_people(primary_id: int, secondary_id: int) -> None:
    conn = get_connection()
    try:
        conn.execute("UPDATE appearances SET person_id = ? WHERE person_id = ?", (primary_id, secondary_id))
        conn.execute("UPDATE person_identity_links SET person_id = ?, updated_at = CURRENT_TIMESTAMP WHERE person_id = ?", (primary_id, secondary_id))
        conn.execute("DELETE FROM people WHERE id = ?", (secondary_id,))
        conn.commit()
    finally:
        conn.close()

def merge_departments(primary_id: int, secondary_id: int) -> None:
    conn = get_connection()
    try:
        conn.execute("UPDATE appearances SET department_id = ? WHERE department_id = ?", (primary_id, secondary_id))
        conn.execute("UPDATE person_mentions SET department_id = ? WHERE department_id = ?", (primary_id, secondary_id))
        conn.execute("DELETE FROM departments WHERE id = ?", (secondary_id,))
        conn.commit()
    finally:
        conn.close()

def fetch_departments() -> list[sqlite3.Row]:
    conn = get_connection()
    try:
        return conn.execute(
            """
            SELECT d.*, COUNT(a.id) as appearance_count 
            FROM departments d 
            LEFT JOIN appearances a ON a.department_id = d.id 
            GROUP BY d.id 
            ORDER BY d.name
            """
        ).fetchall()
    finally:
        conn.close()
