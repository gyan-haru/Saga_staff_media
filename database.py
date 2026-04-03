from __future__ import annotations

import csv
import json
import re
import sqlite3
import urllib.parse
from datetime import date, datetime, timedelta
from functools import lru_cache
from itertools import combinations
from pathlib import Path
from typing import Any

from config import BASE_URL, DB_PATH, GENERIC_PRESS_RELEASE_LABELS, GENERIC_PROPOSAL_LABELS, LIST_SOURCES
from config import DEPARTMENT_HIERARCHY_CSV_PATH
from extractor import (
    PersonMention,
    ProjectRecord,
    build_person_key,
    build_person_mentions,
    classify_person_name_quality,
    clean_department_name,
    clean_person_name,
    is_valid_department_name,
    is_valid_person_name,
    normalize_text,
    normalize_person_name,
)

DEPARTMENT_BOUNDARY_SUFFIXES = ("県", "部", "局", "課", "室", "所", "班", "係", "チーム", "事務局", "担当", "委員会")
DEPARTMENT_SPECIFICITY_SUFFIXES = (
    ("担当", 7),
    ("係", 6),
    ("班", 6),
    ("チーム", 6),
    ("室", 6),
    ("事務局", 6),
    ("課", 5),
    ("所", 5),
    ("センター", 5),
    ("局", 4),
    ("部", 3),
    ("委員会", 3),
    ("県", 2),
    ("庁", 2),
)
DEPARTMENT_UNIT_SUFFIX_PATTERN = r"(?:事務局|センター|チーム|グループ|事業所|学科|課|部|局|室|所|班|係|担当)"
DEPARTMENT_CHILD_SUFFIXES = ("担当", "係", "班", "チーム", "室", "グループ", "センター")
DEPARTMENT_THEME_SUFFIXES = (
    "政策",
    "推進",
    "振興",
    "支援",
    "対策",
    "管理",
    "整備",
    "保全",
    "調整",
    "企画",
    "総務",
    "運営",
    "経営",
    "監理",
    "保護",
)
GENERIC_DEPARTMENT_THEME_TOKENS = {
    "佐賀県",
    "佐賀",
    "県",
    "県庁",
    "部",
    "局",
    "課",
    "室",
    "所",
    "班",
    "係",
    "担当",
    "事務局",
    "委員会",
    "センター",
    "グループ",
    "チーム",
    "政策",
    "推進",
    "振興",
    "支援",
    "対策",
    "管理",
    "整備",
    "保全",
    "調整",
    "企画",
    "総務",
    "運営",
    "経営",
    "監理",
}
GENERIC_PROJECT_THEME_TOKENS = {
    "佐賀県",
    "令和",
    "年度",
    "事業",
    "業務",
    "企画",
    "公募",
    "募集",
    "公告",
    "実施",
    "結果",
    "回答",
    "質問",
    "選定",
    "委託",
    "契約",
    "入札",
    "プロポーザル",
    "コンペ",
    "開催",
    "支援",
    "推進",
    "調査",
    "作成",
    "運営",
    "構築",
    "更新",
    "導入",
    "県",
    "ます",
    "します",
    "ました",
    "について",
    "に関する",
    "及び",
    "ため",
    "もの",
    "こと",
    "など",
    "お知らせ",
    "掲載",
    "公表",
}
GENERIC_POLICY_TOPIC_TOKENS = GENERIC_PROJECT_THEME_TOKENS | {
    "重点",
    "施策",
    "方針",
    "計画",
    "戦略",
    "会見",
    "記者会見",
    "予算",
    "政策",
    "取組",
    "取り組み",
    "実現",
    "推進",
    "支える",
}
POLICY_SOURCE_TYPE_LABELS = {
    "governor_press": "知事会見",
    "budget_brief": "予算資料",
    "policy_brief": "施策資料",
    "priority_plan": "重点方針",
    "manual": "手動登録",
}


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

            CREATE TABLE IF NOT EXISTS department_aliases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                department_id INTEGER NOT NULL,
                alias_name TEXT NOT NULL,
                normalized_alias TEXT NOT NULL UNIQUE,
                alias_type TEXT DEFAULT 'observed',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(department_id) REFERENCES departments(id) ON DELETE CASCADE
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
                source_list_url TEXT DEFAULT '',
                source_department_name TEXT DEFAULT '',
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

            CREATE TABLE IF NOT EXISTS employee_slots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                slot_key TEXT NOT NULL UNIQUE,
                normalized_person_name TEXT NOT NULL,
                display_name TEXT NOT NULL,
                name_quality TEXT DEFAULT 'unknown',
                department_id INTEGER,
                raw_department_name TEXT DEFAULT '',
                title_raw TEXT DEFAULT '',
                active_from TEXT DEFAULT '',
                active_to TEXT DEFAULT '',
                source_transfer_event_id INTEGER,
                previous_slot_id INTEGER,
                next_slot_id INTEGER,
                person_id INTEGER,
                slot_confidence REAL DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(department_id) REFERENCES departments(id) ON DELETE SET NULL,
                FOREIGN KEY(source_transfer_event_id) REFERENCES transfer_events(id) ON DELETE CASCADE,
                FOREIGN KEY(previous_slot_id) REFERENCES employee_slots(id) ON DELETE SET NULL,
                FOREIGN KEY(next_slot_id) REFERENCES employee_slots(id) ON DELETE SET NULL,
                FOREIGN KEY(person_id) REFERENCES people(id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS slot_candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                person_mention_id INTEGER NOT NULL,
                employee_slot_id INTEGER NOT NULL,
                candidate_score REAL DEFAULT 0,
                matched_by TEXT DEFAULT '',
                notes TEXT DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(person_mention_id, employee_slot_id),
                FOREIGN KEY(person_mention_id) REFERENCES person_mentions(id) ON DELETE CASCADE,
                FOREIGN KEY(employee_slot_id) REFERENCES employee_slots(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS project_tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                tag_name TEXT NOT NULL,
                UNIQUE(project_id, tag_name),
                FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS policy_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_type TEXT NOT NULL,
                source_key TEXT NOT NULL UNIQUE,
                title TEXT NOT NULL,
                url TEXT DEFAULT '',
                published_at TEXT DEFAULT '',
                source_year INTEGER DEFAULT 0,
                summary TEXT DEFAULT '',
                raw_text TEXT DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS policy_topics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_key TEXT NOT NULL UNIQUE,
                name TEXT NOT NULL,
                normalized_name TEXT NOT NULL,
                topic_year INTEGER DEFAULT 0,
                origin_type TEXT DEFAULT 'project_inferred',
                description TEXT DEFAULT '',
                keywords_json TEXT DEFAULT '[]',
                priority_weight REAL DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS topic_source_mentions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                topic_id INTEGER NOT NULL,
                policy_source_id INTEGER NOT NULL,
                evidence_snippet TEXT DEFAULT '',
                confidence REAL DEFAULT 0,
                matched_by TEXT DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(topic_id, policy_source_id),
                FOREIGN KEY(topic_id) REFERENCES policy_topics(id) ON DELETE CASCADE,
                FOREIGN KEY(policy_source_id) REFERENCES policy_sources(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS project_topic_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                project_id INTEGER NOT NULL,
                topic_id INTEGER NOT NULL,
                confidence REAL DEFAULT 0,
                matched_by TEXT DEFAULT '',
                evidence_snippet TEXT DEFAULT '',
                is_priority INTEGER DEFAULT 0,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(project_id, topic_id),
                FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE CASCADE,
                FOREIGN KEY(topic_id) REFERENCES policy_topics(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS person_topic_rollups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                person_id INTEGER NOT NULL,
                topic_id INTEGER NOT NULL,
                topic_year INTEGER DEFAULT 0,
                project_count INTEGER DEFAULT 0,
                department_count INTEGER DEFAULT 0,
                priority_project_count INTEGER DEFAULT 0,
                involvement_score REAL DEFAULT 0,
                continuity_score REAL DEFAULT 0,
                visibility_score REAL DEFAULT 0,
                spotlight_score REAL DEFAULT 0,
                first_seen_at TEXT DEFAULT '',
                last_seen_at TEXT DEFAULT '',
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(person_id, topic_id, topic_year),
                FOREIGN KEY(person_id) REFERENCES people(id) ON DELETE CASCADE,
                FOREIGN KEY(topic_id) REFERENCES policy_topics(id) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS interviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                person_id INTEGER NOT NULL,
                project_id INTEGER,
                title TEXT NOT NULL,
                content TEXT NOT NULL,
                published_at TEXT DEFAULT CURRENT_TIMESTAMP,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY(person_id) REFERENCES people(id) ON DELETE CASCADE,
                FOREIGN KEY(project_id) REFERENCES projects(id) ON DELETE SET NULL
            );

            CREATE INDEX IF NOT EXISTS idx_projects_source_type ON projects(source_type);
            CREATE INDEX IF NOT EXISTS idx_projects_published_at ON projects(published_at);
            CREATE INDEX IF NOT EXISTS idx_department_aliases_department_id ON department_aliases(department_id);
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
            CREATE INDEX IF NOT EXISTS idx_employee_slots_name ON employee_slots(normalized_person_name);
            CREATE INDEX IF NOT EXISTS idx_employee_slots_department_id ON employee_slots(department_id);
            CREATE INDEX IF NOT EXISTS idx_employee_slots_person_id ON employee_slots(person_id);
            CREATE INDEX IF NOT EXISTS idx_slot_candidates_mention_id ON slot_candidates(person_mention_id);
            CREATE INDEX IF NOT EXISTS idx_slot_candidates_slot_id ON slot_candidates(employee_slot_id);
            CREATE INDEX IF NOT EXISTS idx_policy_sources_year ON policy_sources(source_year);
            CREATE INDEX IF NOT EXISTS idx_policy_topics_year ON policy_topics(topic_year);
            CREATE INDEX IF NOT EXISTS idx_project_topic_links_project_id ON project_topic_links(project_id);
            CREATE INDEX IF NOT EXISTS idx_project_topic_links_topic_id ON project_topic_links(topic_id);
            CREATE INDEX IF NOT EXISTS idx_topic_source_mentions_topic_id ON topic_source_mentions(topic_id);
            CREATE INDEX IF NOT EXISTS idx_person_topic_rollups_person_id ON person_topic_rollups(person_id);
            CREATE INDEX IF NOT EXISTS idx_person_topic_rollups_topic_year ON person_topic_rollups(topic_year);
            CREATE INDEX IF NOT EXISTS idx_interviews_person_id ON interviews(person_id);
            CREATE INDEX IF NOT EXISTS idx_interviews_project_id ON interviews(project_id);
            """
        )
        conn.commit()

        # Schema migrations
        try:
            conn.execute("ALTER TABLE projects ADD COLUMN review_status TEXT DEFAULT 'pending'")
        except sqlite3.OperationalError:
            pass  # duplicate column name
        try:
            conn.execute("ALTER TABLE projects ADD COLUMN source_list_url TEXT DEFAULT ''")
        except sqlite3.OperationalError:
            pass  # duplicate column name
        try:
            conn.execute("ALTER TABLE projects ADD COLUMN source_department_name TEXT DEFAULT ''")
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
        seed_department_reference(conn)
        conn.commit()

    finally:
        conn.close()


def iter_department_reference_names() -> list[str]:
    generic_labels = GENERIC_PRESS_RELEASE_LABELS | GENERIC_PROPOSAL_LABELS
    names: list[str] = []
    seen: set[str] = set()
    for source in LIST_SOURCES:
        name = clean_department_name((source.get("department_name") or "").strip())
        normalized = normalize_department_for_match(name)
        if not name or name in generic_labels or not normalized or not is_valid_department_name(name):
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        names.append(name)
    return names


def upsert_department_row(conn: sqlite3.Connection, name: str) -> int | None:
    cleaned_name = canonicalize_department_display_name(name)
    normalized_name = normalize_department_for_match(cleaned_name)
    if not cleaned_name or not normalized_name:
        return None
    conn.execute(
        """
        INSERT INTO departments (name, normalized_name)
        VALUES (?, ?)
        ON CONFLICT(normalized_name) DO UPDATE SET name = excluded.name
        """,
        (cleaned_name, normalized_name),
    )
    row = conn.execute("SELECT id FROM departments WHERE normalized_name = ?", (normalized_name,)).fetchone()
    return int(row["id"]) if row else None


def upsert_department_alias(
    conn: sqlite3.Connection,
    department_id: int,
    alias_name: str,
    alias_type: str = "observed",
) -> None:
    alias = clean_department_name(alias_name)
    normalized_alias = normalize_department_for_match(alias)
    if not department_id or not alias or not normalized_alias or not is_valid_department_name(alias):
        return
    conn.execute(
        """
        INSERT INTO department_aliases (department_id, alias_name, normalized_alias, alias_type)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(normalized_alias) DO UPDATE SET
            department_id = excluded.department_id,
            alias_name = excluded.alias_name,
            alias_type = excluded.alias_type
        """,
        (department_id, alias, normalized_alias, alias_type),
    )


def seed_department_reference(conn: sqlite3.Connection) -> int:
    seeded = 0
    for name in iter_department_reference_names():
        department_id = upsert_department_row(conn, name)
        if not department_id:
            continue
        upsert_department_alias(conn, department_id, name, alias_type="source_list")
        seeded += 1
    return seeded


def department_specificity_rank(name: str) -> int:
    candidate = clean_department_name(name)
    if not candidate:
        return 0
    for suffix, rank in DEPARTMENT_SPECIFICITY_SUFFIXES:
        if candidate.endswith(suffix):
            return rank
    best = 0
    for suffix, rank in DEPARTMENT_SPECIFICITY_SUFFIXES:
        if suffix in candidate:
            best = max(best, rank)
    return best


def department_reference_match_score(candidate_norm: str, alias_norm: str, alias_name: str) -> tuple[int, int, int] | None:
    if not candidate_norm or not alias_norm:
        return None
    if candidate_norm == alias_norm:
        return (department_specificity_rank(alias_name), 3, len(alias_norm))
    if candidate_norm.startswith(alias_norm):
        return (department_specificity_rank(alias_name), 2, len(alias_norm))

    index = candidate_norm.find(alias_norm)
    while index >= 0:
        prefix = candidate_norm[:index]
        if any(prefix.endswith(suffix) for suffix in DEPARTMENT_BOUNDARY_SUFFIXES):
            return (department_specificity_rank(alias_name), 1, len(alias_norm))
        index = candidate_norm.find(alias_norm, index + 1)

    return None


def resolve_department_reference_id(conn: sqlite3.Connection, name: str) -> int | None:
    candidate = clean_department_name(name)
    candidate_norm = normalize_department_for_match(candidate)
    if not candidate or not candidate_norm or not is_valid_department_name(candidate):
        return None

    exact_department = conn.execute(
        "SELECT id FROM departments WHERE normalized_name = ?",
        (candidate_norm,),
    ).fetchone()
    if exact_department:
        return int(exact_department["id"])

    exact_alias = conn.execute(
        "SELECT department_id FROM department_aliases WHERE normalized_alias = ?",
        (candidate_norm,),
    ).fetchone()
    if exact_alias:
        return int(exact_alias["department_id"])

    best_department_id: int | None = None
    best_score: tuple[int, int, int] | None = None
    for row in conn.execute(
        """
        SELECT department_id, normalized_alias, alias_name
        FROM department_aliases
        WHERE alias_type = 'source_list'
        ORDER BY LENGTH(normalized_alias) DESC, department_id ASC
        """
    ).fetchall():
        score = department_reference_match_score(
            candidate_norm,
            row["normalized_alias"] or "",
            row["alias_name"] or "",
        )
        if score is not None and (best_score is None or score > best_score):
            best_score = score
            best_department_id = int(row["department_id"])
    return best_department_id if best_score is not None else None


def get_or_create_department(conn: sqlite3.Connection, name: str) -> int | None:
    cleaned_name = clean_department_name(name)
    if not cleaned_name or not is_valid_department_name(cleaned_name):
        return None

    canonical_name = canonicalize_department_display_name(cleaned_name)
    canonical_id = resolve_department_reference_id(conn, canonical_name)
    if canonical_id:
        canonical_row = conn.execute(
            "SELECT name, normalized_name FROM departments WHERE id = ?",
            (canonical_id,),
        ).fetchone()
        if canonical_row:
            matched_name = canonical_row["name"] or ""
            matched_norm = canonical_row["normalized_name"] or ""
            candidate_norm = normalize_department_for_match(canonical_name)
            if matched_norm == candidate_norm or not should_preserve_specific_department(canonical_name, matched_name):
                upsert_department_alias(conn, canonical_id, canonical_name, alias_type="observed")
                upsert_department_alias(conn, canonical_id, cleaned_name, alias_type="observed")
                return canonical_id

    department_id = upsert_department_row(conn, canonical_name)
    if department_id:
        upsert_department_alias(conn, department_id, canonical_name, alias_type="observed")
        upsert_department_alias(conn, department_id, cleaned_name, alias_type="observed")
    return department_id


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
    if mention.name_quality == "surname_only":
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


def extract_department_units(name: str) -> list[str]:
    cleaned = clean_department_name(name)
    if not cleaned:
        return []
    spaced_tokens = [
        token.strip(" 　:：,、・()（）[]【】")
        for token in cleaned.split()
        if token.strip(" 　:：,、・()（）[]【】")
    ]
    token_units = [
        token for token in spaced_tokens if re.search(rf"{DEPARTMENT_UNIT_SUFFIX_PATTERN}$", token)
    ]
    if len(token_units) >= 2:
        return token_units
    if len(token_units) == 1 and spaced_tokens[:1] and spaced_tokens[0] in {"佐賀県", "県", "佐賀県庁", "県庁"}:
        return token_units
    if len(token_units) == 1 and len(spaced_tokens) == 1:
        return token_units
    candidate = cleaned.replace(" ", "")
    units = [
        match.group(0).strip(" 　:：,、・()（）[]【】")
        for match in re.finditer(rf".+?{DEPARTMENT_UNIT_SUFFIX_PATTERN}", candidate)
    ]
    return [unit for unit in units if unit]


def department_semantic_units(name: str) -> list[str]:
    units = extract_department_units(name)
    if not units:
        cleaned = clean_department_name(name)
        return [cleaned] if cleaned else []
    if len(units) == 1:
        return units
    if any(units[-1].endswith(suffix) for suffix in DEPARTMENT_CHILD_SUFFIXES):
        return units[-2:]
    if len(units) == 2 and units[-2].endswith(("部", "局", "委員会", "事務局")):
        return units[-2:]
    return [units[-1]]


def canonicalize_department_display_name(name: str) -> str:
    cleaned = clean_department_name(name)
    units = department_semantic_units(name)
    if not units:
        return cleaned
    if len(units) == 1 and " " in cleaned:
        return cleaned
    return " ".join(units)


def normalize_department_for_match(name: str) -> str:
    units = department_semantic_units(name)
    if units:
        return "".join(units)
    return clean_department_name(name).replace(" ", "")


def should_preserve_specific_department(candidate: str, matched_name: str) -> bool:
    candidate_name = canonicalize_department_display_name(candidate)
    matched_name = canonicalize_department_display_name(matched_name)
    candidate_norm = normalize_department_for_match(candidate_name)
    matched_norm = normalize_department_for_match(matched_name)
    if not candidate_norm or not matched_norm or candidate_norm == matched_norm:
        return False
    candidate_units = department_semantic_units(candidate_name)
    matched_units = department_semantic_units(matched_name)
    if len(candidate_units) > len(matched_units):
        return True
    return department_specificity_rank(candidate_name) > department_specificity_rank(matched_name)


def parse_iso_date(value: str) -> date | None:
    text = (value or "").strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y-%m-%dT%H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(text[:19], fmt).date()
        except ValueError:
            continue
    match = re.match(r"(\d{4})-(\d{2})-(\d{2})", text)
    if match:
        return date(int(match.group(1)), int(match.group(2)), int(match.group(3)))
    return None


def coalesce_row_date(*values: str) -> date | None:
    for value in values:
        parsed = parse_iso_date(value)
        if parsed:
            return parsed
    return None


def days_between_dates(left: date | None, right: date | None) -> int | None:
    if not left or not right:
        return None
    return abs((left - right).days)


def same_calendar_year(left: date | None, right: date | None) -> bool:
    return bool(left and right and left.year == right.year)


def mentions_distinct_same_year_departments(
    previous_mention: dict[str, Any],
    current_mention: dict[str, Any],
) -> bool:
    if not same_calendar_year(previous_mention.get("observed_date"), current_mention.get("observed_date")):
        return False
    if (
        (previous_mention.get("normalized_person_name") or "").strip()
        != (current_mention.get("normalized_person_name") or "").strip()
    ):
        return False

    previous_department = previous_mention.get("raw_department_name") or ""
    current_department = current_mention.get("raw_department_name") or ""
    if not previous_department or not current_department:
        return False
    if department_match_overlap(previous_department, current_department):
        return False

    previous_quality = previous_mention.get("name_quality") or "unknown"
    current_quality = current_mention.get("name_quality") or "unknown"
    return "surname_only" in {previous_quality, current_quality}


def iso_day_before(value: str) -> str:
    parsed = parse_iso_date(value)
    if not parsed:
        return value
    return (parsed - timedelta(days=1)).isoformat()


def department_match_overlap(left: str, right: str) -> bool:
    left_normalized = normalize_department_for_match(left)
    right_normalized = normalize_department_for_match(right)
    if not left_normalized or not right_normalized:
        return False
    if left_normalized == right_normalized:
        return True
    if left_normalized in right_normalized or right_normalized in left_normalized:
        return True
    return False


def department_theme_tokens(name: str) -> set[str]:
    cleaned = clean_department_name(name)
    if not cleaned:
        return set()

    candidates = department_semantic_units(cleaned) or [cleaned]
    tokens: set[str] = set()

    for candidate in candidates:
        base = candidate
        base = re.sub(r"^(佐賀県庁|佐賀県|県庁|県)\s*", "", base)
        base = re.sub(rf"(?:{DEPARTMENT_UNIT_SUFFIX_PATTERN})$", "", base)
        base = base.strip(" 　:：,、・()（）[]【】")
        if not base:
            continue

        for part in re.findall(r"[一-龥ぁ-んァ-ヴーA-Za-z0-9]{2,16}", base):
            token = part.strip()
            if len(token) < 2 or token in GENERIC_DEPARTMENT_THEME_TOKENS:
                continue
            tokens.add(token)
            for suffix in DEPARTMENT_THEME_SUFFIXES:
                if token.endswith(suffix) and len(token) > len(suffix) + 1:
                    trimmed = token[: -len(suffix)]
                    if len(trimmed) >= 2 and trimmed not in GENERIC_DEPARTMENT_THEME_TOKENS:
                        tokens.add(trimmed)
    return tokens


def department_theme_overlap(left: str, right: str) -> bool:
    left_tokens = department_theme_tokens(left)
    right_tokens = department_theme_tokens(right)
    if not left_tokens or not right_tokens:
        return False
    if left_tokens & right_tokens:
        return True
    for left_token in left_tokens:
        for right_token in right_tokens:
            shorter = min(len(left_token), len(right_token))
            if shorter >= 3 and (left_token in right_token or right_token in left_token):
                return True
    return False


def project_theme_tokens(*texts: str) -> set[str]:
    tokens: set[str] = set()

    def expand_fragments(candidate: str) -> set[str]:
        expanded: set[str] = set()
        for fragment in re.findall(r"[一-龥]+[ぁ-ん]*|[ァ-ヴー]+|[A-Za-z]+", candidate):
            item = fragment.strip()
            if len(item) >= 2:
                expanded.add(item)
            if re.fullmatch(r"[一-龥]{4,12}", item):
                for index in range(0, len(item), 2):
                    pair = item[index:index + 2]
                    if len(pair) == 2:
                        expanded.add(pair)
        return expanded

    for text in texts:
        normalized = normalize_text(text or "")
        if not normalized:
            continue
        normalized = re.sub(r"[0-9０-９]+", " ", normalized)
        for token in re.findall(r"[一-龥ぁ-んァ-ヴーA-Za-z]{2,18}", normalized):
            candidate = token.strip()
            if len(candidate) < 2:
                continue
            if candidate in GENERIC_PROJECT_THEME_TOKENS:
                continue
            if candidate.endswith(("します", "しました", "ます", "でした")):
                continue
            if candidate.startswith(("令和", "佐賀県")):
                continue
            tokens.add(candidate)
            for fragment in expand_fragments(candidate):
                if len(fragment) >= 2 and fragment not in GENERIC_PROJECT_THEME_TOKENS:
                    tokens.add(fragment)
            for suffix in DEPARTMENT_THEME_SUFFIXES:
                if candidate.endswith(suffix) and len(candidate) > len(suffix) + 1:
                    trimmed = candidate[: -len(suffix)]
                    if len(trimmed) >= 2 and trimmed not in GENERIC_PROJECT_THEME_TOKENS:
                        tokens.add(trimmed)
    return tokens


def extract_year_from_date_text(value: str) -> int:
    parsed = parse_iso_date(value)
    if parsed:
        return parsed.year
    match = re.search(r"(20\d{2})", value or "")
    if match:
        return int(match.group(1))
    return 0


def normalize_topic_name(name: str) -> str:
    return normalize_text(name or "").replace(" ", "")


def is_valid_topic_name(name: str) -> bool:
    candidate = normalize_text(name or "").strip()
    if not candidate or len(candidate) < 2 or len(candidate) > 14:
        return False
    if candidate in GENERIC_POLICY_TOPIC_TOKENS:
        return False
    if "年度" in candidate or "佐賀県" in candidate:
        return False
    if candidate.endswith(("について", "に関する", "についての", "の推進", "の実施", "の開催")):
        return False
    if candidate.startswith(("令和", "佐賀県", "県")):
        return False
    if re.fullmatch(r"[A-Za-z0-9]+", candidate):
        return False
    return True


def build_policy_source_key(source_type: str, title: str, url: str = "", published_at: str = "") -> str:
    stable_url = (url or "").strip()
    stable_title = normalize_text(title or "")
    stable_date = (published_at or "").strip()
    anchor = stable_url or stable_title
    return f"{source_type}:{anchor}:{stable_date}"


def build_policy_topic_key(name: str, topic_year: int = 0, origin_type: str = "project_inferred") -> str:
    normalized = normalize_topic_name(name)
    return f"{origin_type}:{topic_year}:{normalized}"


def decode_topic_keywords(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        payload = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []
    keywords: list[str] = []
    for item in payload:
        token = normalize_text(str(item or "")).strip()
        if token and token not in keywords:
            keywords.append(token)
    return keywords


def encode_topic_keywords(values: list[str] | set[str]) -> str:
    unique: list[str] = []
    for item in values:
        token = normalize_text(str(item or "")).strip()
        if token and token not in unique:
            unique.append(token)
    return json.dumps(unique, ensure_ascii=False)


def topic_tokens_for_matching(topic_name: str, description: str = "", keywords: list[str] | None = None) -> set[str]:
    tokens = set(project_theme_tokens(topic_name, description))
    for keyword in keywords or []:
        if is_valid_topic_name(keyword):
            tokens.add(normalize_text(keyword))
    if is_valid_topic_name(topic_name):
        tokens.add(normalize_text(topic_name))
    return {token for token in tokens if is_valid_topic_name(token)}


def derive_ranked_topic_candidates(
    title: str,
    summary: str = "",
    purpose: str = "",
    department_name: str = "",
    extra_texts: list[str] | None = None,
    limit: int = 6,
) -> list[dict[str, Any]]:
    scores: dict[str, int] = {}

    def add_tokens(tokens: set[str], weight: int) -> None:
        for token in tokens:
            if not is_valid_topic_name(token):
                continue
            normalized = normalize_topic_name(token)
            if not normalized:
                continue
            scores[token] = scores.get(token, 0) + weight

    add_tokens(project_theme_tokens(title), 5)
    add_tokens(project_theme_tokens(summary), 3)
    add_tokens(project_theme_tokens(purpose), 4)
    add_tokens(department_theme_tokens(department_name), 2)
    for text in extra_texts or []:
        add_tokens(project_theme_tokens(text), 2)

    ranked = sorted(scores.items(), key=lambda item: (item[1], len(item[0]), item[0]), reverse=True)
    chosen: list[dict[str, Any]] = []
    for token, score in ranked:
        if any(
            token != existing["name"]
            and (token in existing["name"] or existing["name"] in token)
            and existing["score"] >= score
            for existing in chosen
        ):
            continue
        chosen.append(
            {
                "name": token,
                "score": score,
                "keywords": [token],
            }
        )
        if len(chosen) >= limit:
            break
    return chosen


def partial_person_name_match(left: str, right: str) -> bool:
    left_normalized = normalize_person_name(left)
    right_normalized = normalize_person_name(right)
    if not left_normalized or not right_normalized or left_normalized == right_normalized:
        return False
    shorter = min(len(left_normalized), len(right_normalized))
    if shorter < 2:
        return False
    return left_normalized.startswith(right_normalized) or right_normalized.startswith(left_normalized)


def is_valid_roster_name(name: str) -> bool:
    normalized = normalize_person_name(name)
    if not normalized or not is_valid_person_name(name):
        return False
    if normalized in {"署名", "主事", "主任", "長", "あて", "印", "A印"}:
        return False
    if re.fullmatch(r"[A-Za-z]+", normalized):
        return False
    if normalized.endswith("印") and len(normalized) <= 3:
        return False
    if normalized.endswith(("主事", "主任", "係長", "課長", "部長", "局長", "技師", "担当")) and len(normalized) <= 4:
        return False
    return True


def is_viable_roster_mention_candidate(item: dict[str, Any]) -> bool:
    if item.get("source_note"):
        return True
    if item.get("contact_email") or item.get("contact_phone"):
        return True
    if item.get("name_quality") == "full_name":
        return True
    if int(item.get("mention_count") or 0) >= 2:
        return True
    return float(item.get("confidence") or 0) >= 0.9


@lru_cache(maxsize=1)
def load_department_hierarchy(csv_path: Path | str = DEPARTMENT_HIERARCHY_CSV_PATH) -> list[dict[str, str]]:
    path = Path(csv_path)
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = []
        for row in reader:
            top_unit = (row.get("top_unit") or "").strip()
            child_name = (row.get("child_name") or "").strip()
            if not top_unit:
                continue
            rows.append(
                {
                    "top_unit": top_unit,
                    "top_url": urllib.parse.urljoin(BASE_URL + "/", (row.get("top_url") or "").strip()),
                    "child_name": child_name,
                    "child_url": urllib.parse.urljoin(BASE_URL + "/", (row.get("child_url") or "").strip()),
                    "tel": (row.get("tel") or "").strip(),
                    "fax": (row.get("fax") or "").strip(),
                    "top_norm": normalize_department_for_match(top_unit),
                    "child_norm": normalize_department_for_match(child_name) if child_name else "",
                }
            )
        return rows


def resolve_department_hierarchy(name: str) -> dict[str, str]:
    department_name = (name or "").strip()
    if not department_name:
        return {"top_unit": "未分類", "top_url": "", "child_name": "", "child_url": ""}

    normalized = normalize_department_for_match(department_name)
    best_row: dict[str, str] | None = None
    best_score = -1
    for row in load_department_hierarchy():
        child_norm = row["child_norm"]
        top_norm = row["top_norm"]
        score = -1
        if child_norm and (normalized == child_norm or child_norm in normalized or normalized in child_norm):
            score = 100 + len(child_norm)
        elif top_norm and top_norm in normalized:
            score = 10 + len(top_norm)
        if score > best_score:
            best_score = score
            best_row = row

    if best_row:
        return {
            "top_unit": best_row["top_unit"],
            "top_url": best_row["top_url"],
            "child_name": best_row["child_name"],
            "child_url": best_row["child_url"],
        }
    return {"top_unit": "未分類", "top_url": "", "child_name": "", "child_url": ""}


def build_department_roster_groups(
    confirmed_people: list[dict[str, Any]],
    slot_roster: list[dict[str, Any]],
    transfer_candidates: list[dict[str, Any]],
    mention_candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    groups: dict[str, dict[str, Any]] = {}

    def get_group(name: str) -> dict[str, Any]:
        hierarchy = resolve_department_hierarchy(name)
        key = hierarchy["top_unit"]
        group = groups.setdefault(
            key,
            {
                "top_unit": hierarchy["top_unit"],
                "top_url": hierarchy["top_url"],
                "confirmed_people": [],
                "slot_roster": [],
                "transfer_candidates": [],
                "mention_candidates": [],
                "total_count": 0,
            },
        )
        return group

    for person in confirmed_people:
        group = get_group(person.get("current_department") or person.get("previous_department") or "")
        group["confirmed_people"].append(person)
        group["total_count"] += 1

    for item in slot_roster:
        group = get_group(item.get("current_department") or item.get("previous_department") or "")
        group["slot_roster"].append(item)
        group["total_count"] += 1

    for item in transfer_candidates:
        group = get_group(item.get("current_department") or item.get("previous_department") or "")
        group["transfer_candidates"].append(item)
        group["total_count"] += 1

    for item in mention_candidates:
        group = get_group(item.get("department_name") or "")
        group["mention_candidates"].append(item)
        group["total_count"] += 1

    payload = list(groups.values())
    payload.sort(
        key=lambda item: (
            item["total_count"],
            len(item["confirmed_people"]),
            item["top_unit"],
        ),
        reverse=True,
    )
    return payload


def hydrate_department_group_profiles(
    conn: sqlite3.Connection,
    department_groups: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not department_groups:
        return department_groups

    group_map = {group["top_unit"]: group for group in department_groups}
    child_map: dict[str, list[str]] = {group["top_unit"]: [] for group in department_groups}
    for row in load_department_hierarchy():
        top_unit = row.get("top_unit") or ""
        child_name = (row.get("child_name") or "").strip()
        if top_unit in child_map and child_name and child_name not in child_map[top_unit]:
            child_map[top_unit].append(child_name)

    keyword_counts: dict[str, dict[str, int]] = {group["top_unit"]: {} for group in department_groups}
    project_rows = conn.execute(
        """
        SELECT
            p.title,
            p.summary,
            p.purpose,
            COALESCE(NULLIF(a.raw_department_name, ''), NULLIF(p.source_department_name, '')) AS department_name
        FROM projects p
        LEFT JOIN appearances a ON a.project_id = p.id
        WHERE p.review_status != 'rejected'
        """
    ).fetchall()
    for row in project_rows:
        department_name = (row["department_name"] or "").strip()
        if not department_name:
            continue
        top_unit = resolve_department_hierarchy(department_name).get("top_unit", "")
        if top_unit not in keyword_counts:
            continue
        for token in department_theme_tokens(department_name):
            keyword_counts[top_unit][token] = keyword_counts[top_unit].get(token, 0) + 2
        for token in project_theme_tokens(row["title"] or "", row["summary"] or "", row["purpose"] or ""):
            keyword_counts[top_unit][token] = keyword_counts[top_unit].get(token, 0) + 1

    for top_unit, group in group_map.items():
        sample_people: list[str] = []
        for source in ("confirmed_people", "slot_roster", "transfer_candidates", "mention_candidates"):
            for item in group.get(source, []):
                name = (item.get("display_name") or "").strip()
                if name and name not in sample_people:
                    sample_people.append(name)
                if len(sample_people) >= 6:
                    break
            if len(sample_people) >= 6:
                break

        focus_keywords = [
            token
            for token, _count in sorted(
                keyword_counts[top_unit].items(),
                key=lambda item: (item[1], len(item[0]), item[0]),
                reverse=True,
            )[:6]
        ]
        child_departments = child_map.get(top_unit, [])[:8]
        group["focus_keywords"] = focus_keywords
        group["child_departments"] = child_departments
        group["sample_people"] = sample_people
        if focus_keywords:
            group["profile_summary"] = " / ".join(focus_keywords[:4]) + " まわりの企画が多い"
        elif child_departments:
            group["profile_summary"] = " / ".join(child_departments[:4]) + " などを束ねる部局"
        else:
            group["profile_summary"] = ""

    return department_groups


def resolve_department_profile_metadata(name: str) -> dict[str, str]:
    hierarchy = resolve_department_hierarchy(name)
    tel = ""
    fax = ""
    target_norm = normalize_department_for_match(name)
    for row in load_department_hierarchy():
        child_name = (row.get("child_name") or "").strip()
        if not child_name:
            continue
        child_norm = normalize_department_for_match(child_name)
        if not child_norm:
            continue
        if target_norm == child_norm or target_norm in child_norm or child_norm in target_norm:
            tel = (row.get("tel") or "").strip()
            fax = (row.get("fax") or "").strip()
            if not hierarchy.get("child_url"):
                hierarchy["child_url"] = row.get("child_url") or ""
            break
    hierarchy["tel"] = tel
    hierarchy["fax"] = fax
    return hierarchy


def fetch_department_profiles(limit: int = 200, db_path: Path | str = DB_PATH) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        department_rows = fetch_departments(db_path=db_path)
        profiles: list[dict[str, Any]] = []
        for row in department_rows[:limit]:
            department_name = row["name"] or ""
            metadata = resolve_department_profile_metadata(department_name)

            linked_project_rows = conn.execute(
                """
                WITH linked_projects AS (
                    SELECT DISTINCT a.project_id
                    FROM appearances a
                    WHERE a.department_id = ?
                    UNION
                    SELECT DISTINCT pm.project_id
                    FROM person_mentions pm
                    WHERE pm.department_id = ?
                )
                SELECT
                    p.id,
                    p.title,
                    p.summary,
                    p.purpose,
                    COALESCE(NULLIF(p.published_at, ''), substr(p.fetched_at, 1, 10)) AS observed_at
                FROM linked_projects lp
                JOIN projects p ON p.id = lp.project_id
                WHERE p.review_status != 'rejected'
                ORDER BY observed_at DESC, p.id DESC
                LIMIT 12
                """,
                (row["id"], row["id"]),
            ).fetchall()

            people_rows = conn.execute(
                """
                WITH linked_people AS (
                    SELECT DISTINCT a.person_id
                    FROM appearances a
                    WHERE a.department_id = ?
                      AND a.person_id IS NOT NULL
                    UNION
                    SELECT DISTINCT pil.person_id
                    FROM person_mentions pm
                    JOIN person_identity_links pil ON pil.person_mention_id = pm.id
                    WHERE pm.department_id = ?
                      AND pil.person_id IS NOT NULL
                      AND pil.link_status IN ('reviewed_match', 'auto_matched')
                )
                SELECT pe.person_key, pe.display_name
                FROM linked_people lp
                JOIN people pe ON pe.id = lp.person_id
                ORDER BY pe.display_name ASC
                LIMIT 8
                """,
                (row["id"], row["id"]),
            ).fetchall()

            active_slots = conn.execute(
                """
                SELECT display_name
                FROM employee_slots
                WHERE department_id = ?
                  AND ? >= COALESCE(NULLIF(active_from, ''), '0000-00-00')
                  AND ? <= COALESCE(NULLIF(active_to, ''), '9999-12-31')
                ORDER BY display_name ASC
                LIMIT 8
                """,
                (row["id"], date.today().isoformat(), date.today().isoformat()),
            ).fetchall()
            mention_people = conn.execute(
                """
                SELECT DISTINCT raw_person_name AS display_name
                FROM person_mentions
                WHERE department_id = ?
                  AND IFNULL(TRIM(raw_person_name), '') != ''
                ORDER BY raw_person_name ASC
                LIMIT 8
                """,
                (row["id"],),
            ).fetchall()

            keyword_counts: dict[str, int] = {}
            for token in department_theme_tokens(department_name):
                keyword_counts[token] = keyword_counts.get(token, 0) + 2
            for project_row in linked_project_rows:
                for token in project_theme_tokens(
                    project_row["title"] or "",
                    project_row["summary"] or "",
                    project_row["purpose"] or "",
                ):
                    keyword_counts[token] = keyword_counts.get(token, 0) + 1

            focus_keywords = [
                token
                for token, _count in sorted(
                    keyword_counts.items(),
                    key=lambda item: (item[1], len(item[0]), item[0]),
                    reverse=True,
                )[:6]
            ]
            visible_people = []
            for source in (people_rows, active_slots, mention_people):
                for item in source:
                    name = (item["display_name"] or "").strip()
                    if name and name not in visible_people:
                        visible_people.append(name)
                    if len(visible_people) >= 8:
                        break
                if len(visible_people) >= 8:
                    break

            summary = ""
            if focus_keywords:
                summary = " / ".join(focus_keywords[:4]) + " に関わる企画が多い部署"
            elif metadata.get("top_unit") and metadata["top_unit"] != "未分類":
                summary = f"{metadata['top_unit']}に属する部署"

            profiles.append(
                {
                    "id": int(row["id"]),
                    "name": department_name,
                    "normalized_name": row["normalized_name"] or "",
                    "top_unit": metadata.get("top_unit", "未分類"),
                    "top_url": metadata.get("top_url", ""),
                    "child_name": metadata.get("child_name", "その他") or "その他",
                    "child_url": metadata.get("child_url", ""),
                    "tel": metadata.get("tel", ""),
                    "fax": metadata.get("fax", ""),
                    "appearance_count": int(row["appearance_count"] or 0),
                    "mention_count": int(row["mention_count"] or 0),
                    "project_count": len(linked_project_rows),
                    "profile_summary": summary,
                    "focus_keywords": focus_keywords,
                    "visible_people": visible_people,
                    "recent_projects": [
                        {
                            "id": int(project_row["id"]),
                            "title": project_row["title"] or "",
                            "observed_at": project_row["observed_at"] or "",
                        }
                        for project_row in linked_project_rows[:3]
                    ],
                    "linked_people": [
                        {
                            "person_key": person_row["person_key"] or "",
                            "display_name": person_row["display_name"] or "",
                        }
                        for person_row in people_rows
                    ],
                }
            )

        profiles.sort(
            key=lambda item: (
                item["project_count"],
                item["appearance_count"] + item["mention_count"],
                item["name"],
            ),
            reverse=True,
        )
        return profiles
    finally:
        conn.close()


def group_department_profiles_by_top_unit(
    departments: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for department in departments:
        top_unit = (department.get("top_unit") or "未分類").strip() or "未分類"
        group = grouped.setdefault(
            top_unit,
            {
                "top_unit": top_unit,
                "top_url": department.get("top_url") or "",
                "departments": [],
                "department_count": 0,
                "project_count": 0,
                "theme_counts": {},
                "sample_people": [],
            },
        )
        if not group["top_url"] and department.get("top_url"):
            group["top_url"] = department["top_url"]
        group["departments"].append(department)
        group["department_count"] += 1
        group["project_count"] += int(department.get("project_count") or 0)

        theme_counts: dict[str, int] = group["theme_counts"]
        project_weight = max(int(department.get("project_count") or 0), 1)
        for index, token in enumerate(department.get("focus_keywords") or []):
            theme_counts[token] = theme_counts.get(token, 0) + max(1, 5 - index) + project_weight

        for person_name in department.get("visible_people") or []:
            if person_name and person_name not in group["sample_people"]:
                group["sample_people"].append(person_name)
            if len(group["sample_people"]) >= 8:
                break

    payload: list[dict[str, Any]] = []
    for group in grouped.values():
        representative_themes = [
            token
            for token, _count in sorted(
                group["theme_counts"].items(),
                key=lambda item: (item[1], len(item[0]), item[0]),
                reverse=True,
            )[:6]
        ]
        departments_in_group = sorted(
            group["departments"],
            key=lambda item: (
                int(item.get("project_count") or 0),
                int(item.get("appearance_count") or 0) + int(item.get("mention_count") or 0),
                item.get("name") or "",
            ),
            reverse=True,
        )
        payload.append(
            {
                "top_unit": group["top_unit"],
                "top_url": group["top_url"],
                "department_count": group["department_count"],
                "project_count": group["project_count"],
                "representative_themes": representative_themes,
                "sample_people": group["sample_people"][:8],
                "departments": departments_in_group,
            }
        )

    payload.sort(
        key=lambda item: (
            item["project_count"],
            item["department_count"],
            item["top_unit"],
        ),
        reverse=True,
    )
    return payload


def upsert_policy_source(
    conn: sqlite3.Connection,
    source_type: str,
    title: str,
    url: str = "",
    published_at: str = "",
    summary: str = "",
    raw_text: str = "",
) -> int:
    source_year = extract_year_from_date_text(published_at or title)
    source_key = build_policy_source_key(source_type, title, url, published_at)
    conn.execute(
        """
        INSERT INTO policy_sources (
            source_type, source_key, title, url, published_at, source_year, summary, raw_text
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_key) DO UPDATE SET
            title = excluded.title,
            url = excluded.url,
            published_at = excluded.published_at,
            source_year = excluded.source_year,
            summary = excluded.summary,
            raw_text = excluded.raw_text,
            updated_at = CURRENT_TIMESTAMP
        """,
        (source_type, source_key, title, url, published_at, source_year, summary, raw_text),
    )
    row = conn.execute("SELECT id FROM policy_sources WHERE source_key = ?", (source_key,)).fetchone()
    return int(row["id"])


def upsert_policy_topic(
    conn: sqlite3.Connection,
    name: str,
    *,
    topic_year: int = 0,
    origin_type: str = "project_inferred",
    description: str = "",
    keywords: list[str] | set[str] | None = None,
    priority_weight: float = 0.0,
) -> int | None:
    cleaned_name = normalize_text(name or "").strip()
    if not is_valid_topic_name(cleaned_name):
        return None
    topic_key = build_policy_topic_key(cleaned_name, topic_year, origin_type)
    normalized_name = normalize_topic_name(cleaned_name)
    encoded_keywords = encode_topic_keywords(list(keywords or []) or [cleaned_name])
    conn.execute(
        """
        INSERT INTO policy_topics (
            topic_key, name, normalized_name, topic_year, origin_type, description, keywords_json, priority_weight
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(topic_key) DO UPDATE SET
            description = CASE
                WHEN IFNULL(TRIM(excluded.description), '') != '' THEN excluded.description
                ELSE policy_topics.description
            END,
            keywords_json = CASE
                WHEN IFNULL(TRIM(excluded.keywords_json), '') != '' THEN excluded.keywords_json
                ELSE policy_topics.keywords_json
            END,
            priority_weight = MAX(policy_topics.priority_weight, excluded.priority_weight),
            updated_at = CURRENT_TIMESTAMP
        """,
        (
            topic_key,
            cleaned_name,
            normalized_name,
            topic_year,
            origin_type,
            (description or "").strip(),
            encoded_keywords,
            float(priority_weight or 0),
        ),
    )
    row = conn.execute("SELECT id FROM policy_topics WHERE topic_key = ?", (topic_key,)).fetchone()
    return int(row["id"]) if row else None


def replace_policy_source_topics(
    conn: sqlite3.Connection,
    policy_source_id: int,
    topic_candidates: list[dict[str, Any]],
) -> int:
    conn.execute("DELETE FROM topic_source_mentions WHERE policy_source_id = ?", (policy_source_id,))
    inserted = 0
    for candidate in topic_candidates:
        topic_id = upsert_policy_topic(
            conn,
            candidate.get("name") or "",
            topic_year=int(candidate.get("topic_year") or 0),
            origin_type="policy_source",
            description=candidate.get("description") or "",
            keywords=candidate.get("keywords") or [],
            priority_weight=float(candidate.get("priority_weight") or 0.8),
        )
        if not topic_id:
            continue
        conn.execute(
            """
            INSERT INTO topic_source_mentions (
                topic_id, policy_source_id, evidence_snippet, confidence, matched_by
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(topic_id, policy_source_id) DO UPDATE SET
                evidence_snippet = excluded.evidence_snippet,
                confidence = excluded.confidence,
                matched_by = excluded.matched_by,
                updated_at = CURRENT_TIMESTAMP
            """,
            (
                topic_id,
                policy_source_id,
                (candidate.get("evidence_snippet") or "").strip(),
                float(candidate.get("confidence") or 0.85),
                (candidate.get("matched_by") or "policy_source").strip(),
            ),
        )
        inserted += 1
    return inserted


def derive_policy_source_topic_candidates(
    title: str,
    summary: str = "",
    raw_text: str = "",
    topic_names: list[str] | None = None,
    topic_year: int = 0,
    limit: int = 8,
) -> list[dict[str, Any]]:
    if topic_names:
        names = [normalize_text(item or "").strip() for item in topic_names]
        ranked_names = [item for item in names if is_valid_topic_name(item)]
    else:
        ranked_names = [
            item["name"]
            for item in derive_ranked_topic_candidates(title, summary, raw_text, limit=limit)
        ]

    candidates: list[dict[str, Any]] = []
    seen: set[str] = set()
    source_text = " ".join(part for part in [title, summary, raw_text] if part).strip()
    for index, name in enumerate(ranked_names):
        normalized = normalize_topic_name(name)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        candidates.append(
            {
                "name": name,
                "topic_year": topic_year,
                "description": normalize_text(summary or "").strip(),
                "keywords": sorted(topic_tokens_for_matching(name, summary)),
                "priority_weight": max(0.55, 0.92 - index * 0.08),
                "evidence_snippet": source_text[:240],
                "confidence": max(0.68, 0.95 - index * 0.05),
                "matched_by": "policy_source_declared" if topic_names else "policy_source_inferred",
            }
        )
        if len(candidates) >= limit:
            break
    return candidates


def linked_person_ids_for_project(conn: sqlite3.Connection, project_id: int) -> list[int]:
    rows = conn.execute(
        """
        SELECT DISTINCT person_id
        FROM (
            SELECT a.person_id AS person_id
            FROM appearances a
            WHERE a.project_id = ?
              AND a.person_id IS NOT NULL
            UNION
            SELECT pil.person_id AS person_id
            FROM person_identity_links pil
            JOIN person_mentions pm ON pm.id = pil.person_mention_id
            WHERE pm.project_id = ?
              AND pil.person_id IS NOT NULL
              AND pil.link_status IN ('reviewed_match', 'auto_matched')
        )
        """,
        (project_id, project_id),
    ).fetchall()
    return [int(row["person_id"]) for row in rows]


def collect_project_topic_candidates(
    conn: sqlite3.Connection,
    project_row: sqlite3.Row | dict[str, Any],
) -> list[dict[str, Any]]:
    row = dict(project_row)
    project_year = extract_year_from_date_text(row.get("published_at") or row.get("fetched_at") or "")
    resolved = resolve_public_appearance(conn, row, row)
    department_name = ""
    if resolved:
        department_name = clean_department_name(resolved.get("display_department_name", "") or "")
    if not department_name:
        department_name = clean_department_name(row.get("source_department_name") or "")

    base_candidates = derive_ranked_topic_candidates(
        row.get("title") or "",
        row.get("summary") or "",
        row.get("purpose") or "",
        department_name=department_name,
        extra_texts=[row.get("source_department_name") or ""],
        limit=4,
    )
    project_tokens = set(project_theme_tokens(row.get("title") or "", row.get("summary") or "", row.get("purpose") or ""))
    project_tokens |= department_theme_tokens(department_name)
    blob = normalize_text(" ".join(
        part for part in [
            row.get("title") or "",
            row.get("summary") or "",
            row.get("purpose") or "",
            department_name,
        ]
        if part
    ))

    linked: dict[int, dict[str, Any]] = {}
    for candidate in base_candidates:
        topic_id = upsert_policy_topic(
            conn,
            candidate["name"],
            topic_year=project_year,
            origin_type="project_inferred",
            description=row.get("summary") or row.get("purpose") or "",
            keywords=candidate["keywords"],
            priority_weight=min(0.25 + candidate["score"] * 0.03, 0.72),
        )
        if not topic_id:
            continue
        linked[topic_id] = {
            "topic_id": topic_id,
            "confidence": min(0.36 + candidate["score"] * 0.06, 0.82),
            "matched_by": "project_inferred_theme",
            "evidence_snippet": candidate["name"],
            "is_priority": 0,
        }

    if project_year:
        policy_topic_rows = conn.execute(
            """
            SELECT *
            FROM policy_topics
            WHERE origin_type = 'policy_source'
              AND (topic_year = 0 OR topic_year IN (?, ?, ?))
            ORDER BY priority_weight DESC, name ASC
            """,
            (project_year, max(project_year - 1, 0), project_year + 1),
        ).fetchall()
    else:
        policy_topic_rows = conn.execute(
            """
            SELECT *
            FROM policy_topics
            WHERE origin_type = 'policy_source'
            ORDER BY priority_weight DESC, name ASC
            """
        ).fetchall()

    for topic_row in policy_topic_rows:
        topic_keywords = topic_tokens_for_matching(
            topic_row["name"] or "",
            topic_row["description"] or "",
            decode_topic_keywords(topic_row["keywords_json"] or "[]"),
        )
        if not topic_keywords:
            continue
        topic_name = normalize_text(topic_row["name"] or "")
        exact_match = bool(topic_name and topic_name in blob)
        overlap = sorted(project_tokens & topic_keywords)
        fuzzy_match = any(token in blob for token in topic_keywords if len(token) >= 2)
        if not exact_match and not overlap and not fuzzy_match:
            continue

        confidence = 0.54
        if exact_match:
            confidence += 0.18
        if overlap:
            confidence += min(0.08 * len(overlap), 0.18)
        if fuzzy_match and not overlap:
            confidence += 0.06
        topic_year = int(topic_row["topic_year"] or 0)
        if topic_year and project_year:
            if topic_year == project_year:
                confidence += 0.08
            elif abs(topic_year - project_year) == 1:
                confidence += 0.03
        confidence += min(float(topic_row["priority_weight"] or 0) * 0.12, 0.1)
        topic_id = int(topic_row["id"])
        current = linked.get(topic_id)
        payload = {
            "topic_id": topic_id,
            "confidence": min(confidence, 0.98),
            "matched_by": "policy_source_exact" if exact_match else "policy_source_overlap",
            "evidence_snippet": " / ".join(overlap[:4]) if overlap else (topic_row["name"] or ""),
            "is_priority": 1,
        }
        if current is None or payload["confidence"] > current["confidence"]:
            linked[topic_id] = payload

    return sorted(
        linked.values(),
        key=lambda item: (item["is_priority"], item["confidence"], item["topic_id"]),
        reverse=True,
    )[:6]


def refresh_project_topic_links(
    conn: sqlite3.Connection,
    project_ids: list[int] | None = None,
) -> int:
    if project_ids is not None and not project_ids:
        return 0
    params: list[Any] = []
    query = """
        SELECT p.*, a.raw_department_name, a.raw_person_name, pe.person_key
        FROM projects p
        LEFT JOIN appearances a ON a.project_id = p.id
        LEFT JOIN people pe ON pe.id = a.person_id
        WHERE p.review_status != 'rejected'
    """
    if project_ids:
        placeholders = ", ".join("?" for _ in project_ids)
        query += f" AND p.id IN ({placeholders})"
        params.extend(project_ids)
    rows = conn.execute(query, params).fetchall()
    if project_ids:
        conn.executemany("DELETE FROM project_topic_links WHERE project_id = ?", [(project_id,) for project_id in project_ids])
    else:
        conn.execute("DELETE FROM project_topic_links")

    inserted = 0
    for row in rows:
        project_id = int(row["id"])
        for candidate in collect_project_topic_candidates(conn, row):
            conn.execute(
                """
                INSERT INTO project_topic_links (
                    project_id, topic_id, confidence, matched_by, evidence_snippet, is_priority
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(project_id, topic_id) DO UPDATE SET
                    confidence = excluded.confidence,
                    matched_by = excluded.matched_by,
                    evidence_snippet = excluded.evidence_snippet,
                    is_priority = excluded.is_priority,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (
                    project_id,
                    candidate["topic_id"],
                    float(candidate["confidence"]),
                    candidate["matched_by"],
                    candidate["evidence_snippet"],
                    int(candidate["is_priority"]),
                ),
            )
            inserted += 1
    return inserted


def refresh_person_topic_rollups(
    conn: sqlite3.Connection,
    person_ids: list[int] | None = None,
) -> int:
    if person_ids is not None and not person_ids:
        return 0
    if person_ids is not None:
        placeholders = ", ".join("?" for _ in person_ids)
        conn.execute(f"DELETE FROM person_topic_rollups WHERE person_id IN ({placeholders})", person_ids)
        filter_sql = f" AND lp.person_id IN ({placeholders})"
        params: list[Any] = person_ids[:]
    else:
        conn.execute("DELETE FROM person_topic_rollups")
        filter_sql = ""
        params = []

    rows = conn.execute(
        f"""
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
        SELECT
            lp.person_id,
            ptl.topic_id,
            pt.topic_year,
            COUNT(DISTINCT lp.project_id) AS project_count,
            COUNT(DISTINCT COALESCE(NULLIF(lp.raw_department_name, ''), '部署不明')) AS department_count,
            SUM(CASE WHEN ptl.is_priority = 1 THEN 1 ELSE 0 END) AS priority_project_count,
            AVG(ptl.confidence) AS avg_confidence,
            MIN(COALESCE(NULLIF(p.published_at, ''), substr(p.fetched_at, 1, 10))) AS first_seen_at,
            MAX(COALESCE(NULLIF(p.published_at, ''), substr(p.fetched_at, 1, 10))) AS last_seen_at
        FROM linked_projects lp
        JOIN projects p ON p.id = lp.project_id
        JOIN project_topic_links ptl ON ptl.project_id = lp.project_id
        JOIN policy_topics pt ON pt.id = ptl.topic_id
        WHERE 1 = 1
        {filter_sql}
        GROUP BY lp.person_id, ptl.topic_id, pt.topic_year
        """,
        params,
    ).fetchall()

    inserted = 0
    for row in rows:
        project_count = int(row["project_count"] or 0)
        department_count = int(row["department_count"] or 0)
        priority_project_count = int(row["priority_project_count"] or 0)
        avg_confidence = float(row["avg_confidence"] or 0)
        first_seen_at = row["first_seen_at"] or ""
        last_seen_at = row["last_seen_at"] or ""
        first_date = parse_iso_date(first_seen_at)
        last_date = parse_iso_date(last_seen_at)
        span_years = 0.0
        if first_date and last_date and last_date >= first_date:
            span_years = (last_date - first_date).days / 365.0
        involvement_score = project_count + department_count * 0.45 + avg_confidence * 0.8 + priority_project_count * 0.75
        continuity_score = min(4.0, span_years * 1.4 + (1.0 if project_count >= 2 else 0.0))
        visibility_score = min(4.5, avg_confidence * 1.8 + priority_project_count * 0.8 + project_count * 0.25)
        spotlight_score = involvement_score + continuity_score + visibility_score
        conn.execute(
            """
            INSERT INTO person_topic_rollups (
                person_id, topic_id, topic_year, project_count, department_count, priority_project_count,
                involvement_score, continuity_score, visibility_score, spotlight_score,
                first_seen_at, last_seen_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(row["person_id"]),
                int(row["topic_id"]),
                int(row["topic_year"] or 0),
                project_count,
                department_count,
                priority_project_count,
                involvement_score,
                continuity_score,
                visibility_score,
                spotlight_score,
                first_seen_at,
                last_seen_at,
            ),
        )
        inserted += 1
    return inserted


def cleanup_orphan_project_inferred_topics(conn: sqlite3.Connection) -> int:
    orphan_ids = [
        int(row["id"])
        for row in conn.execute(
            """
            SELECT pt.id
            FROM policy_topics pt
            LEFT JOIN project_topic_links ptl ON ptl.topic_id = pt.id
            LEFT JOIN topic_source_mentions tsm ON tsm.topic_id = pt.id
            LEFT JOIN person_topic_rollups ptr ON ptr.topic_id = pt.id
            WHERE pt.origin_type = 'project_inferred'
            GROUP BY pt.id
            HAVING COUNT(DISTINCT ptl.id) = 0
               AND COUNT(DISTINCT tsm.id) = 0
               AND COUNT(DISTINCT ptr.id) = 0
            """
        ).fetchall()
    ]
    if orphan_ids:
        conn.executemany("DELETE FROM policy_topics WHERE id = ?", [(topic_id,) for topic_id in orphan_ids])
    return len(orphan_ids)


def rebuild_policy_topics(db_path: Path | str = DB_PATH) -> dict[str, int]:
    init_db(db_path)
    conn = get_connection(db_path)
    try:
        conn.execute("DELETE FROM project_topic_links")
        conn.execute("DELETE FROM person_topic_rollups")
        conn.execute("DELETE FROM policy_topics WHERE origin_type = 'project_inferred'")
        links = refresh_project_topic_links(conn)
        rollups = refresh_person_topic_rollups(conn)
        removed = cleanup_orphan_project_inferred_topics(conn)
        conn.commit()
        return {
            "project_topic_links": links,
            "person_topic_rollups": rollups,
            "topics_removed": removed,
        }
    finally:
        conn.close()


def split_topic_name_field(value: str) -> list[str]:
    tokens = re.split(r"[|,、/／\n]+", value or "")
    names: list[str] = []
    for token in tokens:
        candidate = normalize_text(token or "").strip()
        if candidate and candidate not in names:
            names.append(candidate)
    return names


def normalized_topic_name_set(value: str | list[str] | set[str] | tuple[str, ...]) -> set[str]:
    if isinstance(value, str):
        names = split_topic_name_field(value)
    else:
        names = [normalize_text(str(item or "")).strip() for item in value]
    return {
        normalize_topic_name(name)
        for name in names
        if is_valid_topic_name(name)
    }


def topic_token_set_from_names(names: set[str] | list[str] | tuple[str, ...]) -> set[str]:
    tokens: set[str] = set()
    for name in names:
        normalized = normalize_text(name or "").strip()
        if not is_valid_topic_name(normalized):
            continue
        tokens |= topic_tokens_for_matching(normalized)
    return tokens


def build_topic_year_map(rows: list[sqlite3.Row] | list[dict[str, Any]]) -> dict[str, set[int]]:
    mapping: dict[str, set[int]] = {}
    for row in rows:
        payload = dict(row)
        topic_name = normalize_topic_name(payload.get("name") or payload.get("topic_name") or "")
        topic_year = int(payload.get("topic_year") or 0)
        if not topic_name or topic_year <= 0:
            continue
        mapping.setdefault(topic_name, set()).add(topic_year)
    return mapping


def has_recent_topic_year_match(
    mention_topic_names: set[str],
    mention_year: int,
    candidate_topic_years: dict[str, set[int]],
    tolerance: int = 1,
) -> bool:
    if mention_year <= 0:
        return False
    for name in mention_topic_names:
        for topic_year in candidate_topic_years.get(name, set()):
            if abs(topic_year - mention_year) <= tolerance:
                return True
    return False


def shared_topic_display_names(topic_field: str, candidate_topic_names: set[str]) -> list[str]:
    shared: list[str] = []
    seen: set[str] = set()
    for name in split_topic_name_field(topic_field or ""):
        normalized = normalize_topic_name(name)
        if normalized and normalized in candidate_topic_names and name not in seen:
            shared.append(name)
            seen.add(name)
    return shared


def identity_match_labels(metrics: dict[str, Any]) -> list[str]:
    labels: list[str] = []
    if metrics.get("contact_match"):
        labels.append("連絡先一致")
    if metrics.get("department_match"):
        labels.append("部署一致")
    elif metrics.get("top_unit_match"):
        labels.append("部局一致")
    if metrics.get("department_theme_match"):
        labels.append("部署テーマ一致")
    if metrics.get("project_theme_match"):
        labels.append("企画テーマ一致")
    if metrics.get("policy_topic_match"):
        labels.append("テーマ継続")
    if metrics.get("policy_topic_recent_match"):
        labels.append("同年度テーマ")
    if metrics.get("priority_policy_topic_match"):
        labels.append("重点テーマ")
    if metrics.get("transfer_match"):
        labels.append("異動部署一致")
    if metrics.get("transfer_recent_match"):
        labels.append("異動時期一致")
    return labels


def slot_match_labels(metrics: dict[str, Any], slot_row: sqlite3.Row | dict[str, Any]) -> list[str]:
    slot = dict(slot_row)
    labels: list[str] = []
    if metrics.get("exact_name_match"):
        labels.append("フルネーム一致")
    elif metrics.get("surname_bridge"):
        labels.append("姓ブリッジ")
    if metrics.get("department_exact"):
        labels.append("部署一致")
    elif metrics.get("department_overlap"):
        labels.append("部署近似")
    if metrics.get("department_theme_match"):
        labels.append("部署テーマ一致")
    if metrics.get("project_theme_match"):
        labels.append("企画テーマ一致")
    if metrics.get("policy_topic_match"):
        labels.append("テーマ継続")
    if metrics.get("policy_topic_recent_match"):
        labels.append("同年度テーマ")
    if metrics.get("priority_policy_topic_match"):
        labels.append("重点テーマ")
    if metrics.get("active_match"):
        labels.append("在籍時期一致")
    elif metrics.get("near_start") or metrics.get("near_end"):
        labels.append("異動時期近接")
    if slot.get("person_id"):
        labels.append("既存人物あり")
    return labels


def import_policy_sources_csv(csv_path: Path | str, db_path: Path | str = DB_PATH) -> dict[str, int]:
    init_db(db_path)
    counts = {
        "sources": 0,
        "topics": 0,
        "project_topic_links": 0,
        "person_topic_rollups": 0,
        "topics_removed": 0,
    }
    conn = get_connection(db_path)
    try:
        with Path(csv_path).open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            required = {"source_type", "title", "url", "published_at", "summary", "raw_text", "topic_names"}
            missing = required - set(reader.fieldnames or [])
            if missing:
                raise ValueError(f"policy CSVに必要な列がありません: {', '.join(sorted(missing))}")

            for row in reader:
                source_id = upsert_policy_source(
                    conn,
                    (row.get("source_type") or "manual").strip() or "manual",
                    (row.get("title") or "").strip(),
                    (row.get("url") or "").strip(),
                    (row.get("published_at") or "").strip(),
                    (row.get("summary") or "").strip(),
                    (row.get("raw_text") or "").strip(),
                )
                topic_year = extract_year_from_date_text(row.get("published_at") or row.get("title") or "")
                topic_candidates = derive_policy_source_topic_candidates(
                    row.get("title") or "",
                    row.get("summary") or "",
                    row.get("raw_text") or "",
                    topic_names=split_topic_name_field(row.get("topic_names") or ""),
                    topic_year=topic_year,
                )
                counts["sources"] += 1
                counts["topics"] += replace_policy_source_topics(conn, source_id, topic_candidates)

        counts["project_topic_links"] = refresh_project_topic_links(conn)
        counts["person_topic_rollups"] = refresh_person_topic_rollups(conn)
        counts["topics_removed"] = cleanup_orphan_project_inferred_topics(conn)
        conn.commit()
        return counts
    finally:
        conn.close()


def fetch_policy_topic_index(
    limit: int = 80,
    db_path: Path | str = DB_PATH,
) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        rows = conn.execute(
            """
            WITH project_counts AS (
                SELECT topic_id, COUNT(DISTINCT project_id) AS project_count
                FROM project_topic_links
                GROUP BY topic_id
            ),
            person_counts AS (
                SELECT topic_id,
                       COUNT(DISTINCT person_id) AS person_count,
                       COALESCE(SUM(spotlight_score), 0) AS total_spotlight
                FROM person_topic_rollups
                GROUP BY topic_id
            )
            SELECT
                pt.id,
                pt.name,
                pt.topic_year,
                pt.origin_type,
                pt.description,
                pt.priority_weight,
                COALESCE(pc.project_count, 0) AS project_count,
                COALESCE(rc.person_count, 0) AS person_count,
                COALESCE(rc.total_spotlight, 0) AS total_spotlight
            FROM policy_topics pt
            LEFT JOIN project_counts pc ON pc.topic_id = pt.id
            LEFT JOIN person_counts rc ON rc.topic_id = pt.id
            WHERE COALESCE(pc.project_count, 0) > 0 OR COALESCE(rc.person_count, 0) > 0
            ORDER BY total_spotlight DESC, pt.priority_weight DESC, project_count DESC, pt.name ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
        payload: list[dict[str, Any]] = []
        for row in rows:
            people = conn.execute(
                """
                SELECT pe.person_key, pe.display_name, ptr.spotlight_score
                FROM person_topic_rollups ptr
                JOIN people pe ON pe.id = ptr.person_id
                WHERE ptr.topic_id = ?
                ORDER BY ptr.spotlight_score DESC, ptr.project_count DESC, pe.display_name ASC
                LIMIT 4
                """,
                (row["id"],),
            ).fetchall()
            projects = conn.execute(
                """
                SELECT p.id, p.title, p.published_at, ptl.is_priority
                FROM project_topic_links ptl
                JOIN projects p ON p.id = ptl.project_id
                WHERE ptl.topic_id = ?
                ORDER BY ptl.is_priority DESC, ptl.confidence DESC,
                         COALESCE(NULLIF(p.published_at, ''), substr(p.fetched_at, 1, 10)) DESC
                LIMIT 4
                """,
                (row["id"],),
            ).fetchall()
            payload.append(
                {
                    **dict(row),
                    "people": [dict(item) for item in people],
                    "projects": [dict(item) for item in projects],
                }
            )
        return payload
    finally:
        conn.close()


def fetch_person_identity_context(conn: sqlite3.Connection, person_id: int) -> dict[str, Any]:
    mention_rows = conn.execute(
        """
        SELECT pm.department_id, pm.raw_department_name, pm.contact_email, pm.contact_phone,
               p.published_at AS project_published_at, p.fetched_at AS project_fetched_at,
               p.title AS project_title, p.summary AS project_summary, p.purpose AS project_purpose
        FROM person_identity_links pil
        JOIN person_mentions pm ON pm.id = pil.person_mention_id
        JOIN projects p ON p.id = pm.project_id
        WHERE pil.person_id = ?
          AND pil.link_status IN ('reviewed_match', 'auto_matched')
        """,
        (person_id,),
    ).fetchall()
    appearance_rows = conn.execute(
        """
        SELECT a.department_id, a.raw_department_name, a.contact_email, a.contact_phone,
               p.published_at AS project_published_at, p.fetched_at AS project_fetched_at,
               p.title AS project_title, p.summary AS project_summary, p.purpose AS project_purpose
        FROM appearances a
        JOIN projects p ON p.id = a.project_id
        WHERE a.person_id = ?
        """,
        (person_id,),
    ).fetchall()
    transfer_rows = conn.execute(
        """
        SELECT te.effective_date, te.from_department_id, te.to_department_id,
               te.from_department_raw, te.to_department_raw
        FROM transfer_identity_links til
        JOIN transfer_events te ON te.id = til.transfer_event_id
        WHERE til.person_id = ?
          AND til.link_status IN ('reviewed_match', 'auto_matched')
        ORDER BY COALESCE(NULLIF(te.effective_date, ''), '0000-00-00') DESC, te.id DESC
        """,
        (person_id,),
    ).fetchall()
    topic_rows = conn.execute(
        """
        SELECT
            pt.name,
            ptr.topic_year,
            ptr.priority_project_count,
            ptr.spotlight_score
        FROM person_topic_rollups ptr
        JOIN policy_topics pt ON pt.id = ptr.topic_id
        WHERE ptr.person_id = ?
        ORDER BY ptr.spotlight_score DESC, ptr.priority_project_count DESC, pt.name ASC
        """,
        (person_id,),
    ).fetchall()

    emails: set[str] = set()
    phones: set[str] = set()
    departments: set[str] = set()
    department_top_units: set[str] = set()
    department_theme_profiles: set[str] = set()
    project_theme_profiles: set[str] = set()
    policy_topic_names: set[str] = set()
    priority_policy_topic_names: set[str] = set()
    policy_topic_tokens: set[str] = set()
    department_ids: set[int] = set()
    activity_dates: list[date] = []

    for row in [*mention_rows, *appearance_rows]:
        if row["contact_email"]:
            emails.add(normalize_contact_email(row["contact_email"]))
        if row["contact_phone"]:
            phones.add(normalize_contact_phone(row["contact_phone"]))
        if row["department_id"]:
            department_ids.add(int(row["department_id"]))
        department = normalize_department_for_match(row["raw_department_name"] or "")
        if department:
            departments.add(department)
        for token in department_theme_tokens(row["raw_department_name"] or ""):
            department_theme_profiles.add(token)
        for token in project_theme_tokens(
            row["project_title"] or "",
            row["project_summary"] or "",
            row["project_purpose"] or "",
        ):
            project_theme_profiles.add(token)
        hierarchy = resolve_department_hierarchy(row["raw_department_name"] or "")
        if hierarchy["top_unit"] and hierarchy["top_unit"] != "未分類":
            department_top_units.add(hierarchy["top_unit"])
        activity_date = coalesce_row_date(row["project_published_at"] or "", row["project_fetched_at"] or "")
        if activity_date:
            activity_dates.append(activity_date)

    for row in transfer_rows:
        for token in department_theme_tokens(row["to_department_raw"] or ""):
            department_theme_profiles.add(token)
        for token in department_theme_tokens(row["from_department_raw"] or ""):
            department_theme_profiles.add(token)
    for row in topic_rows:
        topic_name = normalize_text(row["name"] or "").strip()
        if not is_valid_topic_name(topic_name):
            continue
        normalized = normalize_topic_name(topic_name)
        policy_topic_names.add(normalized)
        policy_topic_tokens |= topic_tokens_for_matching(topic_name)
        if int(row["priority_project_count"] or 0) > 0:
            priority_policy_topic_names.add(normalized)

    project_count = conn.execute(
        """
        SELECT COUNT(DISTINCT project_id)
        FROM (
            SELECT a.project_id
            FROM appearances a
            WHERE a.person_id = ?
            UNION
            SELECT pm.project_id
            FROM person_identity_links pil
            JOIN person_mentions pm ON pm.id = pil.person_mention_id
            WHERE pil.person_id = ?
              AND pil.link_status IN ('reviewed_match', 'auto_matched')
        )
        """,
        (person_id, person_id),
    ).fetchone()[0]

    return {
        "emails": emails,
        "phones": phones,
        "departments": departments,
        "department_top_units": department_top_units,
        "department_theme_profiles": department_theme_profiles,
        "project_theme_profiles": project_theme_profiles,
        "policy_topic_names": policy_topic_names,
        "priority_policy_topic_names": priority_policy_topic_names,
        "policy_topic_tokens": policy_topic_tokens,
        "policy_topic_years": build_topic_year_map(topic_rows),
        "department_ids": department_ids,
        "activity_dates": activity_dates,
        "transfer_events": [dict(row) for row in transfer_rows],
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
    mention_department_id = int(mention["department_id"]) if mention.get("department_id") else None
    mention_department_raw = mention.get("raw_department_name", "") or ""
    mention_top_unit = resolve_department_hierarchy(mention_department_raw).get("top_unit", "")
    mention_theme_profile = department_theme_tokens(mention_department_raw)
    mention_project_profile = project_theme_tokens(
        mention.get("project_title", "") or "",
        mention.get("project_summary", "") or "",
        mention.get("project_purpose", "") or "",
    )
    mention_policy_topic_names = normalized_topic_name_set(mention.get("policy_topic_names") or "")
    mention_priority_topic_names = normalized_topic_name_set(mention.get("priority_policy_topic_names") or "")
    mention_policy_topic_tokens = topic_token_set_from_names(mention_policy_topic_names)
    mention_year = extract_year_from_date_text(
        mention.get("project_published_at", "") or mention.get("published_at", "") or mention.get("project_fetched_at", "") or ""
    )
    mention_date = coalesce_row_date(
        mention.get("project_published_at", "") or "",
        mention.get("published_at", "") or "",
        mention.get("project_fetched_at", "") or "",
        mention.get("fetched_at", "") or "",
    )

    contact_match = bool(
        (mention_email and mention_email in context["emails"])
        or (mention_phone and mention_phone in context["phones"])
    )
    department_exact = bool(
        (mention_department_id and mention_department_id in context["department_ids"])
        or (mention_department and mention_department in context["departments"])
    )
    department_overlap = bool(
        mention_department
        and not department_exact
        and any(
            mention_department in department_name or department_name in mention_department
            for department_name in context["departments"]
        )
    )
    top_unit_match = bool(
        mention_top_unit
        and mention_top_unit != "未分類"
        and mention_top_unit in context["department_top_units"]
    )
    department_theme_match = bool(
        mention_theme_profile
        and context.get("department_theme_profiles")
        and bool(mention_theme_profile & context["department_theme_profiles"])
    )
    department_theme_conflict = bool(
        mention_theme_profile
        and context.get("department_theme_profiles")
        and not department_theme_match
        and not department_exact
        and not department_overlap
    )
    project_theme_match = bool(
        mention_project_profile
        and context.get("project_theme_profiles")
        and bool(mention_project_profile & context["project_theme_profiles"])
    )
    project_theme_conflict = bool(
        mention_project_profile
        and context.get("project_theme_profiles")
        and not project_theme_match
        and not department_exact
        and not department_overlap
    )
    policy_topic_match = bool(
        mention_policy_topic_names
        and (
            bool(mention_policy_topic_names & context.get("policy_topic_names", set()))
            or bool(mention_policy_topic_tokens & context.get("policy_topic_tokens", set()))
        )
    )
    priority_policy_topic_match = bool(
        mention_priority_topic_names
        and context.get("priority_policy_topic_names")
        and bool(mention_priority_topic_names & context["priority_policy_topic_names"])
    )
    policy_topic_recent_match = has_recent_topic_year_match(
        mention_policy_topic_names,
        mention_year,
        context.get("policy_topic_years", {}),
    )
    policy_topic_conflict = bool(
        mention_policy_topic_names
        and context.get("policy_topic_names")
        and not policy_topic_match
        and not department_exact
        and not department_overlap
    )
    transfer_to_match = bool(
        mention_department_id
        and any(event.get("to_department_id") == mention_department_id for event in context["transfer_events"])
    )
    transfer_from_only = bool(
        mention_department_id
        and not transfer_to_match
        and any(event.get("from_department_id") == mention_department_id for event in context["transfer_events"])
    )
    transfer_recent_match = False
    if mention_department_id and mention_date:
        for event in context["transfer_events"]:
            event_date = parse_iso_date(event.get("effective_date") or "")
            if not event_date or event.get("to_department_id") != mention_department_id:
                continue
            if abs((mention_date - event_date).days) <= 90:
                transfer_recent_match = True
                break
    activity_recent_match = bool(
        mention_date
        and any(abs((mention_date - activity_date).days) <= 180 for activity_date in context["activity_dates"])
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
    elif top_unit_match:
        score += 0.03
    if department_theme_match:
        score += 0.08
    elif department_theme_conflict and mention.get("name_quality") == "surname_only":
        score -= 0.22
    elif department_theme_conflict and not top_unit_match:
        score -= 0.10
    if project_theme_match:
        score += 0.07
    elif project_theme_conflict and mention.get("name_quality") == "surname_only":
        score -= 0.12
    if policy_topic_match:
        score += 0.12
    elif policy_topic_conflict and mention.get("name_quality") == "surname_only":
        score -= 0.18
    elif policy_topic_conflict:
        score -= 0.08
    if policy_topic_recent_match:
        score += 0.08
    if priority_policy_topic_match:
        score += 0.05
    if transfer_to_match:
        score += 0.20
    if transfer_recent_match:
        score += 0.15
    elif transfer_from_only:
        score -= 0.15
    if activity_recent_match:
        score += 0.10
    if context["project_count"] >= 2:
        score += min(context["project_count"] * 0.02, 0.08)

    return {
        "score": round(min(score, 0.98), 2),
        "contact_match": contact_match,
        "department_match": department_exact or department_overlap,
        "top_unit_match": top_unit_match,
        "department_theme_match": department_theme_match,
        "department_theme_conflict": department_theme_conflict,
        "project_theme_match": project_theme_match,
        "project_theme_conflict": project_theme_conflict,
        "policy_topic_match": policy_topic_match,
        "policy_topic_recent_match": policy_topic_recent_match,
        "priority_policy_topic_match": priority_policy_topic_match,
        "policy_topic_conflict": policy_topic_conflict,
        "transfer_match": transfer_to_match,
        "transfer_recent_match": transfer_recent_match,
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
        "する事業者を選定する",
        "事業者を選定する",
        "を選定する",
        "契約結果について",
        "契約結果",
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
    normalized = re.sub(r"事業者を選定.*$", " ", normalized)
    normalized = re.sub(r"契約結果.*$", " ", normalized)
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


def source_department_fallback(project_row: sqlite3.Row | dict[str, Any]) -> str:
    department = clean_department_name(
        project_row["source_department_name"]
        if isinstance(project_row, sqlite3.Row)
        else project_row.get("source_department_name", "")
    )
    return department if is_valid_department_name(department) else ""


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


def infer_person_from_employee_slot(
    conn: sqlite3.Connection,
    project_row: sqlite3.Row | dict[str, Any],
    appearance_payload: dict[str, Any],
) -> dict[str, Any] | None:
    target_department_name = clean_department_name(
        appearance_payload.get("raw_department_name", "")
        or (
            project_row["source_department_name"]
            if isinstance(project_row, sqlite3.Row)
            else project_row.get("source_department_name", "")
        )
    )
    if not target_department_name or not is_valid_department_name(target_department_name):
        return None

    target_date = coalesce_row_date(
        project_row["published_at"] if isinstance(project_row, sqlite3.Row) else project_row.get("published_at", ""),
        project_row["fetched_at"] if isinstance(project_row, sqlite3.Row) else project_row.get("fetched_at", ""),
    )
    if not target_date:
        return None

    target_department_id = appearance_payload.get("department_id") or resolve_department_reference_id(conn, target_department_name)
    target_department_norm = normalize_department_for_match(target_department_name)
    target_date_iso = target_date.isoformat()

    rows = conn.execute(
        """
        SELECT
            es.id AS employee_slot_id,
            es.department_id,
            es.display_name,
            es.name_quality,
            es.raw_department_name,
            es.active_from,
            es.active_to,
            es.person_id,
            es.slot_confidence,
            d.name AS department_name,
            pe.person_key,
            pe.display_name AS linked_person_name
        FROM employee_slots es
        LEFT JOIN departments d ON d.id = es.department_id
        LEFT JOIN people pe ON pe.id = es.person_id
        WHERE ? >= COALESCE(NULLIF(es.active_from, ''), '0000-01-01')
          AND ? <= COALESCE(NULLIF(es.active_to, ''), '9999-12-31')
        ORDER BY COALESCE(NULLIF(es.active_from, ''), '0000-01-01') DESC, es.id DESC
        """,
        (target_date_iso, target_date_iso),
    ).fetchall()

    if not rows:
        return None

    candidates: list[dict[str, Any]] = []
    for row in rows:
        department_name = row["department_name"] or row["raw_department_name"] or ""
        if not department_name:
            continue
        exact_department_id = bool(target_department_id and row["department_id"] == target_department_id)
        exact_department_norm = bool(
            target_department_norm
            and normalize_department_for_match(department_name) == target_department_norm
        )
        department_overlap = department_match_overlap(target_department_name, department_name)
        if not (exact_department_id or exact_department_norm or department_overlap):
            continue

        person_name = clean_person_name(row["linked_person_name"] or row["display_name"] or "")
        if not is_valid_person_name(person_name) or " " not in person_name:
            continue

        candidates.append(
            {
                "employee_slot_id": row["employee_slot_id"],
                "person_id": row["person_id"],
                "person_name": person_name,
                "person_key": row["person_key"] or build_person_key(person_name),
                "department_name": department_name,
                "exact_department_id": exact_department_id,
                "exact_department_norm": exact_department_norm,
                "department_overlap": department_overlap,
                "slot_confidence": float(row["slot_confidence"] or 0),
            }
        )

    if not candidates:
        return None

    for predicate in (
        lambda item: item["exact_department_id"],
        lambda item: item["exact_department_norm"],
        lambda item: item["department_overlap"],
    ):
        matched = [item for item in candidates if predicate(item)]
        if len(matched) == 1:
            return matched[0]

    return None


def resolve_public_appearance(
    conn: sqlite3.Connection,
    project_row: sqlite3.Row | dict[str, Any],
    appearance_row: sqlite3.Row | dict[str, Any] | None,
) -> dict[str, Any] | None:
    project_id = int(project_row["id"] if isinstance(project_row, sqlite3.Row) else project_row["id"])
    resolved = build_public_appearance_payload(appearance_row)
    source_department = source_department_fallback(project_row)

    if resolved["department_status"] == "missing" and source_department:
        resolved["display_department_name"] = source_department
        resolved["department_status"] = "inherited_source_list"
        resolved["department_status_label"] = "継承: 一覧ページ"

    if resolved["person_status"] == "missing":
        contact_candidate = infer_person_from_contact(conn, resolved, exclude_project_id=project_id)
        if contact_candidate:
            resolved["display_person_name"] = contact_candidate["person_name"]
            resolved["display_person_key"] = contact_candidate["person_key"]
            resolved["person_status"] = "inferred_same_contact"
            resolved["person_status_label"] = "推定: 同じ連絡先"

    if resolved["person_status"] == "missing":
        slot_candidate = infer_person_from_employee_slot(conn, project_row, resolved)
        if slot_candidate:
            resolved["display_person_name"] = slot_candidate["person_name"]
            resolved["display_person_key"] = slot_candidate["person_key"]
            resolved["person_status"] = "inferred_transfer_slot"
            resolved["person_status_label"] = "推定: 異動履歴"

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
            title, url, source_type, source_list_url, source_department_name, summary, purpose, budget,
            application_deadline, submission_deadline, published_at,
            raw_text, html_text, pdf_urls_json, zip_urls_json, fetched_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(url) DO UPDATE SET
            title = excluded.title,
            source_type = excluded.source_type,
            source_list_url = excluded.source_list_url,
            source_department_name = excluded.source_department_name,
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
            record.source_list_url,
            record.source_department_name,
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
            p.title AS project_title,
            p.summary AS project_summary,
            p.purpose AS project_purpose,
            p.published_at AS project_published_at,
            p.fetched_at AS project_fetched_at,
            pil.id AS link_id,
            pil.link_status AS existing_link_status
        FROM person_mentions pm
        JOIN projects p ON p.id = pm.project_id
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


def derive_person_mentions_for_project(
    conn: sqlite3.Connection,
    project_id: int,
    record: ProjectRecord,
) -> list[PersonMention]:
    if record.person_mentions:
        source_department = source_department_fallback({"source_department_name": record.source_department_name})
        if not source_department:
            return record.person_mentions
        enriched_mentions: list[PersonMention] = []
        for mention in record.person_mentions:
            if mention.department_name:
                enriched_mentions.append(mention)
                continue
            enriched_mentions.append(
                PersonMention(
                    department_name=source_department,
                    person_name=mention.person_name,
                    person_key=mention.person_key,
                    person_role=mention.person_role,
                    contact_email=mention.contact_email,
                    contact_phone=mention.contact_phone,
                    extracted_section=mention.extracted_section,
                    name_quality=mention.name_quality,
                    source_confidence=mention.source_confidence,
                )
            )
        return enriched_mentions

    source_department = source_department_fallback({"source_department_name": record.source_department_name})
    related = infer_related_project_appearance(
        conn,
        {
            "id": project_id,
            "title": record.title,
            "source_type": record.source_type,
            "source_department_name": record.source_department_name,
        },
        exclude_project_id=project_id,
    )
    if related:
        related_department = clean_department_name(related.get("raw_department_name", "") or "")
        department = related_department if is_valid_department_name(related_department) else source_department
        related_person = related.get("raw_person_name", "") or ""
        snippet = f"Inherited from related project: {related.get('title', '')}"
        fallback_text = " ".join(
            part for part in [related.get("contact_email", ""), related.get("contact_phone", "")] if part
        )
        return build_person_mentions(
            department,
            [related_person] if is_valid_person_name(related_person) else [],
            related.get("role", "") or "contact",
            snippet,
            0.25 if is_valid_person_name(related_person) else 0.12,
            fallback_text,
        )

    if source_department:
        return [
            PersonMention(
                department_name=source_department,
                person_name="",
                person_key="",
                person_role="contact",
                contact_email="",
                contact_phone="",
                extracted_section=f"Source list fallback: {record.source_list_url}",
                name_quality="unknown",
                source_confidence=0.05,
            )
        ]

    return []


def save_project_record(record: ProjectRecord, db_path: Path | str = DB_PATH) -> int:
    conn = get_connection(db_path)
    try:
        project_id = upsert_project(conn, record)
        effective_mentions = derive_person_mentions_for_project(conn, project_id, record)
        effective_record = ProjectRecord(
            title=record.title,
            url=record.url,
            source_type=record.source_type,
            summary=record.summary,
            purpose=record.purpose,
            budget=record.budget,
            application_deadline=record.application_deadline,
            submission_deadline=record.submission_deadline,
            published_at=record.published_at,
            raw_text=record.raw_text,
            html_text=record.html_text,
            pdf_urls=record.pdf_urls,
            zip_urls=record.zip_urls,
            person_mentions=effective_mentions,
            fetched_at=record.fetched_at,
            source_list_url=record.source_list_url,
            source_department_name=record.source_department_name,
        )
        replace_project_person_mentions(conn, project_id, effective_record)
        replace_project_appearance(conn, project_id, effective_record)
        refresh_transfer_links_for_project(conn, project_id)
        refresh_slot_candidates(conn, project_id)
        refresh_project_topic_links(conn, [project_id])
        refresh_person_topic_rollups(conn, linked_person_ids_for_project(conn, project_id))
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
                SELECT p.*, a.raw_department_name, a.department_id as resolved_department_id, a.raw_person_name, pe.person_key
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
                SELECT p.*, a.raw_department_name, a.department_id as resolved_department_id, a.raw_person_name, pe.person_key
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
            item["department_id"] = item.get("resolved_department_id")
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


def fetch_project_detail(project_id: int, db_path: Path | str = DB_PATH) -> dict[str, Any] | None:
    import json

    conn = get_connection(db_path)
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
        payload["transfer_candidates"] = fetch_transfer_candidates_for_project(conn, row, appearance)
        payload["topic_links"] = [
            dict(item)
            for item in conn.execute(
                """
                SELECT
                    pt.id AS topic_id,
                    pt.name,
                    pt.topic_year,
                    pt.origin_type,
                    pt.description,
                    ptl.confidence,
                    ptl.matched_by,
                    ptl.is_priority,
                    COUNT(DISTINCT tsm.policy_source_id) AS source_count
                FROM project_topic_links ptl
                JOIN policy_topics pt ON pt.id = ptl.topic_id
                LEFT JOIN topic_source_mentions tsm ON tsm.topic_id = pt.id
                WHERE ptl.project_id = ?
                GROUP BY pt.id, pt.name, pt.topic_year, pt.origin_type, pt.description,
                         ptl.confidence, ptl.matched_by, ptl.is_priority
                ORDER BY ptl.is_priority DESC, ptl.confidence DESC, pt.name ASC
                LIMIT 8
                """,
                (project_id,),
            ).fetchall()
        ]
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
        filtered = [row for row in rows if is_valid_roster_name(row["display_name"])]
        return filtered[:limit]
    finally:
        conn.close()


def fetch_staff_roster(
    limit_confirmed: int = 300,
    limit_slot_roster: int = 500,
    limit_transfer_candidates: int = 500,
    limit_mention_candidates: int = 300,
    db_path: Path | str = DB_PATH,
) -> dict[str, Any]:
    conn = get_connection(db_path)
    try:
        confirmed_rows = conn.execute(
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
            ),
            full_name_evidence AS (
                SELECT DISTINCT pil.person_id
                FROM person_identity_links pil
                JOIN person_mentions pm ON pm.id = pil.person_mention_id
                WHERE pil.person_id IS NOT NULL
                  AND pil.link_status IN ('reviewed_match', 'auto_matched')
                  AND pm.name_quality = 'full_name'
                UNION
                SELECT DISTINCT til.person_id
                FROM transfer_identity_links til
                JOIN transfer_events te ON te.id = til.transfer_event_id
                WHERE til.person_id IS NOT NULL
                  AND til.link_status IN ('reviewed_match', 'auto_matched')
                  AND te.name_quality = 'full_name'
            )
            SELECT pe.id, pe.person_key, pe.display_name,
                   COUNT(DISTINCT lp.project_id) AS project_count,
                   CASE WHEN f.person_id IS NOT NULL THEN 1 ELSE 0 END AS has_full_name_evidence
            FROM people pe
            LEFT JOIN linked_projects lp ON lp.person_id = pe.id
            LEFT JOIN full_name_evidence f ON f.person_id = pe.id
            GROUP BY pe.id
            HAVING project_count > 0
            ORDER BY project_count DESC, pe.display_name ASC
            LIMIT ?
            """,
            (limit_confirmed,),
        ).fetchall()

        confirmed_people: list[dict[str, Any]] = []
        weak_people_candidates: list[dict[str, Any]] = []
        for row in confirmed_rows:
            if not is_valid_roster_name(row["display_name"]):
                continue
            transfer_summary = conn.execute(
                """
                SELECT
                    te.effective_date,
                    te.from_department_raw,
                    te.to_department_raw,
                    COUNT(*) OVER() AS transfer_count
                FROM transfer_identity_links til
                JOIN transfer_events te ON te.id = til.transfer_event_id
                WHERE til.person_id = ?
                  AND til.link_status IN ('reviewed_match', 'auto_matched')
                ORDER BY COALESCE(NULLIF(te.effective_date, ''), '0000-00-00') DESC, te.id DESC
                LIMIT 1
                """,
                (row["id"],),
            ).fetchone()
            latest_department = conn.execute(
                """
                WITH linked_projects AS (
                    SELECT DISTINCT a.project_id, a.raw_department_name
                    FROM appearances a
                    WHERE a.person_id = ?
                    UNION
                    SELECT DISTINCT pm.project_id, pm.raw_department_name
                    FROM person_identity_links pil
                    JOIN person_mentions pm ON pm.id = pil.person_mention_id
                    WHERE pil.person_id = ?
                      AND pil.link_status IN ('reviewed_match', 'auto_matched')
                )
                SELECT lp.raw_department_name
                FROM linked_projects lp
                JOIN projects p ON p.id = lp.project_id
                ORDER BY COALESCE(NULLIF(p.published_at, ''), substr(p.fetched_at, 1, 10)) DESC, p.id DESC
                LIMIT 1
                """,
                (row["id"], row["id"]),
            ).fetchone()
            latest_project = conn.execute(
                """
                WITH linked_projects AS (
                    SELECT DISTINCT a.project_id
                    FROM appearances a
                    WHERE a.person_id = ?
                    UNION
                    SELECT DISTINCT pm.project_id
                    FROM person_identity_links pil
                    JOIN person_mentions pm ON pm.id = pil.person_mention_id
                    WHERE pil.person_id = ?
                      AND pil.link_status IN ('reviewed_match', 'auto_matched')
                )
                SELECT p.id, p.title,
                       COALESCE(NULLIF(p.published_at, ''), substr(p.fetched_at, 1, 10)) AS observed_at
                FROM linked_projects lp
                JOIN projects p ON p.id = lp.project_id
                ORDER BY observed_at DESC, p.id DESC
                LIMIT 1
                """,
                (row["id"], row["id"]),
            ).fetchone()

            current_department = ""
            previous_department = ""
            transfer_count = 0
            latest_transfer_date = ""
            if transfer_summary:
                current_department = (transfer_summary["to_department_raw"] or "").strip()
                previous_department = (transfer_summary["from_department_raw"] or "").strip()
                transfer_count = int(transfer_summary["transfer_count"] or 0)
                latest_transfer_date = transfer_summary["effective_date"] or ""
            if not current_department and latest_department:
                current_department = (latest_department["raw_department_name"] or "").strip()

            payload = {
                "person_key": row["person_key"],
                "display_name": row["display_name"],
                "project_count": int(row["project_count"] or 0),
                "transfer_count": transfer_count,
                "current_department": current_department,
                "previous_department": previous_department,
                "latest_transfer_date": latest_transfer_date,
                "latest_project_id": int(latest_project["id"]) if latest_project else None,
                "latest_project_title": latest_project["title"] if latest_project else "",
                "latest_observed_at": latest_project["observed_at"] if latest_project else "",
            }
            if bool(row["has_full_name_evidence"]):
                confirmed_people.append(payload)
            else:
                weak_people_candidates.append(payload)

        roster_as_of = date.today().isoformat()
        active_slot_rows = conn.execute(
            """
            SELECT
                es.id,
                es.display_name,
                es.name_quality,
                es.raw_department_name,
                es.active_from,
                es.active_to,
                es.slot_confidence,
                prev.raw_department_name AS previous_department,
                pe.person_key
            FROM employee_slots es
            LEFT JOIN employee_slots prev ON prev.id = es.previous_slot_id
            LEFT JOIN people pe ON pe.id = es.person_id
            WHERE IFNULL(TRIM(es.display_name), '') != ''
              AND es.name_quality = 'full_name'
              AND ? >= COALESCE(NULLIF(es.active_from, ''), '0000-00-00')
              AND ? <= COALESCE(NULLIF(es.active_to, ''), '9999-12-31')
            ORDER BY COALESCE(NULLIF(es.active_from, ''), '0000-00-00') DESC, es.id DESC
            LIMIT ?
            """,
            (roster_as_of, roster_as_of, limit_slot_roster),
        ).fetchall()

        slot_roster: list[dict[str, Any]] = []
        for row in active_slot_rows:
            display_name = (row["display_name"] or "").strip()
            current_department = (row["raw_department_name"] or "").strip()
            if not is_valid_roster_name(display_name):
                continue
            if not current_department:
                continue
            slot_roster.append(
                {
                    "employee_slot_id": int(row["id"]),
                    "display_name": display_name,
                    "person_key": row["person_key"] or "",
                    "current_department": current_department,
                    "previous_department": (row["previous_department"] or "").strip(),
                    "active_from": row["active_from"] or "",
                    "active_to": row["active_to"] or "",
                    "slot_confidence": float(row["slot_confidence"] or 0),
                }
            )

        transfer_candidate_rows = conn.execute(
            """
            SELECT
                te.id,
                te.normalized_person_name,
                te.raw_person_name,
                te.name_quality,
                te.effective_date,
                te.from_department_raw,
                te.to_department_raw,
                ts.title AS source_title,
                ts.url AS source_url,
                til.person_id,
                til.confidence,
                til.notes,
                pe.person_key,
                pe.display_name AS candidate_person_name
            FROM transfer_events te
            JOIN transfer_sources ts ON ts.id = te.transfer_source_id
            LEFT JOIN transfer_identity_links til ON til.transfer_event_id = te.id
            LEFT JOIN people pe ON pe.id = til.person_id
            WHERE ts.source_type IN ('newspaper_transfer_list', 'official_transfer_list')
              AND (
                    til.id IS NULL
                    OR til.link_status = 'review_pending'
                  )
            ORDER BY COALESCE(NULLIF(te.effective_date, ''), NULLIF(ts.published_at, ''), '0000-00-00') DESC,
                     te.id DESC
            """
        ).fetchall()

        transfer_candidates_by_name: dict[str, dict[str, Any]] = {}
        for row in transfer_candidate_rows:
            key = (row["normalized_person_name"] or "").strip()
            if not key:
                continue
            if not is_valid_roster_name(row["raw_person_name"] or ""):
                continue
            current = transfer_candidates_by_name.setdefault(
                key,
                {
                    "display_name": row["raw_person_name"],
                    "normalized_name": key,
                    "name_quality": row["name_quality"] or "unknown",
                    "latest_effective_date": row["effective_date"] or "",
                    "current_department": (row["to_department_raw"] or "").strip(),
                    "previous_department": (row["from_department_raw"] or "").strip(),
                    "transfer_count": 0,
                    "source_title": row["source_title"] or "",
                    "source_url": row["source_url"] or "",
                    "candidate_person_name": row["candidate_person_name"] or "",
                    "candidate_person_key": row["person_key"] or "",
                    "candidate_confidence": float(row["confidence"] or 0),
                    "candidate_notes": row["notes"] or "",
                },
            )
            current["transfer_count"] += 1
            if (row["raw_person_name"] or "") and len((row["raw_person_name"] or "").strip()) > len(current["display_name"] or ""):
                current["display_name"] = row["raw_person_name"]
            if (row["effective_date"] or "") >= current["latest_effective_date"]:
                current["latest_effective_date"] = row["effective_date"] or ""
                current["current_department"] = (row["to_department_raw"] or "").strip()
                current["previous_department"] = (row["from_department_raw"] or "").strip()
                current["source_title"] = row["source_title"] or ""
                current["source_url"] = row["source_url"] or ""
            if float(row["confidence"] or 0) >= current["candidate_confidence"]:
                current["candidate_confidence"] = float(row["confidence"] or 0)
                current["candidate_person_name"] = row["candidate_person_name"] or ""
                current["candidate_person_key"] = row["person_key"] or ""
                current["candidate_notes"] = row["notes"] or ""

        transfer_candidates = sorted(
            transfer_candidates_by_name.values(),
            key=lambda item: (
                item["candidate_confidence"],
                item["transfer_count"],
                item["latest_effective_date"],
                item["display_name"],
            ),
            reverse=True,
        )[:limit_transfer_candidates]

        mention_candidate_rows = conn.execute(
            """
            SELECT
                pm.id,
                pm.normalized_person_name,
                pm.raw_person_name,
                pm.name_quality,
                pm.raw_department_name,
                pm.contact_email,
                pm.contact_phone,
                pm.source_confidence,
                p.title,
                p.url,
                COALESCE(NULLIF(p.published_at, ''), substr(p.fetched_at, 1, 10)) AS observed_at
            FROM person_mentions pm
            JOIN projects p ON p.id = pm.project_id
            LEFT JOIN person_identity_links pil ON pil.person_mention_id = pm.id
            WHERE IFNULL(TRIM(pm.raw_person_name), '') != ''
              AND (
                    pil.id IS NULL
                    OR (pil.link_status = 'review_pending' AND pil.person_id IS NULL)
                  )
            ORDER BY observed_at DESC, pm.id DESC
            """
        ).fetchall()

        mention_candidates_by_key: dict[str, dict[str, Any]] = {}
        for row in mention_candidate_rows:
            name_key = (row["normalized_person_name"] or "").strip()
            department_key = normalize_department_for_match(row["raw_department_name"] or "")
            if not name_key:
                continue
            if not is_valid_roster_name(row["raw_person_name"] or ""):
                continue
            group_key = f"{name_key}|{department_key or 'nodept'}"
            current = mention_candidates_by_key.setdefault(
                group_key,
                {
                    "display_name": row["raw_person_name"],
                    "normalized_name": name_key,
                    "name_quality": row["name_quality"] or "unknown",
                    "department_name": (row["raw_department_name"] or "").strip(),
                    "latest_project_title": row["title"] or "",
                    "latest_project_url": row["url"] or "",
                    "latest_observed_at": row["observed_at"] or "",
                    "mention_count": 0,
                    "contact_email": row["contact_email"] or "",
                    "contact_phone": row["contact_phone"] or "",
                    "confidence": float(row["source_confidence"] or 0),
                },
            )
            current["mention_count"] += 1
            if (row["observed_at"] or "") >= current["latest_observed_at"]:
                current["latest_observed_at"] = row["observed_at"] or ""
                current["latest_project_title"] = row["title"] or ""
                current["latest_project_url"] = row["url"] or ""
            if float(row["source_confidence"] or 0) >= current["confidence"]:
                current["confidence"] = float(row["source_confidence"] or 0)
                current["contact_email"] = row["contact_email"] or current["contact_email"]
                current["contact_phone"] = row["contact_phone"] or current["contact_phone"]

        mention_candidates = sorted(
            [
                item
                for item in [
                    *[
                        {
                            "display_name": item["display_name"],
                            "normalized_name": normalize_person_name(item["display_name"]),
                            "name_quality": "surname_only",
                            "department_name": item["current_department"],
                            "latest_project_title": item["latest_project_title"],
                            "latest_project_id": item["latest_project_id"],
                            "latest_project_url": "",
                            "latest_observed_at": item["latest_observed_at"],
                            "mention_count": item["project_count"],
                            "contact_email": "",
                            "contact_phone": "",
                            "confidence": 0.4,
                            "person_key": item["person_key"],
                            "source_note": "人物ページありだがフルネーム証拠なし",
                        }
                        for item in weak_people_candidates
                    ],
                    *mention_candidates_by_key.values(),
                ]
                if is_viable_roster_mention_candidate(item)
            ],
            key=lambda item: (
                item["confidence"],
                item["mention_count"],
                item["latest_observed_at"],
                item["display_name"],
            ),
            reverse=True,
        )[:limit_mention_candidates]

        department_groups = build_department_roster_groups(
            confirmed_people,
            slot_roster,
            transfer_candidates,
            mention_candidates,
        )
        department_groups = hydrate_department_group_profiles(conn, department_groups)

        return {
            "confirmed_people": confirmed_people,
            "slot_roster": slot_roster,
            "slot_roster_as_of": roster_as_of,
            "transfer_candidates": transfer_candidates,
            "mention_candidates": mention_candidates,
            "department_groups": department_groups,
            "stats": {
                "confirmed_count": len(confirmed_people),
                "slot_roster_count": len(slot_roster),
                "transfer_candidate_count": len(transfer_candidates),
                "mention_candidate_count": len(mention_candidates),
                "department_group_count": len(department_groups),
            },
        }
    finally:
        conn.close()


def fetch_network_snapshot(
    project_limit: int = 140,
    relationship_limit: int = 18,
    top_unit_filter: str | None = None,
    db_path: Path | str = DB_PATH,
) -> dict[str, Any]:
    conn = get_connection(db_path)
    try:
        selected_top_unit = (top_unit_filter or "").strip()
        project_rows = conn.execute(
            """
            SELECT p.*, a.raw_department_name, a.raw_person_name, pe.person_key
            FROM projects p
            LEFT JOIN appearances a ON a.project_id = p.id
            LEFT JOIN people pe ON pe.id = a.person_id
            WHERE p.review_status != 'rejected'
            ORDER BY COALESCE(NULLIF(p.published_at, ''), substr(p.fetched_at, 1, 10)) DESC, p.id DESC
            LIMIT ?
            """,
            (project_limit,),
        ).fetchall()
        project_topic_map: dict[int, list[sqlite3.Row]] = {}
        project_ids = [int(row["id"]) for row in project_rows]
        if project_ids:
            placeholders = ", ".join("?" for _ in project_ids)
            topic_rows = conn.execute(
                f"""
                SELECT
                    ptl.project_id,
                    pt.id AS topic_id,
                    pt.name,
                    pt.topic_year,
                    pt.origin_type,
                    ptl.confidence,
                    ptl.is_priority
                FROM project_topic_links ptl
                JOIN policy_topics pt ON pt.id = ptl.topic_id
                WHERE ptl.project_id IN ({placeholders})
                ORDER BY ptl.project_id ASC, ptl.is_priority DESC, ptl.confidence DESC, pt.name ASC
                """,
                project_ids,
            ).fetchall()
            for topic_row in topic_rows:
                project_topic_map.setdefault(int(topic_row["project_id"]), []).append(topic_row)

        node_map: dict[str, dict[str, Any]] = {}
        edge_map: dict[tuple[str, str, str], dict[str, Any]] = {}
        cluster_map: dict[str, dict[str, Any]] = {}

        def short_label(text: str, max_length: int = 32) -> str:
            candidate = (text or "").strip()
            if len(candidate) <= max_length:
                return candidate
            return candidate[: max_length - 1].rstrip() + "…"

        def ensure_cluster(top_unit: str) -> dict[str, Any]:
            key = top_unit or "未分類"
            cluster = cluster_map.setdefault(
                key,
                {
                    "top_unit": key,
                    "project_ids": set(),
                    "department_ids": set(),
                    "person_ids": set(),
                    "topic_ids": set(),
                },
            )
            return cluster

        def ensure_node(
            node_id: str,
            *,
            label: str,
            node_type: str,
            group: str,
            href: str = "",
            subtitle: str = "",
        ) -> dict[str, Any]:
            node = node_map.get(node_id)
            if node is None:
                node = {
                    "id": node_id,
                    "label": label,
                    "short_label": short_label(label, 34 if node_type == "project" else 18),
                    "type": node_type,
                    "group": group or "未分類",
                    "href": href,
                    "subtitle": subtitle,
                    "weight": 0,
                }
                node_map[node_id] = node
            if subtitle and not node.get("subtitle"):
                node["subtitle"] = subtitle
            node["weight"] += 1
            return node

        def ensure_edge(source: str, target: str, edge_type: str) -> None:
            key = tuple(sorted((source, target))) + (edge_type,)
            edge = edge_map.get(key)
            if edge is None:
                edge = {
                    "source": source,
                    "target": target,
                    "type": edge_type,
                    "weight": 0,
                }
                edge_map[key] = edge
            edge["weight"] += 1

        for row in project_rows:
            resolved = resolve_public_appearance(conn, row, row)
            if not resolved:
                continue

            department_name = clean_department_name(resolved.get("display_department_name", "") or "")
            person_name = clean_person_name(resolved.get("display_person_name", "") or "")
            project_id = int(row["id"])
            hierarchy = resolve_department_hierarchy(department_name) if department_name else {"top_unit": row["source_type"] or "未分類"}
            top_unit = hierarchy.get("top_unit") or row["source_type"] or "未分類"
            if selected_top_unit and top_unit != selected_top_unit:
                continue
            cluster = ensure_cluster(top_unit)
            cluster["project_ids"].add(project_id)

            project_node_id = f"project:{project_id}"
            ensure_node(
                project_node_id,
                label=row["title"] or "",
                node_type="project",
                group=top_unit,
                href=f"/projects/{project_id}",
                subtitle=(row["published_at"] or row["source_type"] or "").strip(),
            )

            if department_name:
                department_node_id = f"department:{normalize_department_for_match(department_name)}"
                ensure_node(
                    department_node_id,
                    label=department_name,
                    node_type="department",
                    group=top_unit,
                    subtitle=top_unit,
                )
                cluster["department_ids"].add(department_node_id)
                ensure_edge(department_node_id, project_node_id, "department_project")

            if person_name and is_valid_person_name(person_name):
                person_key = (resolved.get("display_person_key") or "").strip()
                if person_key:
                    person_node_id = f"person:{person_key}"
                    href = f"/people/{urllib.parse.quote(person_key)}"
                    node_type = "person"
                else:
                    person_node_id = f"candidate_person:{normalize_person_name(person_name)}:{normalize_department_for_match(department_name)}"
                    href = ""
                    node_type = "candidate_person"
                ensure_node(
                    person_node_id,
                    label=person_name,
                    node_type=node_type,
                    group=top_unit,
                    href=href,
                    subtitle=resolved.get("person_status_label", "") or "",
                )
                cluster["person_ids"].add(person_node_id)
                ensure_edge(person_node_id, project_node_id, "person_project")

            for topic_row in project_topic_map.get(project_id, [])[:3]:
                topic_node_id = f"topic:{int(topic_row['topic_id'])}:{top_unit}"
                topic_subtitle_parts = []
                if topic_row["topic_year"]:
                    topic_subtitle_parts.append(str(topic_row["topic_year"]))
                if int(topic_row["is_priority"] or 0):
                    topic_subtitle_parts.append("重点施策")
                ensure_node(
                    topic_node_id,
                    label=topic_row["name"] or "",
                    node_type="topic",
                    group=top_unit,
                    subtitle=" / ".join(topic_subtitle_parts),
                )
                cluster["topic_ids"].add(topic_node_id)
                ensure_edge(topic_node_id, project_node_id, "topic_project")

        nodes: list[dict[str, Any]] = []
        for node in node_map.values():
            base_size = {
                "project": 10,
                "department": 16,
                "person": 18,
                "candidate_person": 13,
                "topic": 22,
            }.get(node["type"], 12)
            growth = {
                "project": min(node["weight"], 2),
                "department": min(node["weight"] * 2, 16),
                "person": min(node["weight"] * 2.5, 20),
                "candidate_person": min(node["weight"] * 2, 12),
                "topic": min(node["weight"] * 3, 24),
            }.get(node["type"], min(node["weight"], 6))
            node["size"] = base_size + growth
            nodes.append(node)

        edges = sorted(
            edge_map.values(),
            key=lambda item: (item["weight"], item["type"]),
            reverse=True,
        )

        clusters = sorted(
            [
                {
                    "top_unit": cluster["top_unit"],
                    "project_count": len(cluster["project_ids"]),
                    "department_count": len(cluster["department_ids"]),
                    "person_count": len(cluster["person_ids"]),
                    "topic_count": len(cluster["topic_ids"]),
                }
                for cluster in cluster_map.values()
            ],
            key=lambda item: (
                item["project_count"] + item["person_count"] + item["topic_count"],
                item["department_count"],
                item["top_unit"],
            ),
            reverse=True,
        )

        transfer_flow_counts: dict[tuple[str, str], int] = {}
        for row in conn.execute(
            """
            SELECT from_department_raw, to_department_raw
            FROM transfer_events
            WHERE IFNULL(TRIM(from_department_raw), '') != ''
               OR IFNULL(TRIM(to_department_raw), '') != ''
            """
        ).fetchall():
            source_top = resolve_department_hierarchy(row["from_department_raw"] or "").get("top_unit", "")
            target_top = resolve_department_hierarchy(row["to_department_raw"] or "").get("top_unit", "")
            if not source_top or not target_top or source_top == target_top or "未分類" in {source_top, target_top}:
                continue
            if selected_top_unit and selected_top_unit not in {source_top, target_top}:
                continue
            key = (source_top, target_top)
            transfer_flow_counts[key] = transfer_flow_counts.get(key, 0) + 1

        continuity_counts: dict[tuple[str, str], dict[str, Any]] = {}
        linked_rows = conn.execute(
            """
            WITH linked_projects AS (
                SELECT DISTINCT a.person_id, a.raw_department_name
                FROM appearances a
                WHERE a.person_id IS NOT NULL
                UNION
                SELECT DISTINCT pil.person_id, pm.raw_department_name
                FROM person_identity_links pil
                JOIN person_mentions pm ON pm.id = pil.person_mention_id
                WHERE pil.person_id IS NOT NULL
                  AND pil.link_status IN ('reviewed_match', 'auto_matched')
            )
            SELECT pe.id AS person_id, pe.display_name, lp.raw_department_name
            FROM linked_projects lp
            JOIN people pe ON pe.id = lp.person_id
            WHERE IFNULL(TRIM(lp.raw_department_name), '') != ''
            ORDER BY pe.id ASC
            """
        ).fetchall()
        departments_by_person: dict[int, tuple[str, set[str]]] = {}
        for row in linked_rows:
            if not is_valid_roster_name(row["display_name"] or ""):
                continue
            person_id = int(row["person_id"])
            person_name = row["display_name"] or ""
            top_unit = resolve_department_hierarchy(row["raw_department_name"] or "").get("top_unit", "")
            if not top_unit or top_unit == "未分類":
                continue
            current = departments_by_person.setdefault(person_id, (person_name, set()))
            current[1].add(top_unit)

        for person_name, top_units in departments_by_person.values():
            ordered_top_units = sorted(top_units)
            for left, right in combinations(ordered_top_units, 2):
                if selected_top_unit and selected_top_unit not in {left, right}:
                    continue
                key = (left, right)
                payload = continuity_counts.setdefault(
                    key,
                    {
                        "source_top_unit": left,
                        "target_top_unit": right,
                        "count": 0,
                        "sample_people": [],
                    },
                )
                payload["count"] += 1
                if person_name not in payload["sample_people"] and len(payload["sample_people"]) < 3:
                    payload["sample_people"].append(person_name)

        transfer_flows = sorted(
            [
                {
                    "source_top_unit": source,
                    "target_top_unit": target,
                    "count": count,
                }
                for (source, target), count in transfer_flow_counts.items()
            ],
            key=lambda item: (item["count"], item["source_top_unit"], item["target_top_unit"]),
            reverse=True,
        )[:relationship_limit]

        continuity_links = sorted(
            continuity_counts.values(),
            key=lambda item: (item["count"], item["source_top_unit"], item["target_top_unit"]),
            reverse=True,
        )[:relationship_limit]

        return {
            "graph": {
                "nodes": nodes,
                "edges": edges,
            },
            "clusters": clusters[:12],
            "transfer_flows": transfer_flows,
            "continuity_links": continuity_links,
            "selected_top_unit": selected_top_unit,
            "stats": {
                "project_count": sum(1 for node in nodes if node["type"] == "project"),
                "department_count": sum(1 for node in nodes if node["type"] == "department"),
                "person_count": sum(1 for node in nodes if node["type"] in {"person", "candidate_person"}),
                "topic_count": sum(1 for node in nodes if node["type"] == "topic"),
                "verified_person_count": sum(1 for node in nodes if node["type"] == "person"),
                "cluster_count": len(clusters),
                "edge_count": len(edges),
            },
        }
    finally:
        conn.close()


def summarize_person_movements(
    department_history: list[sqlite3.Row] | list[dict[str, Any]],
    transfer_history: list[sqlite3.Row] | list[dict[str, Any]],
) -> dict[str, Any]:
    chain: list[dict[str, Any]] = []
    seen: set[str] = set()

    def add_department(name: str, as_of: str, source: str) -> None:
        department_name = (name or "").strip()
        if not department_name or department_name == "部署不明" or department_name in seen:
            return
        seen.add(department_name)
        chain.append(
            {
                "name": department_name,
                "as_of": as_of,
                "source": source,
            }
        )

    latest_transfer: dict[str, Any] | None = None
    for item in transfer_history:
        row = dict(item)
        if latest_transfer is None:
            latest_transfer = row
        as_of = row.get("effective_date") or row.get("source_published_at") or ""
        add_department(row.get("to_department_raw") or "", as_of, "transfer_to")
        add_department(row.get("from_department_raw") or "", as_of, "transfer_from")

    if not chain:
        for item in department_history:
            row = dict(item)
            add_department(row.get("department_name") or "", row.get("last_seen_at") or "", "project_history")

    return {
        "current_department": chain[0] if chain else None,
        "previous_departments": chain[1:4],
        "latest_transfer": latest_transfer,
        "department_chain": chain,
    }


def fetch_related_people_for_person(
    conn: sqlite3.Connection,
    person_id: int,
    own_project_ids: set[int],
    own_departments: set[str],
    limit: int = 8,
) -> list[dict[str, Any]]:
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
        SELECT
            pe.id AS person_id,
            pe.person_key,
            pe.display_name,
            lp.project_id,
            lp.raw_department_name,
            p.title,
            COALESCE(NULLIF(p.published_at, ''), substr(p.fetched_at, 1, 10)) AS activity_at
        FROM linked_projects lp
        JOIN people pe ON pe.id = lp.person_id
        JOIN projects p ON p.id = lp.project_id
        WHERE pe.id != ?
        ORDER BY activity_at DESC, p.id DESC
        """,
        (person_id,),
    ).fetchall()

    related: dict[int, dict[str, Any]] = {}
    for row in rows:
        if not is_valid_person_name(row["display_name"]):
            continue

        current = related.setdefault(
            int(row["person_id"]),
            {
                "person_id": int(row["person_id"]),
                "person_key": row["person_key"],
                "display_name": row["display_name"],
                "shared_project_ids": set(),
                "shared_departments": set(),
                "latest_shared_project_title": "",
                "latest_shared_at": "",
            },
        )

        project_id = int(row["project_id"])
        department_name = (row["raw_department_name"] or "").strip()
        activity_at = row["activity_at"] or ""

        if project_id in own_project_ids:
            current["shared_project_ids"].add(project_id)
            if activity_at >= current["latest_shared_at"]:
                current["latest_shared_at"] = activity_at
                current["latest_shared_project_title"] = row["title"]

        if department_name and department_name in own_departments:
            current["shared_departments"].add(department_name)

    payload: list[dict[str, Any]] = []
    for item in related.values():
        shared_project_count = len(item["shared_project_ids"])
        shared_department_count = len(item["shared_departments"])
        if shared_project_count == 0 and shared_department_count == 0:
            continue
        payload.append(
            {
                "person_id": item["person_id"],
                "person_key": item["person_key"],
                "display_name": item["display_name"],
                "shared_project_count": shared_project_count,
                "shared_department_count": shared_department_count,
                "shared_departments": sorted(item["shared_departments"])[:3],
                "latest_shared_project_title": item["latest_shared_project_title"],
                "latest_shared_at": item["latest_shared_at"],
                "connection_score": shared_project_count * 3 + shared_department_count,
            }
        )

    payload.sort(
        key=lambda item: (
            item["connection_score"],
            item["shared_project_count"],
            item["shared_department_count"],
            item["latest_shared_at"],
            item["display_name"],
        ),
        reverse=True,
    )
    return payload[:limit]


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

        transfer_history = conn.execute(
            """
            SELECT
                te.id AS transfer_event_id,
                te.effective_date,
                te.from_department_raw,
                te.to_department_raw,
                te.from_title_raw,
                te.to_title_raw,
                ts.source_type,
                ts.title AS source_title,
                ts.url AS source_url,
                ts.published_at AS source_published_at,
                til.link_status,
                til.confidence
            FROM transfer_identity_links til
            JOIN transfer_events te ON te.id = til.transfer_event_id
            JOIN transfer_sources ts ON ts.id = te.transfer_source_id
            WHERE til.person_id = ?
              AND til.link_status IN ('reviewed_match', 'auto_matched')
            ORDER BY COALESCE(NULLIF(te.effective_date, ''), NULLIF(ts.published_at, ''), '0000-00-00') DESC,
                     te.id DESC
            """,
            (person["id"],),
        ).fetchall()

        movement_summary = summarize_person_movements(department_history, transfer_history)
        own_project_ids = {int(item["id"]) for item in projects}
        own_departments = {
            (item["department_name"] or "").strip()
            for item in department_history
            if (item["department_name"] or "").strip() and (item["department_name"] or "").strip() != "部署不明"
        }
        related_people = fetch_related_people_for_person(conn, int(person["id"]), own_project_ids, own_departments)
        topic_rollups = conn.execute(
            """
            SELECT
                ptr.*,
                pt.name AS topic_name,
                pt.origin_type,
                pt.description
            FROM person_topic_rollups ptr
            JOIN policy_topics pt ON pt.id = ptr.topic_id
            WHERE ptr.person_id = ?
            ORDER BY ptr.spotlight_score DESC, ptr.priority_project_count DESC, ptr.project_count DESC, pt.name ASC
            LIMIT 12
            """,
            (person["id"],),
        ).fetchall()
        topic_summary = {
            "topic_count": len(topic_rollups),
            "priority_topic_count": sum(1 for item in topic_rollups if int(item["priority_project_count"] or 0) > 0),
            "focus_topic_names": [item["topic_name"] for item in topic_rollups[:5]],
        }

        interviews = conn.execute(
            """
            SELECT i.*, pr.title AS project_title 
            FROM interviews i
            LEFT JOIN projects pr ON pr.id = i.project_id
            WHERE i.person_id = ?
            ORDER BY i.published_at DESC
            """,
            (person["id"],),
        ).fetchall()

        return {
            "person": dict(person),
            "projects": [dict(item) for item in projects],
            "departments": [row["raw_department_name"] for row in departments],
            "department_history": [dict(item) for item in department_history],
            "transfer_history": [dict(item) for item in transfer_history],
            "movement_summary": movement_summary,
            "related_people": related_people,
            "topic_rollups": [dict(item) for item in topic_rollups],
            "topic_summary": topic_summary,
            "interviews": [dict(item) for item in interviews],
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


def cleanup_orphan_departments(db_path: Path | str = DB_PATH) -> int:
    conn = get_connection(db_path)
    try:
        orphan_ids = [
            int(row["id"])
            for row in conn.execute(
                """
                SELECT d.id
                FROM departments d
                LEFT JOIN appearances a ON a.department_id = d.id
                LEFT JOIN person_mentions pm ON pm.department_id = d.id
                LEFT JOIN transfer_events te_from ON te_from.from_department_id = d.id
                LEFT JOIN transfer_events te_to ON te_to.to_department_id = d.id
                LEFT JOIN department_aliases da ON da.department_id = d.id
                GROUP BY d.id
                HAVING COUNT(DISTINCT a.id) = 0
                   AND COUNT(DISTINCT pm.id) = 0
                   AND COUNT(DISTINCT te_from.id) = 0
                   AND COUNT(DISTINCT te_to.id) = 0
                   AND COUNT(DISTINCT da.id) = 0
                """
            ).fetchall()
        ]
        if orphan_ids:
            conn.executemany("DELETE FROM departments WHERE id = ?", [(department_id,) for department_id in orphan_ids])
            conn.commit()
        return len(orphan_ids)
    finally:
        conn.close()


def refresh_department_references(db_path: Path | str = DB_PATH) -> dict[str, int]:
    init_db(db_path)
    conn = get_connection(db_path)
    counts = {
        "person_mentions_updated": 0,
        "appearances_updated": 0,
        "transfer_from_updated": 0,
        "transfer_to_updated": 0,
    }
    try:
        seed_department_reference(conn)

        mention_rows = conn.execute("SELECT id, raw_department_name, department_id FROM person_mentions").fetchall()
        for row in mention_rows:
            department_id = get_or_create_department(conn, row["raw_department_name"] or "")
            if department_id != row["department_id"]:
                conn.execute("UPDATE person_mentions SET department_id = ? WHERE id = ?", (department_id, row["id"]))
                counts["person_mentions_updated"] += 1

        appearance_rows = conn.execute("SELECT id, raw_department_name, department_id FROM appearances").fetchall()
        for row in appearance_rows:
            department_id = get_or_create_department(conn, row["raw_department_name"] or "")
            if department_id != row["department_id"]:
                conn.execute("UPDATE appearances SET department_id = ? WHERE id = ?", (department_id, row["id"]))
                counts["appearances_updated"] += 1

        transfer_rows = conn.execute("SELECT id, from_department_raw, from_department_id, to_department_raw, to_department_id FROM transfer_events").fetchall()
        for row in transfer_rows:
            from_department_id = get_or_create_department(conn, row["from_department_raw"] or "")
            to_department_id = get_or_create_department(conn, row["to_department_raw"] or "")
            if from_department_id != row["from_department_id"]:
                conn.execute("UPDATE transfer_events SET from_department_id = ? WHERE id = ?", (from_department_id, row["id"]))
                counts["transfer_from_updated"] += 1
            if to_department_id != row["to_department_id"]:
                conn.execute("UPDATE transfer_events SET to_department_id = ? WHERE id = ?", (to_department_id, row["id"]))
                counts["transfer_to_updated"] += 1

        conn.commit()
    finally:
        conn.close()

    counts["orphan_departments_removed"] = cleanup_orphan_departments(db_path)
    return counts


def build_transfer_source_key(
    source_type: str,
    title: str,
    url: str = "",
    published_at: str = "",
    effective_date: str = "",
) -> str:
    stable_url = (url or "").strip()
    stable_title = normalize_text(title or "")
    stable_date = (effective_date or published_at or "").strip()
    anchor = stable_url or stable_title
    return f"{source_type}:{anchor}:{stable_date}"


def upsert_transfer_source(
    conn: sqlite3.Connection,
    source_type: str,
    title: str,
    url: str = "",
    publisher: str = "",
    published_at: str = "",
    effective_date: str = "",
    raw_text: str = "",
) -> int:
    source_key = build_transfer_source_key(source_type, title, url, published_at, effective_date)
    conn.execute(
        """
        INSERT INTO transfer_sources (
            source_type, source_key, title, url, publisher, published_at, effective_date, raw_text
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_key) DO UPDATE SET
            title = excluded.title,
            url = excluded.url,
            publisher = excluded.publisher,
            published_at = excluded.published_at,
            effective_date = excluded.effective_date,
            raw_text = excluded.raw_text,
            updated_at = CURRENT_TIMESTAMP
        """,
        (source_type, source_key, title, url, publisher, published_at, effective_date, raw_text),
    )
    row = conn.execute("SELECT id FROM transfer_sources WHERE source_key = ?", (source_key,)).fetchone()
    return int(row["id"])


def replace_transfer_events_for_source(
    conn: sqlite3.Connection,
    transfer_source_id: int,
    events: list[dict[str, str]],
) -> int:
    conn.execute("DELETE FROM transfer_events WHERE transfer_source_id = ?", (transfer_source_id,))
    inserted = 0
    for index, event in enumerate(events):
        raw_person_name = (event.get("person_name") or "").strip()
        normalized_person_name = normalize_person_name(raw_person_name)
        if not raw_person_name or not normalized_person_name:
            continue
        from_department_raw = clean_department_name(event.get("from_department") or "")
        to_department_raw = clean_department_name(event.get("to_department") or "")
        conn.execute(
            """
            INSERT INTO transfer_events (
                transfer_source_id, event_index, effective_date, raw_person_name, normalized_person_name,
                name_quality, from_department_raw, from_department_id, to_department_raw, to_department_id,
                from_title_raw, to_title_raw, evidence_snippet, review_status
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
            """,
            (
                transfer_source_id,
                index,
                (event.get("effective_date") or "").strip(),
                raw_person_name,
                normalized_person_name,
                classify_person_name_quality(raw_person_name),
                from_department_raw,
                get_or_create_department(conn, from_department_raw),
                to_department_raw,
                get_or_create_department(conn, to_department_raw),
                (event.get("from_title") or "").strip(),
                (event.get("to_title") or "").strip(),
                (event.get("evidence_snippet") or "").strip(),
            ),
        )
        inserted += 1
    return inserted


def import_transfers_csv(csv_path: Path | str, db_path: Path | str = DB_PATH) -> dict[str, int]:
    init_db(db_path)
    source_groups: dict[str, dict[str, Any]] = {}
    with Path(csv_path).open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        required = {
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
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"transfer CSVに必要な列がありません: {', '.join(sorted(missing))}")

        for row in reader:
            source_type = (row.get("source_type") or "").strip()
            title = (row.get("title") or "").strip()
            url = (row.get("url") or "").strip()
            published_at = (row.get("published_at") or "").strip()
            effective_date = (row.get("effective_date") or "").strip()
            source_key = build_transfer_source_key(source_type, title, url, published_at, effective_date)
            bucket = source_groups.setdefault(
                source_key,
                {
                    "source_type": source_type,
                    "title": title,
                    "url": url,
                    "publisher": (row.get("publisher") or "").strip(),
                    "published_at": published_at,
                    "effective_date": effective_date,
                    "raw_text": (row.get("raw_text") or "").strip(),
                    "events": [],
                },
            )
            bucket["events"].append(row)

    conn = get_connection(db_path)
    counts = {"sources": 0, "events": 0, "transfer_links": 0, "employee_slots": 0, "slot_candidates": 0}
    try:
        for bucket in source_groups.values():
            source_id = upsert_transfer_source(
                conn,
                bucket["source_type"],
                bucket["title"],
                bucket["url"],
                bucket["publisher"],
                bucket["published_at"],
                bucket["effective_date"],
                bucket["raw_text"],
            )
            counts["sources"] += 1
            counts["events"] += replace_transfer_events_for_source(conn, source_id, bucket["events"])
        conn.commit()
        counts["transfer_links"] = auto_link_transfer_events(conn)
        slot_counts = rebuild_employee_slots(conn)
        counts["employee_slots"] = slot_counts["slots_created"]
        counts["slot_candidates"] = refresh_slot_candidates(conn)
        conn.commit()
    finally:
        conn.close()
    return counts


def refresh_employee_slots(db_path: Path | str = DB_PATH) -> dict[str, int]:
    init_db(db_path)
    conn = get_connection(db_path)
    try:
        slot_counts = rebuild_employee_slots(conn)
        slot_counts["slot_candidates"] = refresh_slot_candidates(conn)
        conn.commit()
        return slot_counts
    finally:
        conn.close()


def score_transfer_identity_candidate(
    transfer_event_row: sqlite3.Row | dict[str, Any],
    person_row: sqlite3.Row | dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    event = dict(transfer_event_row)
    person = dict(person_row)

    event_name = event.get("normalized_person_name", "") or ""
    person_name = person.get("normalized_name", "") or ""
    event_name_quality = event.get("name_quality", "unknown") or "unknown"

    exact_name_match = bool(event_name and person_name and event_name == person_name)
    surname_prefix_match = bool(
        (
            event_name_quality == "surname_only"
            and event_name
            and person_name
            and person_name.startswith(event_name)
        )
        or partial_person_name_match(event_name, person_name)
    )
    to_department_overlap = bool(
        event.get("to_department_raw")
        and any(department_match_overlap(event.get("to_department_raw") or "", department_name) for department_name in context["departments"])
    )
    from_department_overlap = bool(
        event.get("from_department_raw")
        and any(department_match_overlap(event.get("from_department_raw") or "", department_name) for department_name in context["departments"])
    )
    to_department_match = bool(
        event.get("to_department_id")
        and int(event["to_department_id"]) in context["department_ids"]
    )
    from_department_match = bool(
        event.get("from_department_id")
        and int(event["from_department_id"]) in context["department_ids"]
    )
    to_top_unit = resolve_department_hierarchy(event.get("to_department_raw") or "").get("top_unit", "")
    from_top_unit = resolve_department_hierarchy(event.get("from_department_raw") or "").get("top_unit", "")
    to_department_top_unit_match = bool(
        to_top_unit and to_top_unit != "未分類" and to_top_unit in context.get("department_top_units", set())
    )
    from_department_top_unit_match = bool(
        from_top_unit and from_top_unit != "未分類" and from_top_unit in context.get("department_top_units", set())
    )

    event_date = parse_iso_date(event.get("effective_date") or "")
    activity_recent = bool(
        event_date
        and any(abs((event_date - activity_date).days) <= 180 for activity_date in context["activity_dates"])
    )

    score = 0.1
    if exact_name_match:
        score += 0.45
    elif surname_prefix_match:
        score += 0.12
    else:
        score -= 0.50

    if to_department_match:
        score += 0.20
    elif to_department_overlap:
        score += 0.12
    elif to_department_top_unit_match:
        score += 0.05
    if from_department_match:
        score += 0.15
    elif from_department_overlap:
        score += 0.10
    elif from_department_top_unit_match:
        score += 0.04
    if activity_recent:
        score += 0.10
    if context["project_count"] >= 2:
        score += min(context["project_count"] * 0.02, 0.08)
    if surname_prefix_match and not exact_name_match and not (to_department_match or to_department_overlap or from_department_match or from_department_overlap):
        score -= 0.10

    return {
        "score": round(max(min(score, 0.98), 0), 2),
        "exact_name_match": exact_name_match,
        "surname_prefix_match": surname_prefix_match,
        "to_department_match": to_department_match,
        "from_department_match": from_department_match,
        "to_department_overlap": to_department_overlap,
        "from_department_overlap": from_department_overlap,
        "to_department_top_unit_match": to_department_top_unit_match,
        "from_department_top_unit_match": from_department_top_unit_match,
        "activity_recent": activity_recent,
    }


def upsert_transfer_identity_link(
    conn: sqlite3.Connection,
    transfer_event_id: int,
    person_id: int | None,
    link_status: str,
    confidence: float,
    matched_by: str,
    notes: str = "",
) -> None:
    conn.execute(
        """
        INSERT INTO transfer_identity_links (
            transfer_event_id, person_id, link_status, confidence, matched_by, notes, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(transfer_event_id) DO UPDATE SET
            person_id = excluded.person_id,
            link_status = excluded.link_status,
            confidence = excluded.confidence,
            matched_by = excluded.matched_by,
            notes = excluded.notes,
            updated_at = CURRENT_TIMESTAMP
        """,
        (transfer_event_id, person_id, link_status, confidence, matched_by, notes),
    )


def auto_link_transfer_events(
    conn: sqlite3.Connection,
    transfer_source_id: int | None = None,
    person_normalized_name: str | None = None,
) -> int:
    query = """
        SELECT te.*, til.link_status AS existing_link_status
        FROM transfer_events te
        LEFT JOIN transfer_identity_links til ON til.transfer_event_id = te.id
    """
    params: list[Any] = []
    conditions: list[str] = []
    if transfer_source_id is not None:
        conditions.append("te.transfer_source_id = ?")
        params.append(transfer_source_id)
    if person_normalized_name:
        conditions.append(
            """
            (
                te.normalized_person_name = ?
                OR (te.name_quality = 'surname_only' AND ? LIKE te.normalized_person_name || '%')
            )
            """
        )
        params.extend([person_normalized_name, person_normalized_name])
    if conditions:
        query += " WHERE " + " AND ".join(condition.strip() for condition in conditions)
    rows = conn.execute(query, params).fetchall()
    linked = 0

    for row in rows:
        existing_status = row["existing_link_status"] or ""
        if existing_status in {"reviewed_match", "reviewed_distinct"}:
            continue

        normalized_name = row["normalized_person_name"] or ""
        if not normalized_name:
            continue

        people = conn.execute(
            """
            SELECT *
            FROM people
            WHERE normalized_name = ?
               OR (? != '' AND normalized_name LIKE ?)
               OR (? != '' AND ? LIKE normalized_name || '%')
            ORDER BY id ASC
            """,
            (
                normalized_name,
                normalized_name if (row["name_quality"] or "") == "surname_only" else "",
                f"{normalized_name}%",
                normalized_name,
                normalized_name,
            ),
        ).fetchall()

        scored: list[tuple[sqlite3.Row, dict[str, Any]]] = []
        for person in people:
            if not is_valid_person_name(person["display_name"]):
                continue
            context = fetch_person_identity_context(conn, int(person["id"]))
            metrics = score_transfer_identity_candidate(row, person, context)
            if metrics["score"] <= 0:
                continue
            scored.append((person, metrics))

        scored.sort(
            key=lambda item: (
                item[1]["score"],
                1 if item[1]["to_department_match"] else 0,
                1 if item[1]["to_department_overlap"] else 0,
                1 if item[1]["from_department_match"] else 0,
                1 if item[1]["from_department_overlap"] else 0,
                1 if item[1]["exact_name_match"] else 0,
                1 if item[1]["activity_recent"] else 0,
                item[0]["display_name"],
            ),
            reverse=True,
        )

        if not scored:
            continue

        top_person, top_metrics = scored[0]
        second_score = scored[1][1]["score"] if len(scored) > 1 else 0.0
        gap = top_metrics["score"] - second_score

        strong_department_signal = any(
            [
                top_metrics["to_department_match"],
                top_metrics["to_department_overlap"],
                top_metrics["from_department_match"],
                top_metrics["from_department_overlap"],
            ]
        )
        broad_department_signal = strong_department_signal or any(
            [
                top_metrics.get("to_department_top_unit_match"),
                top_metrics.get("from_department_top_unit_match"),
            ]
        )

        if top_metrics["score"] >= 0.85 and gap >= 0.12:
            upsert_transfer_identity_link(
                conn,
                int(row["id"]),
                int(top_person["id"]),
                "auto_matched",
                top_metrics["score"],
                "transfer_continuity",
            )
            linked += 1
        elif (
            top_metrics["score"] >= 0.68
            or (top_metrics["exact_name_match"] and top_metrics["score"] >= 0.55)
            or (
                top_metrics["surname_prefix_match"]
                and strong_department_signal
                and top_metrics["score"] >= 0.42
                and gap >= 0.05
            )
            or (
                top_metrics["surname_prefix_match"]
                and broad_department_signal
                and top_metrics["activity_recent"]
                and top_metrics["score"] >= 0.44
                and gap >= 0.08
            )
        ):
            upsert_transfer_identity_link(
                conn,
                int(row["id"]),
                int(top_person["id"]),
                "review_pending",
                top_metrics["score"],
                "transfer_continuity",
                notes=(
                    "Exact name match from kiji data"
                    if top_metrics["exact_name_match"]
                    else "Surname/full-name bridge from kiji data"
                ),
            )
            linked += 1

    return linked


def refresh_transfer_links_for_person(conn: sqlite3.Connection, person_id: int | None) -> int:
    if not person_id:
        return 0
    person = conn.execute("SELECT normalized_name FROM people WHERE id = ?", (person_id,)).fetchone()
    normalized_name = (person["normalized_name"] or "").strip() if person else ""
    if not normalized_name:
        return 0
    return auto_link_transfer_events(conn, person_normalized_name=normalized_name)


def refresh_transfer_links_for_project(conn: sqlite3.Connection, project_id: int) -> int:
    names = [
        (row["normalized_person_name"] or "").strip()
        for row in conn.execute(
            """
            SELECT DISTINCT normalized_person_name
            FROM person_mentions
            WHERE project_id = ?
              AND IFNULL(TRIM(normalized_person_name), '') != ''
            """,
            (project_id,),
        ).fetchall()
    ]
    refreshed = 0
    for normalized_name in names:
        refreshed += auto_link_transfer_events(conn, person_normalized_name=normalized_name)
    return refreshed


def find_matching_open_slot(
    open_slots: list[dict[str, Any]],
    department_id: int | None,
    department_name: str,
) -> dict[str, Any] | None:
    candidates: list[tuple[int, int, str, dict[str, Any]]] = []
    for slot in open_slots:
        if slot.get("active_to"):
            continue
        exact = bool(department_id and slot.get("department_id") and int(slot["department_id"]) == int(department_id))
        overlap = bool(department_name and department_match_overlap(department_name, slot.get("raw_department_name", "")))
        if exact or overlap:
            candidates.append((1 if exact else 0, 1 if overlap else 0, slot.get("active_from", ""), slot))
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item[0], item[1], item[2]), reverse=True)
    return candidates[0][3]


def rebuild_employee_slots(conn: sqlite3.Connection) -> dict[str, int]:
    conn.execute("DELETE FROM employee_slots")

    rows = conn.execute(
        """
        SELECT
            te.*,
            ts.published_at AS source_published_at,
            til.person_id AS linked_person_id,
            til.confidence AS linked_confidence,
            pe.display_name AS linked_person_name,
            pe.normalized_name AS linked_normalized_name
        FROM transfer_events te
        JOIN transfer_sources ts ON ts.id = te.transfer_source_id
        LEFT JOIN transfer_identity_links til ON til.transfer_event_id = te.id
        LEFT JOIN people pe ON pe.id = til.person_id
        WHERE (
                te.name_quality = 'full_name'
                OR til.person_id IS NOT NULL
              )
          AND IFNULL(TRIM(COALESCE(pe.normalized_name, te.normalized_person_name)), '') != ''
        ORDER BY
            COALESCE(pe.normalized_name, te.normalized_person_name) ASC,
            COALESCE(NULLIF(te.effective_date, ''), NULLIF(ts.published_at, ''), '0000-00-00') ASC,
            te.id ASC
        """
    ).fetchall()

    open_slots_by_name: dict[str, list[dict[str, Any]]] = {}
    counts = {"slots_created": 0, "slots_closed": 0}

    for row in rows:
        slot_name = (row["linked_normalized_name"] or row["normalized_person_name"] or "").strip()
        if not slot_name:
            continue

        display_name = (row["linked_person_name"] or row["raw_person_name"] or "").strip()
        name_quality = "full_name" if row["linked_person_id"] or row["name_quality"] == "full_name" else (row["name_quality"] or "unknown")
        from_department_name = (row["from_department_raw"] or "").strip()
        to_department_name = (row["to_department_raw"] or "").strip()
        effective_from = (row["effective_date"] or row["source_published_at"] or "").strip()
        open_slots = open_slots_by_name.setdefault(slot_name, [])

        current_slot = find_matching_open_slot(open_slots, row["to_department_id"], to_department_name)
        previous_slot = find_matching_open_slot(open_slots, row["from_department_id"], from_department_name)

        if current_slot and previous_slot and current_slot["id"] == previous_slot["id"]:
            conn.execute(
                """
                UPDATE employee_slots
                SET display_name = ?, name_quality = ?, title_raw = ?, person_id = COALESCE(person_id, ?),
                    slot_confidence = MAX(slot_confidence, ?), source_transfer_event_id = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    display_name or current_slot["display_name"],
                    name_quality,
                    (row["to_title_raw"] or current_slot.get("title_raw") or "").strip(),
                    row["linked_person_id"],
                    float(row["linked_confidence"] or 0.72),
                    int(row["id"]),
                    current_slot["id"],
                ),
            )
            current_slot["display_name"] = display_name or current_slot["display_name"]
            current_slot["person_id"] = row["linked_person_id"] or current_slot.get("person_id")
            current_slot["title_raw"] = (row["to_title_raw"] or current_slot.get("title_raw") or "").strip()
            current_slot["slot_confidence"] = max(float(current_slot.get("slot_confidence") or 0), float(row["linked_confidence"] or 0.72))
            continue

        if previous_slot:
            end_date = iso_day_before(effective_from) if effective_from else ""
            conn.execute(
                "UPDATE employee_slots SET active_to = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (end_date, previous_slot["id"]),
            )
            previous_slot["active_to"] = end_date
            open_slots[:] = [slot for slot in open_slots if slot["id"] != previous_slot["id"]]
            counts["slots_closed"] += 1

        if not (row["to_department_id"] or to_department_name):
            continue

        if current_slot:
            conn.execute(
                """
                UPDATE employee_slots
                SET display_name = ?, name_quality = ?, title_raw = ?, person_id = COALESCE(person_id, ?),
                    slot_confidence = MAX(slot_confidence, ?), source_transfer_event_id = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (
                    display_name or current_slot["display_name"],
                    name_quality,
                    (row["to_title_raw"] or current_slot.get("title_raw") or "").strip(),
                    row["linked_person_id"],
                    float(row["linked_confidence"] or 0.72),
                    int(row["id"]),
                    current_slot["id"],
                ),
            )
            current_slot["display_name"] = display_name or current_slot["display_name"]
            current_slot["person_id"] = row["linked_person_id"] or current_slot.get("person_id")
            current_slot["title_raw"] = (row["to_title_raw"] or current_slot.get("title_raw") or "").strip()
            current_slot["slot_confidence"] = max(float(current_slot.get("slot_confidence") or 0), float(row["linked_confidence"] or 0.72))
            continue

        slot_key = ":".join(
            [
                slot_name,
                normalize_department_for_match(to_department_name) or str(row["to_department_id"] or "nodept"),
                effective_from or "unknown",
                str(row["id"]),
            ]
        )
        confidence = float(row["linked_confidence"] or (0.82 if name_quality == "full_name" else 0.68))
        conn.execute(
            """
            INSERT INTO employee_slots (
                slot_key, normalized_person_name, display_name, name_quality, department_id, raw_department_name,
                title_raw, active_from, active_to, source_transfer_event_id, previous_slot_id, person_id, slot_confidence
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?, ?)
            """,
            (
                slot_key,
                slot_name,
                display_name,
                name_quality,
                row["to_department_id"],
                to_department_name,
                (row["to_title_raw"] or "").strip(),
                effective_from,
                int(row["id"]),
                previous_slot["id"] if previous_slot else None,
                row["linked_person_id"],
                confidence,
            ),
        )
        slot_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
        if previous_slot:
            conn.execute(
                "UPDATE employee_slots SET next_slot_id = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                (slot_id, previous_slot["id"]),
            )

        open_slots.append(
            {
                "id": slot_id,
                "department_id": row["to_department_id"],
                "raw_department_name": to_department_name,
                "display_name": display_name,
                "active_from": effective_from,
                "active_to": "",
                "person_id": row["linked_person_id"],
                "title_raw": (row["to_title_raw"] or "").strip(),
                "slot_confidence": confidence,
            }
        )
        counts["slots_created"] += 1

    return counts


def score_slot_candidate(
    mention_row: sqlite3.Row | dict[str, Any],
    slot_row: sqlite3.Row | dict[str, Any],
) -> dict[str, Any]:
    mention = dict(mention_row)
    slot = dict(slot_row)

    mention_name = (mention.get("normalized_person_name") or "").strip()
    slot_name = (slot.get("normalized_person_name") or "").strip()
    if not mention_name or not slot_name:
        return {"score": 0.0}

    exact_name_match = mention_name == slot_name
    surname_bridge = bool(
        mention_name != slot_name
        and (slot_name.startswith(mention_name) or mention_name.startswith(slot_name) or partial_person_name_match(mention_name, slot_name))
    )
    if not exact_name_match and not surname_bridge:
        return {"score": 0.0}

    mention_department = (mention.get("raw_department_name") or "").strip()
    mention_department_id = int(mention["department_id"]) if mention.get("department_id") else None
    slot_department = (slot.get("raw_department_name") or "").strip()
    slot_department_id = int(slot["department_id"]) if slot.get("department_id") else None
    mention_top_unit = resolve_department_hierarchy(mention_department).get("top_unit", "")
    slot_top_unit = resolve_department_hierarchy(slot_department).get("top_unit", "")
    mention_policy_topic_names = normalized_topic_name_set(mention.get("policy_topic_names") or "")
    mention_priority_topic_names = normalized_topic_name_set(mention.get("priority_policy_topic_names") or "")
    slot_policy_topic_names = normalized_topic_name_set(slot.get("person_policy_topic_names") or "")
    slot_priority_topic_names = normalized_topic_name_set(slot.get("person_priority_policy_topic_names") or "")
    mention_year = extract_year_from_date_text(
        mention.get("project_published_at", "") or mention.get("published_at", "") or mention.get("project_fetched_at", "") or ""
    )
    slot_topic_years: dict[str, set[int]] = {}
    for item in split_topic_name_field(slot.get("person_policy_topic_years") or ""):
        topic_name, _, year_text = item.partition("@")
        normalized_name = normalize_topic_name(topic_name)
        try:
            topic_year = int(year_text)
        except ValueError:
            topic_year = 0
        if normalized_name and topic_year > 0:
            slot_topic_years.setdefault(normalized_name, set()).add(topic_year)
    theme_match = department_theme_overlap(mention_department, slot_department)
    project_theme_match = bool(
        project_theme_tokens(
            mention.get("project_title", "") or "",
            mention.get("project_summary", "") or "",
            mention.get("project_purpose", "") or "",
        )
        & department_theme_tokens(slot_department)
    )
    policy_topic_match = bool(
        mention_policy_topic_names
        and slot_policy_topic_names
        and bool(mention_policy_topic_names & slot_policy_topic_names)
    )
    priority_policy_topic_match = bool(
        mention_priority_topic_names
        and slot_priority_topic_names
        and bool(mention_priority_topic_names & slot_priority_topic_names)
    )
    policy_topic_recent_match = has_recent_topic_year_match(
        mention_policy_topic_names,
        mention_year,
        slot_topic_years,
    )
    department_exact = bool(
        (mention_department_id and slot_department_id and mention_department_id == slot_department_id)
        or (
            mention_department
            and slot_department
            and normalize_department_for_match(mention_department) == normalize_department_for_match(slot_department)
        )
    )
    department_overlap = bool(not department_exact and mention_department and slot_department and department_match_overlap(mention_department, slot_department))

    mention_date = coalesce_row_date(
        mention.get("project_published_at", "") or "",
        mention.get("published_at", "") or "",
        mention.get("project_fetched_at", "") or "",
        mention.get("fetched_at", "") or "",
    )
    slot_start = parse_iso_date(slot.get("active_from") or "")
    slot_end = parse_iso_date(slot.get("active_to") or "")
    active_match = bool(mention_date and slot_start and mention_date >= slot_start and (slot_end is None or mention_date <= slot_end))
    near_start = bool(mention_date and slot_start and mention_date < slot_start and (slot_start - mention_date).days <= 120)
    near_end = bool(mention_date and slot_end and mention_date > slot_end and (mention_date - slot_end).days <= 120)

    score = 0.05
    reasons: list[str] = []
    if exact_name_match:
        score += 0.38
        reasons.append("full-name match")
    elif surname_bridge:
        score += 0.18
        reasons.append("surname bridge")

    if department_exact:
        score += 0.28
        reasons.append("department exact")
    elif department_overlap:
        score += 0.18
        reasons.append("department overlap")
    elif theme_match:
        score += 0.08
        reasons.append("department theme")
    elif project_theme_match:
        score += 0.06
        reasons.append("project theme")
    if policy_topic_match:
        score += 0.13
        reasons.append("policy topic")
    if policy_topic_recent_match:
        score += 0.08
        reasons.append("policy topic recency")
    if priority_policy_topic_match:
        score += 0.04
        reasons.append("priority topic")

    if active_match:
        score += 0.24
        reasons.append("active window")
    elif near_start:
        score += 0.14
        reasons.append("near transfer start")
    elif near_end:
        score += 0.10
        reasons.append("near transfer end")

    if slot.get("person_id"):
        score += 0.05
        reasons.append("linked person")

    score += min(float(mention.get("source_confidence") or 0) * 0.05, 0.05)

    if mention.get("name_quality") == "surname_only" and not (department_exact or department_overlap):
        score -= 0.12
    if mention.get("name_quality") == "surname_only" and not (active_match or near_start or near_end):
        score -= 0.08
    if surname_bridge and not (department_exact or department_overlap or theme_match or project_theme_match):
        score -= 0.18
    if surname_bridge and mention_policy_topic_names and slot_policy_topic_names and not policy_topic_match:
        score -= 0.16
    if (
        surname_bridge
        and mention_top_unit
        and slot_top_unit
        and mention_top_unit != "未分類"
        and slot_top_unit != "未分類"
        and mention_top_unit != slot_top_unit
    ):
        score -= 0.12

    if exact_name_match and department_exact and active_match:
        matched_by = "slot_exact"
    elif surname_bridge and department_exact and (active_match or near_start or near_end):
        matched_by = "slot_surname_department_time"
    elif surname_bridge and (theme_match or project_theme_match) and (active_match or near_start or near_end):
        matched_by = "slot_surname_theme_time"
    elif exact_name_match and department_overlap:
        matched_by = "slot_exact_overlap"
    else:
        matched_by = "slot_candidate"

    return {
        "score": round(max(min(score, 0.99), 0), 2),
        "exact_name_match": exact_name_match,
        "surname_bridge": surname_bridge,
        "department_exact": department_exact,
        "department_overlap": department_overlap,
        "department_theme_match": theme_match,
        "project_theme_match": project_theme_match,
        "policy_topic_match": policy_topic_match,
        "policy_topic_recent_match": policy_topic_recent_match,
        "priority_policy_topic_match": priority_policy_topic_match,
        "active_match": active_match,
        "near_start": near_start,
        "near_end": near_end,
        "matched_by": matched_by,
        "notes": ", ".join(reasons),
    }


def upsert_slot_candidate(
    conn: sqlite3.Connection,
    mention_id: int,
    employee_slot_id: int,
    candidate_score: float,
    matched_by: str,
    notes: str = "",
) -> None:
    conn.execute(
        """
        INSERT INTO slot_candidates (
            person_mention_id, employee_slot_id, candidate_score, matched_by, notes, updated_at
        )
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(person_mention_id, employee_slot_id) DO UPDATE SET
            candidate_score = excluded.candidate_score,
            matched_by = excluded.matched_by,
            notes = excluded.notes,
            updated_at = CURRENT_TIMESTAMP
        """,
        (mention_id, employee_slot_id, candidate_score, matched_by, notes),
    )


def refresh_slot_candidates(conn: sqlite3.Connection, project_id: int | None = None) -> int:
    query = """
        WITH project_topic_names AS (
            SELECT
                ptl.project_id,
                GROUP_CONCAT(DISTINCT pt.name) AS policy_topic_names,
                GROUP_CONCAT(DISTINCT CASE WHEN ptl.is_priority = 1 THEN pt.name END) AS priority_policy_topic_names
            FROM project_topic_links ptl
            JOIN policy_topics pt ON pt.id = ptl.topic_id
            GROUP BY ptl.project_id
        )
        SELECT
            pm.*,
            p.published_at AS project_published_at,
            p.fetched_at AS project_fetched_at,
            p.title AS project_title,
            p.summary AS project_summary,
            p.purpose AS project_purpose,
            ptn.policy_topic_names,
            ptn.priority_policy_topic_names
        FROM person_mentions pm
        JOIN projects p ON p.id = pm.project_id
        LEFT JOIN project_topic_names ptn ON ptn.project_id = pm.project_id
        WHERE IFNULL(TRIM(pm.normalized_person_name), '') != ''
          AND IFNULL(TRIM(pm.raw_person_name), '') != ''
    """
    params: list[Any] = []
    if project_id is not None:
        query += " AND pm.project_id = ?"
        params.append(project_id)
    rows = conn.execute(query, params).fetchall()

    mention_ids = [int(row["id"]) for row in rows]
    if mention_ids:
        conn.executemany("DELETE FROM slot_candidates WHERE person_mention_id = ?", [(mention_id,) for mention_id in mention_ids])

    inserted = 0
    for row in rows:
        mention_name = (row["normalized_person_name"] or "").strip()
        slot_rows = conn.execute(
            """
            SELECT
                es.*,
                pe.person_key,
                pe.display_name AS linked_person_name,
                te.effective_date AS source_effective_date,
                ts.title AS source_title,
                ts.url AS source_url,
                tp.person_policy_topic_names,
                tp.person_priority_policy_topic_names,
                tp.person_policy_topic_years
            FROM employee_slots es
            LEFT JOIN people pe ON pe.id = es.person_id
            LEFT JOIN transfer_events te ON te.id = es.source_transfer_event_id
            LEFT JOIN transfer_sources ts ON ts.id = te.transfer_source_id
            LEFT JOIN (
                SELECT
                    ptr.person_id,
                    GROUP_CONCAT(DISTINCT pt.name) AS person_policy_topic_names,
                    GROUP_CONCAT(DISTINCT CASE WHEN ptr.priority_project_count > 0 THEN pt.name END) AS person_priority_policy_topic_names,
                    GROUP_CONCAT(DISTINCT pt.name || '@' || ptr.topic_year) AS person_policy_topic_years
                FROM person_topic_rollups ptr
                JOIN policy_topics pt ON pt.id = ptr.topic_id
                GROUP BY ptr.person_id
            ) tp ON tp.person_id = es.person_id
            WHERE es.normalized_person_name = ?
               OR es.normalized_person_name LIKE ?
               OR ? LIKE es.normalized_person_name || '%'
            ORDER BY COALESCE(NULLIF(es.active_to, ''), '9999-12-31') DESC,
                     COALESCE(NULLIF(es.active_from, ''), '0000-00-00') DESC,
                     es.id DESC
            """,
            (mention_name, f"{mention_name}%", mention_name),
        ).fetchall()

        scored: list[tuple[sqlite3.Row, dict[str, Any]]] = []
        for slot in slot_rows:
            metrics = score_slot_candidate(row, slot)
            if metrics["score"] < 0.42:
                continue
            scored.append((slot, metrics))

        scored.sort(
            key=lambda item: (
                item[1]["score"],
                1 if item[1].get("department_exact") else 0,
                1 if item[1].get("active_match") else 0,
                item[0]["active_from"] or "",
                item[0]["display_name"] or "",
            ),
            reverse=True,
        )

        for slot, metrics in scored[:5]:
            upsert_slot_candidate(
                conn,
                int(row["id"]),
                int(slot["id"]),
                metrics["score"],
                metrics["matched_by"],
                metrics["notes"],
            )
            inserted += 1

    return inserted


def fetch_slot_candidates_for_mention(conn: sqlite3.Connection, mention_id: int, limit: int = 5) -> list[dict[str, Any]]:
    mention = conn.execute(
        """
        WITH project_topic_names AS (
            SELECT
                ptl.project_id,
                GROUP_CONCAT(DISTINCT pt.name) AS policy_topic_names,
                GROUP_CONCAT(DISTINCT CASE WHEN ptl.is_priority = 1 THEN pt.name END) AS priority_policy_topic_names
            FROM project_topic_links ptl
            JOIN policy_topics pt ON pt.id = ptl.topic_id
            GROUP BY ptl.project_id
        )
        SELECT
            pm.*,
            p.published_at AS project_published_at,
            p.fetched_at AS project_fetched_at,
            p.title AS project_title,
            p.summary AS project_summary,
            p.purpose AS project_purpose,
            ptn.policy_topic_names,
            ptn.priority_policy_topic_names
        FROM person_mentions pm
        JOIN projects p ON p.id = pm.project_id
        LEFT JOIN project_topic_names ptn ON ptn.project_id = pm.project_id
        WHERE pm.id = ?
        """,
        (mention_id,),
    ).fetchone()
    if not mention:
        return []
    mention_payload = dict(mention)

    rows = conn.execute(
        """
        WITH person_topic_names AS (
            SELECT
                ptr.person_id,
                GROUP_CONCAT(DISTINCT pt.name) AS person_policy_topic_names,
                GROUP_CONCAT(DISTINCT CASE WHEN ptr.priority_project_count > 0 THEN pt.name END) AS person_priority_policy_topic_names,
                GROUP_CONCAT(DISTINCT pt.name || '@' || ptr.topic_year) AS person_policy_topic_years
            FROM person_topic_rollups ptr
            JOIN policy_topics pt ON pt.id = ptr.topic_id
            GROUP BY ptr.person_id
        )
        SELECT
            sc.candidate_score,
            sc.matched_by,
            sc.notes,
            es.id AS employee_slot_id,
            es.display_name,
            es.name_quality,
            es.raw_department_name,
            es.title_raw,
            es.active_from,
            es.active_to,
            es.person_id,
            pe.person_key,
            pe.display_name AS linked_person_name,
            te.effective_date AS source_effective_date,
            ts.title AS source_title,
            ts.url AS source_url,
            ptn.person_policy_topic_names,
            ptn.person_priority_policy_topic_names,
            ptn.person_policy_topic_years
        FROM slot_candidates sc
        JOIN employee_slots es ON es.id = sc.employee_slot_id
        LEFT JOIN people pe ON pe.id = es.person_id
        LEFT JOIN transfer_events te ON te.id = es.source_transfer_event_id
        LEFT JOIN transfer_sources ts ON ts.id = te.transfer_source_id
        LEFT JOIN person_topic_names ptn ON ptn.person_id = es.person_id
        WHERE sc.person_mention_id = ?
        ORDER BY sc.candidate_score DESC, COALESCE(NULLIF(es.active_from, ''), '0000-00-00') DESC, es.id DESC
        LIMIT ?
        """,
        (mention_id, limit),
    ).fetchall()
    payload: list[dict[str, Any]] = []
    for row in rows:
        metrics = score_slot_candidate(mention, row)
        item = dict(row)
        item["match_labels"] = slot_match_labels(metrics, row)
        item["shared_policy_topics"] = shared_topic_display_names(
            mention_payload.get("policy_topic_names") or "",
            normalized_topic_name_set(item.get("person_policy_topic_names") or ""),
        )
        item["shared_priority_topics"] = shared_topic_display_names(
            mention_payload.get("priority_policy_topic_names") or "",
            normalized_topic_name_set(item.get("person_priority_policy_topic_names") or ""),
        )
        item["policy_topic_match"] = metrics.get("policy_topic_match", False)
        item["policy_topic_recent_match"] = metrics.get("policy_topic_recent_match", False)
        item["priority_policy_topic_match"] = metrics.get("priority_policy_topic_match", False)
        payload.append(item)
    return payload


def slot_timeline_transition_bonus(
    previous_mention: dict[str, Any],
    current_mention: dict[str, Any],
    previous: dict[str, Any] | None,
    current: dict[str, Any],
    gap_days: int | None,
) -> float:
    if not previous or not previous.get("employee_slot_id") or not current.get("employee_slot_id"):
        return 0.0

    bonus = 0.0
    if mentions_distinct_same_year_departments(previous_mention, current_mention):
        same_slot = previous["employee_slot_id"] == current["employee_slot_id"]
        same_person = bool(
            previous.get("person_id")
            and current.get("person_id")
            and previous.get("person_id") == current.get("person_id")
        )
        if same_slot or same_person:
            bonus -= 0.28
        elif not department_match_overlap(
            previous.get("raw_department_name", ""),
            current.get("raw_department_name", ""),
        ):
            bonus += 0.04

    if previous["employee_slot_id"] == current["employee_slot_id"]:
        if gap_days is None or gap_days <= 180:
            bonus += 0.12
        elif gap_days <= 365:
            bonus += 0.06
    elif previous.get("person_id") and current.get("person_id") and previous.get("person_id") == current.get("person_id"):
        if gap_days is None or gap_days <= 365:
            bonus += 0.08
    elif (
        previous.get("raw_department_name")
        and current.get("raw_department_name")
        and department_match_overlap(previous.get("raw_department_name", ""), current.get("raw_department_name", ""))
        and gap_days is not None
        and gap_days <= 60
    ):
        bonus -= 0.04
    return bonus


def compute_slot_timeline_recommendations(
    conn: sqlite3.Connection,
    mention_ids: list[int] | None = None,
) -> dict[int, dict[str, Any]]:
    mention_query = """
        SELECT pm.id, pm.normalized_person_name, pm.raw_person_name, pm.raw_department_name, pm.name_quality,
               p.published_at AS project_published_at, p.fetched_at AS project_fetched_at
        FROM person_mentions pm
        JOIN projects p ON p.id = pm.project_id
        WHERE IFNULL(TRIM(pm.normalized_person_name), '') != ''
    """
    params: list[Any] = []
    if mention_ids:
        placeholders = ",".join("?" for _ in mention_ids)
        mention_query += f" AND pm.id IN ({placeholders})"
        params.extend(mention_ids)

    mention_rows = conn.execute(mention_query, params).fetchall()
    if not mention_rows:
        return {}

    grouped_mentions: dict[str, list[dict[str, Any]]] = {}
    candidate_map: dict[int, list[dict[str, Any]]] = {}
    for row in mention_rows:
        mention = dict(row)
        mention["observed_date"] = coalesce_row_date(mention.get("project_published_at", "") or "", mention.get("project_fetched_at", "") or "")
        mention["observed_sort"] = mention["observed_date"].isoformat() if mention["observed_date"] else ""
        grouped_mentions.setdefault(mention["normalized_person_name"], []).append(mention)
        candidate_map[int(mention["id"])] = fetch_slot_candidates_for_mention(conn, int(mention["id"]), limit=5)

    recommendations: dict[int, dict[str, Any]] = {}
    for mentions in grouped_mentions.values():
        ordered_mentions = sorted(
            mentions,
            key=lambda item: (
                item["observed_sort"],
                normalize_department_for_match(item.get("raw_department_name", "")),
                int(item["id"]),
            ),
        )
        if not ordered_mentions:
            continue

        states_by_step: list[list[dict[str, Any]]] = []
        for mention in ordered_mentions:
            states = [
                {
                    **candidate,
                    "total_score": float(candidate.get("candidate_score") or 0),
                    "previous_index": None,
                }
                for candidate in candidate_map.get(int(mention["id"]), [])
            ]
            if mention.get("name_quality") == "surname_only":
                states = [
                    *states,
                    {
                        "employee_slot_id": None,
                        "candidate_score": 0.14,
                        "matched_by": "timeline_hold_distinct",
                        "notes": "Same-year cross-department surname matches stay unlinked until roster evidence is strong",
                        "display_name": "",
                        "raw_department_name": mention.get("raw_department_name", "") or "",
                        "person_id": None,
                        "active_from": "",
                        "active_to": "",
                        "total_score": 0.14,
                        "previous_index": None,
                    }
                ]
            elif not states:
                states = [
                    {
                        "employee_slot_id": None,
                        "candidate_score": 0.0,
                        "matched_by": "no_slot_candidate",
                        "notes": "",
                        "display_name": "",
                        "raw_department_name": mention.get("raw_department_name", "") or "",
                        "person_id": None,
                        "active_from": "",
                        "active_to": "",
                        "total_score": 0.0,
                        "previous_index": None,
                    }
                ]
            states_by_step.append(states)

        for step in range(1, len(states_by_step)):
            current_mention = ordered_mentions[step]
            previous_mention = ordered_mentions[step - 1]
            gap_days = days_between_dates(current_mention.get("observed_date"), previous_mention.get("observed_date"))
            previous_states = states_by_step[step - 1]
            for current_index, current_state in enumerate(states_by_step[step]):
                best_total: float | None = None
                best_prev_index: int | None = None
                for prev_index, previous_state in enumerate(previous_states):
                    transition = slot_timeline_transition_bonus(
                        previous_mention,
                        current_mention,
                        previous_state,
                        current_state,
                        gap_days,
                    )
                    candidate_total = float(previous_state["total_score"]) + float(current_state.get("candidate_score") or 0) + transition
                    if best_total is None or candidate_total > best_total:
                        best_total = candidate_total
                        best_prev_index = prev_index
                states_by_step[step][current_index]["total_score"] = best_total if best_total is not None else float(current_state.get("candidate_score") or 0)
                states_by_step[step][current_index]["previous_index"] = best_prev_index

        last_states = states_by_step[-1]
        best_last_index = max(
            range(len(last_states)),
            key=lambda index: (
                float(last_states[index]["total_score"]),
                float(last_states[index].get("candidate_score") or 0),
            ),
        )

        selected_indices: list[int | None] = [None] * len(states_by_step)
        current_index: int | None = best_last_index
        for step in range(len(states_by_step) - 1, -1, -1):
            selected_indices[step] = current_index
            if current_index is None:
                break
            current_index = states_by_step[step][current_index].get("previous_index")

        for step_index, (mention, selected_index) in enumerate(zip(ordered_mentions, selected_indices)):
            if selected_index is None:
                continue
            selected_state = states_by_step[step_index][selected_index]
            if not selected_state.get("employee_slot_id"):
                continue
            recommendations[int(mention["id"])] = {
                "employee_slot_id": selected_state.get("employee_slot_id"),
                "candidate_score": float(selected_state.get("candidate_score") or 0),
                "timeline_total_score": round(float(selected_state.get("total_score") or 0), 2),
                "display_name": selected_state.get("display_name") or "",
                "matched_by": selected_state.get("matched_by") or "",
                "notes": selected_state.get("notes") or "",
            }

    return recommendations


def fetch_transfer_candidates_for_project(
    conn: sqlite3.Connection,
    project_row: sqlite3.Row | dict[str, Any],
    appearance_row: sqlite3.Row | dict[str, Any] | None,
    limit: int = 5,
) -> list[dict[str, Any]]:
    project = dict(project_row)
    appearance = dict(appearance_row) if appearance_row else {}

    target_department_id = appearance.get("department_id")
    target_department_name = (appearance.get("raw_department_name") or "").strip()
    if not target_department_id:
        source_department_name = (project.get("source_department_name") or "").strip()
        if source_department_name:
            target_department_id = resolve_department_reference_id(conn, source_department_name)
            target_department_name = target_department_name or source_department_name
    if not target_department_id:
        return []

    target_date = coalesce_row_date(project.get("published_at", "") or "", project.get("fetched_at", "") or "")
    rows = conn.execute(
        """
        SELECT
            te.id AS transfer_event_id,
            te.effective_date,
            te.from_department_raw,
            te.to_department_raw,
            te.from_title_raw,
            te.to_title_raw,
            ts.source_type,
            ts.title AS source_title,
            ts.url AS source_url,
            ts.published_at AS source_published_at,
            til.confidence,
            til.link_status,
            pe.id AS person_id,
            pe.person_key,
            pe.display_name,
            CASE WHEN te.to_department_id = ? THEN 1 ELSE 0 END AS to_department_match,
            CASE WHEN te.from_department_id = ? THEN 1 ELSE 0 END AS from_department_match
        FROM transfer_events te
        JOIN transfer_identity_links til ON til.transfer_event_id = te.id
        JOIN people pe ON pe.id = til.person_id
        JOIN transfer_sources ts ON ts.id = te.transfer_source_id
        WHERE til.link_status IN ('reviewed_match', 'auto_matched')
          AND (te.to_department_id = ? OR te.from_department_id = ?)
        ORDER BY COALESCE(NULLIF(te.effective_date, ''), NULLIF(ts.published_at, ''), '0000-00-00') DESC,
                 te.id DESC
        """,
        (target_department_id, target_department_id, target_department_id, target_department_id),
    ).fetchall()

    best_by_person: dict[int, dict[str, Any]] = {}
    for row in rows:
        if not is_valid_person_name(row["display_name"]):
            continue
        event_date = coalesce_row_date(row["effective_date"] or "", row["source_published_at"] or "")
        days_delta = days_between_dates(target_date, event_date)
        if target_date and days_delta is not None and days_delta > 540:
            continue

        match_type = "to_department" if row["to_department_match"] else "from_department"
        recency_bonus = 0.0
        if days_delta is not None:
            if days_delta <= 90:
                recency_bonus = 0.25
            elif days_delta <= 180:
                recency_bonus = 0.15
            elif days_delta <= 365:
                recency_bonus = 0.08

        score = 0.25 + float(row["confidence"] or 0) * 0.35
        if row["to_department_match"]:
            score += 0.25
        if row["from_department_match"]:
            score += 0.10
        score += recency_bonus

        item = {
            "transfer_event_id": int(row["transfer_event_id"]),
            "person_id": int(row["person_id"]),
            "person_key": row["person_key"],
            "display_name": row["display_name"],
            "effective_date": row["effective_date"],
            "from_department_raw": row["from_department_raw"],
            "to_department_raw": row["to_department_raw"],
            "from_title_raw": row["from_title_raw"],
            "to_title_raw": row["to_title_raw"],
            "source_type": row["source_type"],
            "source_title": row["source_title"],
            "source_url": row["source_url"],
            "link_status": row["link_status"],
            "confidence": float(row["confidence"] or 0),
            "days_delta": days_delta,
            "match_type": match_type,
            "candidate_score": round(min(score, 0.99), 2),
            "target_department_name": target_department_name,
        }
        current = best_by_person.get(item["person_id"])
        if not current or item["candidate_score"] > current["candidate_score"]:
            best_by_person[item["person_id"]] = item

    return sorted(
        best_by_person.values(),
        key=lambda item: (
            item["candidate_score"],
            1 if item["match_type"] == "to_department" else 0,
            -item["days_delta"] if item["days_delta"] is not None else -9999,
            item["display_name"],
        ),
        reverse=True,
    )[:limit]


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
        WITH project_topic_names AS (
            SELECT
                ptl.project_id,
                GROUP_CONCAT(DISTINCT pt.name) AS policy_topic_names,
                GROUP_CONCAT(DISTINCT CASE WHEN ptl.is_priority = 1 THEN pt.name END) AS priority_policy_topic_names
            FROM project_topic_links ptl
            JOIN policy_topics pt ON pt.id = ptl.topic_id
            GROUP BY ptl.project_id
        )
        SELECT
            pm.*,
            p.published_at AS project_published_at,
            p.fetched_at AS project_fetched_at,
            p.title AS project_title,
            p.summary AS project_summary,
            p.purpose AS project_purpose,
            ptn.policy_topic_names,
            ptn.priority_policy_topic_names
        FROM person_mentions pm
        JOIN projects p ON p.id = pm.project_id
        LEFT JOIN project_topic_names ptn ON ptn.project_id = pm.project_id
        WHERE pm.id = ?
        """,
        (mention_id,),
    ).fetchone()
    if not mention or not (mention["normalized_person_name"] or "").strip():
        return []
    mention_payload = dict(mention)

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
        match_labels = identity_match_labels(metrics)
        payload.append(
            {
                **dict(person),
                "candidate_score": metrics["score"],
                "contact_match": metrics["contact_match"],
                "department_match": metrics["department_match"],
                "department_theme_match": metrics["department_theme_match"],
                "project_theme_match": metrics["project_theme_match"],
                "policy_topic_match": metrics["policy_topic_match"],
                "policy_topic_recent_match": metrics["policy_topic_recent_match"],
                "priority_policy_topic_match": metrics["priority_policy_topic_match"],
                "transfer_match": metrics["transfer_match"],
                "transfer_recent_match": metrics["transfer_recent_match"],
                "match_labels": match_labels,
                "shared_policy_topics": shared_topic_display_names(
                    mention_payload.get("policy_topic_names") or "",
                    context.get("policy_topic_names", set()),
                ),
                "shared_priority_topics": shared_topic_display_names(
                    mention_payload.get("priority_policy_topic_names") or "",
                    context.get("priority_policy_topic_names", set()),
                ),
            }
        )
    payload.sort(
        key=lambda item: (
            item["candidate_score"],
            1 if item["transfer_recent_match"] else 0,
            1 if item["transfer_match"] else 0,
            1 if item["policy_topic_recent_match"] else 0,
            1 if item["policy_topic_match"] else 0,
            1 if item["contact_match"] else 0,
            item["project_count"],
            item["display_name"],
        ),
        reverse=True,
    )
    return payload[:limit]


def fetch_pending_identity_reviews(limit: int = 100, db_path: Path | str = DB_PATH) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        rows = conn.execute(
            """
            WITH project_topic_names AS (
                SELECT
                    ptl.project_id,
                    GROUP_CONCAT(DISTINCT pt.name) AS policy_topic_names,
                    GROUP_CONCAT(DISTINCT CASE WHEN ptl.is_priority = 1 THEN pt.name END) AS priority_policy_topic_names
                FROM project_topic_links ptl
                JOIN policy_topics pt ON pt.id = ptl.topic_id
                GROUP BY ptl.project_id
            )
            SELECT
                pm.*,
                p.title,
                p.url,
                p.published_at,
                p.source_type,
                ptn.policy_topic_names,
                ptn.priority_policy_topic_names,
                pil.person_id,
                pil.link_status,
                pil.confidence,
                pil.matched_by,
                pe.display_name AS linked_person_name,
                pe.person_key AS linked_person_key
            FROM person_mentions pm
            JOIN projects p ON p.id = pm.project_id
            LEFT JOIN project_topic_names ptn ON ptn.project_id = pm.project_id
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

        recommendations = compute_slot_timeline_recommendations(conn, [int(row["id"]) for row in rows])
        payload: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["policy_topics"] = split_topic_name_field(
                item.get("priority_policy_topic_names") or item.get("policy_topic_names") or ""
            )
            item["candidates"] = fetch_identity_candidates_for_mention(conn, int(row["id"]))
            slot_candidates = fetch_slot_candidates_for_mention(conn, int(row["id"]))
            recommendation = recommendations.get(int(row["id"]))
            for candidate in slot_candidates:
                candidate["timeline_recommended"] = bool(
                    recommendation
                    and recommendation.get("employee_slot_id")
                    and candidate.get("employee_slot_id") == recommendation.get("employee_slot_id")
                )
                if candidate["timeline_recommended"]:
                    candidate["timeline_total_score"] = recommendation.get("timeline_total_score", 0)
            item["slot_candidates"] = slot_candidates
            item["timeline_recommendation"] = recommendation or {}
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
        refresh_transfer_links_for_person(conn, person_id)
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
        refresh_transfer_links_for_person(conn, person_id)
        conn.commit()
        return person_id
    finally:
        conn.close()


def ensure_person_for_employee_slot(conn: sqlite3.Connection, employee_slot_id: int) -> int | None:
    slot = conn.execute("SELECT * FROM employee_slots WHERE id = ?", (employee_slot_id,)).fetchone()
    if not slot:
        return None
    if slot["person_id"]:
        return int(slot["person_id"])

    display_name = (slot["display_name"] or "").strip()
    if not display_name or not is_valid_person_name(display_name):
        return None

    person_id = create_distinct_person(conn, display_name, build_person_key(display_name))
    if not person_id:
        return None

    conn.execute(
        """
        UPDATE employee_slots
        SET person_id = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """,
        (person_id, employee_slot_id),
    )
    return person_id


def link_person_mention_to_timeline_recommendation(
    mention_id: int,
    employee_slot_id: int,
    db_path: Path | str = DB_PATH,
) -> int | None:
    conn = get_connection(db_path)
    try:
        mention = conn.execute("SELECT project_id FROM person_mentions WHERE id = ?", (mention_id,)).fetchone()
        slot = conn.execute("SELECT * FROM employee_slots WHERE id = ?", (employee_slot_id,)).fetchone()
        if not mention or not slot:
            return None

        person_id = ensure_person_for_employee_slot(conn, employee_slot_id)
        if not person_id:
            return None

        upsert_person_identity_link(
            conn,
            mention_id,
            person_id,
            "reviewed_match",
            0.98,
            "timeline_recommendation",
            notes=f"Adopted employee slot #{employee_slot_id}",
        )

        if slot["source_transfer_event_id"]:
            upsert_transfer_identity_link(
                conn,
                int(slot["source_transfer_event_id"]),
                person_id,
                "reviewed_match",
                max(float(slot["slot_confidence"] or 0), 0.9),
                "timeline_recommendation",
                notes=f"Confirmed from employee slot #{employee_slot_id}",
            )

        refresh_project_appearance_from_mentions(conn, int(mention["project_id"]))
        refresh_transfer_links_for_person(conn, person_id)
        if slot["source_transfer_event_id"]:
            rebuild_employee_slots(conn)
        else:
            conn.execute(
                """
                UPDATE employee_slots
                SET person_id = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (person_id, employee_slot_id),
            )
        refresh_slot_candidates(conn)
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
        refresh_transfer_links_for_person(conn, person_id)
        refresh_slot_candidates(conn, project_id)

        conn.commit()
    finally:
        conn.close()

def merge_people(primary_id: int, secondary_id: int) -> None:
    conn = get_connection()
    try:
        conn.execute("UPDATE appearances SET person_id = ? WHERE person_id = ?", (primary_id, secondary_id))
        conn.execute("UPDATE person_identity_links SET person_id = ?, updated_at = CURRENT_TIMESTAMP WHERE person_id = ?", (primary_id, secondary_id))
        conn.execute(
            "UPDATE transfer_identity_links SET person_id = ?, updated_at = CURRENT_TIMESTAMP WHERE person_id = ?",
            (primary_id, secondary_id),
        )
        conn.execute("UPDATE employee_slots SET person_id = ? WHERE person_id = ?", (primary_id, secondary_id))
        conn.execute("DELETE FROM people WHERE id = ?", (secondary_id,))
        refresh_transfer_links_for_person(conn, primary_id)
        rebuild_employee_slots(conn)
        refresh_slot_candidates(conn)
        conn.commit()
    finally:
        conn.close()

def merge_departments(primary_id: int, secondary_id: int) -> None:
    conn = get_connection()
    try:
        for row in conn.execute(
            "SELECT alias_name, alias_type FROM department_aliases WHERE department_id = ? ORDER BY id ASC",
            (secondary_id,),
        ).fetchall():
            upsert_department_alias(conn, primary_id, row["alias_name"], row["alias_type"] or "observed")
        conn.execute("DELETE FROM department_aliases WHERE department_id = ?", (secondary_id,))
        conn.execute("UPDATE appearances SET department_id = ? WHERE department_id = ?", (primary_id, secondary_id))
        conn.execute("UPDATE person_mentions SET department_id = ? WHERE department_id = ?", (primary_id, secondary_id))
        conn.execute("UPDATE transfer_events SET from_department_id = ? WHERE from_department_id = ?", (primary_id, secondary_id))
        conn.execute("UPDATE transfer_events SET to_department_id = ? WHERE to_department_id = ?", (primary_id, secondary_id))
        conn.execute("UPDATE employee_slots SET department_id = ? WHERE department_id = ?", (primary_id, secondary_id))
        conn.execute("DELETE FROM departments WHERE id = ?", (secondary_id,))
        rebuild_employee_slots(conn)
        refresh_slot_candidates(conn)
        conn.commit()
    finally:
        conn.close()

def fetch_departments(db_path: Path | str = DB_PATH) -> list[sqlite3.Row]:
    conn = get_connection(db_path)
    try:
        return conn.execute(
            """
            SELECT d.*, COUNT(DISTINCT a.id) as appearance_count, COUNT(DISTINCT pm.id) as mention_count
            FROM departments d 
            LEFT JOIN appearances a ON a.department_id = d.id 
            LEFT JOIN person_mentions pm ON pm.department_id = d.id
            GROUP BY d.id 
            HAVING COUNT(DISTINCT a.id) > 0 OR COUNT(DISTINCT pm.id) > 0
            ORDER BY d.name
            """
        ).fetchall()
    finally:
        conn.close()

def save_interview(person_id: int, title: str, content: str, project_id: int | None = None, db_path: Path | str = DB_PATH) -> int:
    conn = get_connection(db_path)
    try:
        cursor = conn.execute(
            """
            INSERT INTO interviews (person_id, project_id, title, content)
            VALUES (?, ?, ?, ?)
            """,
            (person_id, project_id, title, content)
        )
        conn.commit()
        return cursor.lastrowid
    finally:
        conn.close()

def fetch_interview(interview_id: int, db_path: Path | str = DB_PATH) -> dict[str, Any] | None:
    conn = get_connection(db_path)
    try:
        row = conn.execute(
            """
            SELECT i.*, p.display_name AS person_name, pr.title AS project_title
            FROM interviews i
            LEFT JOIN people p ON p.id = i.person_id
            LEFT JOIN projects pr ON pr.id = i.project_id
            WHERE i.id = ?
            """,
            (interview_id,)
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()

def fetch_interviews(db_path: Path | str = DB_PATH) -> list[dict[str, Any]]:
    conn = get_connection(db_path)
    try:
        rows = conn.execute(
            """
            SELECT i.*, p.display_name AS person_name, pr.title AS project_title
            FROM interviews i
            LEFT JOIN people p ON p.id = i.person_id
            LEFT JOIN projects pr ON pr.id = i.project_id
            ORDER BY i.published_at DESC
            """
        ).fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()

def fetch_department_detail(department_id: int, db_path: Path | str = DB_PATH) -> dict[str, Any] | None:
    conn = get_connection(db_path)
    try:
        dept = conn.execute("SELECT * FROM departments WHERE id = ?", (department_id,)).fetchone()
        if not dept:
            return None

        aliases = conn.execute("SELECT alias_name FROM department_aliases WHERE department_id = ? ORDER BY alias_name", (department_id,)).fetchall()
        alias_names = [r["alias_name"] for r in aliases]

        projects = conn.execute(
            """
            SELECT p.*, MAX(a.raw_department_name) as raw_department_name
            FROM projects p
            JOIN appearances a ON a.project_id = p.id
            WHERE a.department_id = ?
            GROUP BY p.id
            ORDER BY COALESCE(NULLIF(p.published_at, ''), substr(p.fetched_at, 1, 10)) DESC
            """,
            (department_id,),
        ).fetchall()
        project_ids = [int(r["id"]) for r in projects]

        topic_rollups = []
        if project_ids:
            placeholders = ",".join("?" for _ in project_ids)
            topic_rows = conn.execute(
                f"""
                SELECT 
                    pt.id as topic_id, pt.name as topic_name, pt.topic_year, pt.origin_type,
                    COUNT(DISTINCT ptl.project_id) as project_count,
                    SUM(ptl.is_priority) as priority_count
                FROM project_topic_links ptl
                JOIN policy_topics pt ON pt.id = ptl.topic_id
                WHERE ptl.project_id IN ({placeholders})
                GROUP BY pt.id
                ORDER BY priority_count DESC, project_count DESC, pt.name ASC
                LIMIT 20
                """,
                project_ids
            ).fetchall()
            topic_rollups = [dict(r) for r in topic_rows]

        people = conn.execute(
            """
            SELECT 
                pe.id, pe.person_key, pe.display_name,
                COUNT(DISTINCT a.project_id) as project_count,
                MAX(COALESCE(NULLIF(p.published_at, ''), substr(p.fetched_at, 1, 10))) as latest_project_date
            FROM appearances a
            JOIN people pe ON pe.id = a.person_id
            LEFT JOIN projects p ON p.id = a.project_id
            WHERE a.department_id = ?
            GROUP BY pe.id
            ORDER BY project_count DESC, latest_project_date DESC
            """,
            (department_id,),
        ).fetchall()

        candidate_people_rows = conn.execute(
            """
            SELECT raw_person_name, COUNT(DISTINCT project_id) as project_count
            FROM appearances
            WHERE department_id = ? AND person_id IS NULL AND raw_person_name IS NOT NULL AND raw_person_name != ''
            GROUP BY raw_person_name
            ORDER BY project_count DESC
            LIMIT 30
            """,
            (department_id,),
        ).fetchall()

        return {
            "department": dict(dept),
            "aliases": alias_names,
            "projects": [dict(p) for p in projects],
            "topic_rollups": topic_rollups,
            "people": [dict(p) for p in people],
            "candidate_people": [{"name": r["raw_person_name"], "count": r["project_count"]} for r in candidate_people_rows],
        }
    finally:
        conn.close()
