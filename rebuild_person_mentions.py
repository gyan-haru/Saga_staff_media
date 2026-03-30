from __future__ import annotations

import argparse

from database import (
    DB_PATH,
    cleanup_orphan_people,
    get_connection,
    init_db,
    replace_project_appearance,
    replace_project_person_mentions,
)
from extractor import ProjectRecord, dedupe_person_mentions, find_person_mentions


def rebuild_person_mentions(db_path: str = str(DB_PATH), limit: int | None = None) -> int:
    init_db(db_path)
    conn = get_connection(db_path)
    updated = 0
    try:
        query = """
            SELECT *
            FROM projects
            ORDER BY id ASC
        """
        rows = conn.execute(query).fetchall()
        if limit is not None:
            rows = rows[:limit]

        for row in rows:
            mentions = dedupe_person_mentions(find_person_mentions(row["raw_text"] or ""))
            record = ProjectRecord(
                title=row["title"],
                url=row["url"],
                source_type=row["source_type"],
                summary=row["summary"] or "",
                purpose=row["purpose"] or "",
                budget=row["budget"] or "",
                application_deadline=row["application_deadline"] or "",
                submission_deadline=row["submission_deadline"] or "",
                published_at=row["published_at"] or "",
                raw_text=row["raw_text"] or "",
                html_text=row["html_text"] or "",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=mentions,
                fetched_at=row["fetched_at"] or "",
            )
            replace_project_person_mentions(conn, row["id"], record, allow_empty_replace=True)
            replace_project_appearance(conn, row["id"], record, allow_empty_replace=True)
            updated += 1

        conn.commit()
    finally:
        conn.close()
    cleanup_orphan_people(db_path)
    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description="保存済み raw_text から person_mentions / appearances を再構築する")
    parser.add_argument("--db", default=str(DB_PATH), help="対象DBパス")
    parser.add_argument("--limit", type=int, default=None, help="処理件数上限")
    args = parser.parse_args()

    updated = rebuild_person_mentions(db_path=args.db, limit=args.limit)
    print(f"rebuilt person mentions for {updated} projects")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
