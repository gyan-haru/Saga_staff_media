from __future__ import annotations

import argparse

from database import (
    DB_PATH,
    cleanup_orphan_people,
    derive_person_mentions_for_project,
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
                source_list_url=row["source_list_url"] or "",
                source_department_name=row["source_department_name"] or "",
            )
            effective_mentions = derive_person_mentions_for_project(conn, row["id"], record)
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
            replace_project_person_mentions(conn, row["id"], effective_record, allow_empty_replace=True)
            replace_project_appearance(conn, row["id"], effective_record, allow_empty_replace=True)
            updated += 1

        conn.commit()
    finally:
        conn.close()
    cleanup_orphan_people(db_path)
    return updated


def rebuild_person_mentions_window(
    db_path: str = str(DB_PATH),
    start_id: int = 1,
    limit: int | None = None,
    commit_every: int = 50,
) -> int:
    init_db(db_path)
    conn = get_connection(db_path)
    updated = 0
    try:
        rows = conn.execute(
            """
            SELECT *
            FROM projects
            WHERE id >= ?
            ORDER BY id ASC
            """,
            (start_id,),
        ).fetchall()
        if limit is not None:
            rows = rows[:limit]

        total = len(rows)
        for index, row in enumerate(rows, start=1):
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
                source_list_url=row["source_list_url"] or "",
                source_department_name=row["source_department_name"] or "",
            )
            effective_mentions = derive_person_mentions_for_project(conn, row["id"], record)
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
            replace_project_person_mentions(conn, row["id"], effective_record, allow_empty_replace=True)
            replace_project_appearance(conn, row["id"], effective_record, allow_empty_replace=True)
            updated += 1

            if commit_every and index % commit_every == 0:
                conn.commit()
                print(f"rebuild progress: {index}/{total} projects", flush=True)

        conn.commit()
    finally:
        conn.close()
    cleanup_orphan_people(db_path)
    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description="保存済み raw_text から person_mentions / appearances を再構築する")
    parser.add_argument("--db", default=str(DB_PATH), help="対象DBパス")
    parser.add_argument("--limit", type=int, default=None, help="処理件数上限")
    parser.add_argument("--start-id", type=int, default=1, help="処理開始 project.id")
    parser.add_argument("--commit-every", type=int, default=50, help="途中コミット間隔")
    args = parser.parse_args()

    if args.start_id != 1 or args.commit_every != 50:
        updated = rebuild_person_mentions_window(
            db_path=args.db,
            start_id=args.start_id,
            limit=args.limit,
            commit_every=args.commit_every,
        )
    else:
        updated = rebuild_person_mentions(db_path=args.db, limit=args.limit)
    print(f"rebuilt person mentions for {updated} projects", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
