import sqlite3
import tempfile
import unittest
from pathlib import Path

from database import (
    cleanup_orphan_people,
    create_person_for_mention,
    fetch_identity_candidates_for_mention,
    fetch_people,
    fetch_person_detail,
    get_connection,
    init_db,
    resolve_public_appearance,
    save_project_record,
)
from extractor import PersonMention, ProjectRecord


class DatabaseIdentityTestCase(unittest.TestCase):
    def test_init_db_creates_transfer_history_tables(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            tables = {
                row[0]
                for row in cur.execute(
                    "select name from sqlite_master where type = 'table' and name like 'transfer_%'"
                ).fetchall()
            }
            source_columns = {
                row[1]
                for row in cur.execute("pragma table_info(transfer_sources)").fetchall()
            }
            event_columns = {
                row[1]
                for row in cur.execute("pragma table_info(transfer_events)").fetchall()
            }
            link_columns = {
                row[1]
                for row in cur.execute("pragma table_info(transfer_identity_links)").fetchall()
            }
            conn.close()

            self.assertEqual(
                tables,
                {"transfer_sources", "transfer_events", "transfer_identity_links"},
            )
            self.assertTrue({"source_type", "source_key", "effective_date"}.issubset(source_columns))
            self.assertTrue(
                {"transfer_source_id", "raw_person_name", "to_department_raw", "review_status"}.issubset(event_columns)
            )
            self.assertTrue(
                {"transfer_event_id", "person_id", "link_status", "confidence"}.issubset(link_columns)
            )

    def test_save_project_record_creates_mentions_and_identity_links(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            record = ProjectRecord(
                title="test",
                url="https://example.com/project",
                source_type="press_release",
                summary="summary",
                purpose="purpose",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-03-30",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 政策部 広報広聴課",
                        person_name="川久保",
                        person_key="",
                        person_role="contact",
                        contact_email="kouhou@example.jp",
                        contact_phone="0952-25-7351",
                        extracted_section="担当:川久保、緒方",
                        name_quality="surname_only",
                        source_confidence=0.95,
                    ),
                    PersonMention(
                        department_name="佐賀県 政策部 広報広聴課",
                        person_name="緒方",
                        person_key="",
                        person_role="contact",
                        contact_email="kouhou@example.jp",
                        contact_phone="0952-25-7351",
                        extracted_section="担当:川久保、緒方",
                        name_quality="surname_only",
                        source_confidence=0.95,
                    ),
                ],
                fetched_at="2026-03-30T12:00:00",
            )

            save_project_record(record, db_path)

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            self.assertEqual(cur.execute("select count(*) from person_mentions").fetchone()[0], 2)
            self.assertEqual(cur.execute("select count(*) from person_identity_links").fetchone()[0], 2)
            self.assertEqual(cur.execute("select count(*) from appearances").fetchone()[0], 1)
            conn.close()

    def test_create_person_for_mention_links_manual_person(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = get_connection(db_path)
            conn.execute(
                """
                INSERT INTO projects (title, url, source_type, summary, purpose, budget, application_deadline,
                                      submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                                      zip_urls_json, fetched_at)
                VALUES ('test', 'https://example.com/project-2', 'press_release', '', '', '', '', '', '', '', '', '[]', '[]', '')
                """
            )
            project_id = conn.execute("select id from projects where url = 'https://example.com/project-2'").fetchone()[0]
            conn.execute(
                """
                INSERT INTO person_mentions (
                    project_id, mention_index, raw_department_name, raw_person_name, normalized_person_name,
                    name_quality, role, contact_email, contact_phone, extracted_section, source_confidence, review_status
                )
                VALUES (?, 0, '佐賀県 政策部', '川久保', '川久保', 'surname_only', 'contact', '', '', 'snippet', 0.7, 'pending')
                """,
                (project_id,),
            )
            mention_id = conn.execute("select id from person_mentions where project_id = ?", (project_id,)).fetchone()[0]
            conn.commit()
            conn.close()

            person_id = create_person_for_mention(mention_id, db_path)

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            self.assertIsNotNone(person_id)
            self.assertEqual(cur.execute("select count(*) from people").fetchone()[0], 1)
            self.assertEqual(
                cur.execute("select link_status from person_identity_links where person_mention_id = ?", (mention_id,)).fetchone()[0],
                "reviewed_match",
            )
            conn.close()

    def test_create_person_for_mention_allows_distinct_same_name_people(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = get_connection(db_path)
            conn.execute(
                """
                INSERT INTO projects (title, url, source_type, summary, purpose, budget, application_deadline,
                                      submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                                      zip_urls_json, fetched_at)
                VALUES ('test-1', 'https://example.com/project-a', 'press_release', '', '', '', '', '', '', '', '', '[]', '[]', '')
                """
            )
            project_a = conn.execute("select id from projects where url = 'https://example.com/project-a'").fetchone()[0]
            conn.execute(
                """
                INSERT INTO person_mentions (
                    project_id, mention_index, raw_department_name, raw_person_name, normalized_person_name,
                    name_quality, role, contact_email, contact_phone, extracted_section, source_confidence, review_status
                )
                VALUES (?, 0, '佐賀県 政策部', '川久保', '川久保', 'surname_only', 'contact', '', '', 'snippet-a', 0.7, 'pending')
                """,
                (project_a,),
            )
            mention_a = conn.execute("select id from person_mentions where project_id = ?", (project_a,)).fetchone()[0]

            conn.execute(
                """
                INSERT INTO projects (title, url, source_type, summary, purpose, budget, application_deadline,
                                      submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                                      zip_urls_json, fetched_at)
                VALUES ('test-2', 'https://example.com/project-b', 'press_release', '', '', '', '', '', '', '', '', '[]', '[]', '')
                """
            )
            project_b = conn.execute("select id from projects where url = 'https://example.com/project-b'").fetchone()[0]
            conn.execute(
                """
                INSERT INTO person_mentions (
                    project_id, mention_index, raw_department_name, raw_person_name, normalized_person_name,
                    name_quality, role, contact_email, contact_phone, extracted_section, source_confidence, review_status
                )
                VALUES (?, 0, '佐賀県 地域交流部', '川久保', '川久保', 'surname_only', 'contact', '', '', 'snippet-b', 0.7, 'pending')
                """,
                (project_b,),
            )
            mention_b = conn.execute("select id from person_mentions where project_id = ?", (project_b,)).fetchone()[0]
            conn.commit()
            conn.close()

            person_a = create_person_for_mention(mention_a, db_path)
            person_b = create_person_for_mention(mention_b, db_path)

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            self.assertNotEqual(person_a, person_b)
            self.assertEqual(cur.execute("select count(*) from people").fetchone()[0], 2)
            keys = [row[0] for row in cur.execute("select person_key from people order by id asc").fetchall()]
            self.assertEqual(len(set(keys)), 2)
            conn.close()

    def test_fetch_identity_candidates_prefers_same_contact_match(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = get_connection(db_path)
            for index, url in enumerate(
                [
                    "https://example.com/project-source-a",
                    "https://example.com/project-source-b",
                    "https://example.com/project-pending",
                ],
                start=1,
            ):
                conn.execute(
                    """
                    INSERT INTO projects (title, url, source_type, summary, purpose, budget, application_deadline,
                                          submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                                          zip_urls_json, fetched_at)
                    VALUES (?, ?, 'press_release', '', '', '', '', '', '', '', '', '[]', '[]', '')
                    """,
                    (f"test-{index}", url),
                )

            project_a = conn.execute("select id from projects where url = 'https://example.com/project-source-a'").fetchone()[0]
            project_b = conn.execute("select id from projects where url = 'https://example.com/project-source-b'").fetchone()[0]
            project_pending = conn.execute("select id from projects where url = 'https://example.com/project-pending'").fetchone()[0]

            conn.execute(
                """
                INSERT INTO person_mentions (
                    project_id, mention_index, raw_department_name, raw_person_name, normalized_person_name,
                    name_quality, role, contact_email, contact_phone, extracted_section, source_confidence, review_status
                )
                VALUES (?, 0, '佐賀県 政策部 広報広聴課', '川久保', '川久保', 'surname_only', 'contact',
                        'kouhou@example.jp', '0952-25-7351', 'snippet-a', 0.9, 'pending')
                """,
                (project_a,),
            )
            mention_a = conn.execute("select id from person_mentions where project_id = ?", (project_a,)).fetchone()[0]

            conn.execute(
                """
                INSERT INTO person_mentions (
                    project_id, mention_index, raw_department_name, raw_person_name, normalized_person_name,
                    name_quality, role, contact_email, contact_phone, extracted_section, source_confidence, review_status
                )
                VALUES (?, 0, '佐賀県 地域交流部', '川久保', '川久保', 'surname_only', 'contact',
                        'kokusai@example.jp', '0952-25-7000', 'snippet-b', 0.9, 'pending')
                """,
                (project_b,),
            )
            mention_b = conn.execute("select id from person_mentions where project_id = ?", (project_b,)).fetchone()[0]

            conn.execute(
                """
                INSERT INTO person_mentions (
                    project_id, mention_index, raw_department_name, raw_person_name, normalized_person_name,
                    name_quality, role, contact_email, contact_phone, extracted_section, source_confidence, review_status
                )
                VALUES (?, 0, '佐賀県 政策部 広報広聴課', '川久保', '川久保', 'surname_only', 'contact',
                        'kouhou@example.jp', '0952-25-7351', 'snippet-pending', 0.9, 'pending')
                """,
                (project_pending,),
            )
            pending_mention = conn.execute("select id from person_mentions where project_id = ?", (project_pending,)).fetchone()[0]
            conn.commit()
            conn.close()

            person_a = create_person_for_mention(mention_a, db_path)
            person_b = create_person_for_mention(mention_b, db_path)

            conn = get_connection(db_path)
            candidates = fetch_identity_candidates_for_mention(conn, pending_mention)
            conn.close()

            self.assertEqual(len(candidates), 2)
            self.assertEqual(candidates[0]["id"], person_a)
            self.assertEqual(candidates[1]["id"], person_b)
            self.assertTrue(candidates[0]["contact_match"])
            self.assertGreater(candidates[0]["candidate_score"], candidates[1]["candidate_score"])

    def test_save_project_record_auto_matches_same_name_with_same_contact(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = get_connection(db_path)
            conn.execute(
                """
                INSERT INTO projects (title, url, source_type, summary, purpose, budget, application_deadline,
                                      submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                                      zip_urls_json, fetched_at)
                VALUES ('seed', 'https://example.com/seed', 'press_release', '', '', '', '', '', '', '', '', '[]', '[]', '')
                """
            )
            project_id = conn.execute("select id from projects where url = 'https://example.com/seed'").fetchone()[0]
            conn.execute(
                """
                INSERT INTO person_mentions (
                    project_id, mention_index, raw_department_name, raw_person_name, normalized_person_name,
                    name_quality, role, contact_email, contact_phone, extracted_section, source_confidence, review_status
                )
                VALUES (?, 0, '佐賀県 政策部 広報広聴課', '川久保', '川久保', 'surname_only', 'contact',
                        'kouhou@example.jp', '0952-25-7351', 'seed', 0.95, 'pending')
                """,
                (project_id,),
            )
            mention_id = conn.execute("select id from person_mentions where project_id = ?", (project_id,)).fetchone()[0]
            conn.commit()
            conn.close()
            create_person_for_mention(mention_id, db_path)

            matched_record = ProjectRecord(
                title="matched",
                url="https://example.com/matched",
                source_type="press_release",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-03-30",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 政策部 広報広聴課",
                        person_name="川久保",
                        person_key="",
                        person_role="contact",
                        contact_email="kouhou@example.jp",
                        contact_phone="0952-25-7351",
                        extracted_section="matched",
                        name_quality="surname_only",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2026-03-30T12:30:00",
            )
            save_project_record(matched_record, db_path)

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            matched_project_id = cur.execute(
                "select id from projects where url = 'https://example.com/matched'"
            ).fetchone()[0]
            status = cur.execute(
                """
                select pil.link_status
                from person_identity_links pil
                join person_mentions pm on pm.id = pil.person_mention_id
                where pm.project_id = ?
                """
                ,
                (matched_project_id,),
            ).fetchone()[0]
            self.assertEqual(status, "auto_matched")
            conn.close()

    def test_resolve_public_appearance_infers_person_from_same_contact(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            known_record = ProjectRecord(
                title="known",
                url="https://example.com/known",
                source_type="press_release",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-03-30",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 観光課",
                        person_name="岩根",
                        person_key="",
                        person_role="contact",
                        contact_email="kankou@example.jp",
                        contact_phone="0952-25-7098",
                        extracted_section="known",
                        name_quality="surname_only",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2026-03-30T12:00:00",
            )
            save_project_record(known_record, db_path)

            conn = get_connection(db_path)
            conn.execute(
                """
                INSERT INTO projects (title, url, source_type, summary, purpose, budget, application_deadline,
                                      submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                                      zip_urls_json, fetched_at)
                VALUES ('unknown', 'https://example.com/unknown', 'press_release', '', '', '', '', '', '', '', '', '[]', '[]', '')
                """
            )
            unknown_id = conn.execute("select id from projects where url = 'https://example.com/unknown'").fetchone()[0]
            conn.execute(
                """
                INSERT INTO appearances (
                    project_id, raw_department_name, raw_person_name, role, contact_email, contact_phone, extracted_section
                )
                VALUES (?, '佐賀県 観光課', '', 'contact', 'kankou@example.jp', '0952-25-7098', 'unknown')
                """,
                (unknown_id,),
            )
            conn.commit()

            project_row = conn.execute("select * from projects where id = ?", (unknown_id,)).fetchone()
            appearance_row = conn.execute("select * from appearances where project_id = ?", (unknown_id,)).fetchone()
            resolved = resolve_public_appearance(conn, project_row, appearance_row)
            conn.close()

            self.assertIsNotNone(resolved)
            self.assertEqual(resolved["display_person_name"], "岩根")
            self.assertEqual(resolved["person_status"], "inferred_same_contact")

    def test_resolve_public_appearance_inherits_from_related_result_page(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = get_connection(db_path)
            conn.execute(
                """
                INSERT INTO projects (title, url, source_type, summary, purpose, budget, application_deadline,
                                      submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                                      zip_urls_json, fetched_at)
                VALUES ('令和8年度グランブルー・ツーリズム情報発信事業業務委託に係るプロポーザルを実施します',
                        'https://example.com/source', 'proposal', '', '', '', '', '', '', '', '', '[]', '[]', '')
                """
            )
            source_id = conn.execute("select id from projects where url = 'https://example.com/source'").fetchone()[0]
            conn.execute(
                """
                INSERT INTO appearances (
                    project_id, raw_department_name, raw_person_name, role, contact_email, contact_phone, extracted_section
                )
                VALUES (?, '佐賀県 観光課 インバウンド担当', '岩根', 'contact', 'kankou@example.jp', '0952-25-7098', 'source')
                """,
                (source_id,),
            )

            conn.execute(
                """
                INSERT INTO projects (title, url, source_type, summary, purpose, budget, application_deadline,
                                      submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                                      zip_urls_json, fetched_at)
                VALUES ('令和8年度グランブルー・ツーリズム情報発信事業業務委託に係るプロポーザルへの質問書に対する回答を公表します',
                        'https://example.com/result', 'proposal', '', '', '', '', '', '', '', '', '[]', '[]', '')
                """
            )
            result_id = conn.execute("select id from projects where url = 'https://example.com/result'").fetchone()[0]
            conn.commit()

            project_row = conn.execute("select * from projects where id = ?", (result_id,)).fetchone()
            resolved = resolve_public_appearance(conn, project_row, None)
            conn.close()

            self.assertIsNotNone(resolved)
            self.assertEqual(resolved["display_person_name"], "岩根")
            self.assertEqual(resolved["display_department_name"], "佐賀県 観光課 インバウンド担当")
            self.assertEqual(resolved["person_status"], "inherited_related_project")
            self.assertEqual(resolved["source_project_id"], source_id)

    def test_fetch_people_counts_secondary_identity_links(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            record = ProjectRecord(
                title="test",
                url="https://example.com/multi",
                source_type="press_release",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-03-30",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 政策部 広報広聴課",
                        person_name="川久保",
                        person_key="",
                        person_role="contact",
                        contact_email="kouhou@example.jp",
                        contact_phone="0952-25-7351",
                        extracted_section="snippet",
                        name_quality="surname_only",
                        source_confidence=0.95,
                    ),
                    PersonMention(
                        department_name="佐賀県 政策部 広報広聴課",
                        person_name="緒方",
                        person_key="",
                        person_role="contact",
                        contact_email="kouhou@example.jp",
                        contact_phone="0952-25-7351",
                        extracted_section="snippet",
                        name_quality="surname_only",
                        source_confidence=0.95,
                    ),
                ],
                fetched_at="2026-03-30T12:00:00",
            )
            save_project_record(record, db_path)

            conn = get_connection(db_path)
            mention_ids = [row["id"] for row in conn.execute("select id from person_mentions order by id asc").fetchall()]
            conn.close()
            create_person_for_mention(mention_ids[0], db_path)
            create_person_for_mention(mention_ids[1], db_path)

            people = fetch_people(limit=20, db_path=db_path)
            ogata_key = next(person["person_key"] for person in people if person["display_name"] == "緒方")
            detail = fetch_person_detail(ogata_key, db_path=db_path)

            self.assertEqual([person["display_name"] for person in people], ["川久保", "緒方"])
            self.assertTrue(all(person["project_count"] == 1 for person in people))
            self.assertIsNotNone(detail)
            self.assertEqual(len(detail["projects"]), 1)
            self.assertEqual(detail["projects"][0]["title"], "test")

    def test_cleanup_orphan_people_removes_only_unlinked_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = get_connection(db_path)
            conn.execute(
                "INSERT INTO people (person_key, display_name, normalized_name) VALUES ('orphan-key', '玄海創生', '玄海創生')"
            )
            conn.execute(
                "INSERT INTO people (person_key, display_name, normalized_name) VALUES ('linked-key', '川久保', '川久保')"
            )
            linked_id = conn.execute("select id from people where person_key = 'linked-key'").fetchone()[0]
            conn.execute(
                """
                INSERT INTO projects (title, url, source_type, summary, purpose, budget, application_deadline,
                                      submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                                      zip_urls_json, fetched_at)
                VALUES ('linked-project', 'https://example.com/linked', 'press_release', '', '', '', '', '', '', '', '', '[]', '[]', '')
                """
            )
            project_id = conn.execute("select id from projects where url = 'https://example.com/linked'").fetchone()[0]
            conn.execute(
                """
                INSERT INTO person_mentions (
                    project_id, mention_index, raw_department_name, raw_person_name, normalized_person_name,
                    name_quality, role, contact_email, contact_phone, extracted_section, source_confidence, review_status
                )
                VALUES (?, 0, '佐賀県 政策部', '川久保', '川久保', 'surname_only', 'contact', '', '', 'snippet', 0.7, 'pending')
                """,
                (project_id,),
            )
            mention_id = conn.execute("select id from person_mentions where project_id = ?", (project_id,)).fetchone()[0]
            conn.execute(
                """
                INSERT INTO person_identity_links (person_mention_id, person_id, link_status, confidence, matched_by, notes)
                VALUES (?, ?, 'reviewed_match', 1.0, 'manual', '')
                """,
                (mention_id, linked_id),
            )
            conn.commit()
            conn.close()

            removed = cleanup_orphan_people(db_path)

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            self.assertEqual(removed, 1)
            self.assertEqual(cur.execute("select count(*) from people where person_key = 'orphan-key'").fetchone()[0], 0)
            self.assertEqual(cur.execute("select count(*) from people where person_key = 'linked-key'").fetchone()[0], 1)
            conn.close()


if __name__ == "__main__":
    unittest.main()
