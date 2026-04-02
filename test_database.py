import sqlite3
import tempfile
import unittest
from pathlib import Path

from database import (
    cleanup_orphan_people,
    compute_slot_timeline_recommendations,
    create_person_for_mention,
    department_theme_tokens,
    fetch_department_profiles,
    fetch_network_snapshot,
    fetch_identity_candidates_for_mention,
    fetch_pending_identity_reviews,
    fetch_people,
    fetch_person_detail,
    fetch_policy_topic_index,
    fetch_project_detail,
    fetch_slot_candidates_for_mention,
    fetch_staff_roster,
    get_connection,
    get_or_create_department,
    group_department_profiles_by_top_unit,
    import_policy_sources_csv,
    import_transfers_csv,
    init_db,
    link_person_mention_to_timeline_recommendation,
    normalize_department_for_match,
    normalize_project_title_for_pairing,
    refresh_department_references,
    resolve_department_hierarchy,
    resolve_public_appearance,
    save_project_record,
    score_person_identity_candidate,
    score_slot_candidate,
    titles_look_related,
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

    def test_init_db_creates_employee_slot_tables(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            tables = {
                row[0]
                for row in cur.execute(
                    "select name from sqlite_master where type = 'table' and name in ('employee_slots', 'slot_candidates')"
                ).fetchall()
            }
            slot_columns = {
                row[1]
                for row in cur.execute("pragma table_info(employee_slots)").fetchall()
            }
            candidate_columns = {
                row[1]
                for row in cur.execute("pragma table_info(slot_candidates)").fetchall()
            }
            conn.close()

            self.assertEqual(tables, {"employee_slots", "slot_candidates"})
            self.assertTrue(
                {"slot_key", "normalized_person_name", "raw_department_name", "active_from", "active_to"}.issubset(slot_columns)
            )
            self.assertTrue(
                {"person_mention_id", "employee_slot_id", "candidate_score", "matched_by"}.issubset(candidate_columns)
            )

    def test_init_db_creates_department_aliases_table(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            tables = {
                row[0]
                for row in cur.execute(
                    "select name from sqlite_master where type = 'table' and name = 'department_aliases'"
                ).fetchall()
            }
            alias_columns = {
                row[1]
                for row in cur.execute("pragma table_info(department_aliases)").fetchall()
            }
            conn.close()

            self.assertEqual(tables, {"department_aliases"})
            self.assertTrue({"department_id", "alias_name", "normalized_alias", "alias_type"}.issubset(alias_columns))

    def test_init_db_creates_policy_topic_tables(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            tables = {
                row[0]
                for row in cur.execute(
                    "select name from sqlite_master where type = 'table' and name in ('policy_sources', 'policy_topics', 'topic_source_mentions', 'project_topic_links', 'person_topic_rollups')"
                ).fetchall()
            }
            topic_columns = {
                row[1]
                for row in cur.execute("pragma table_info(policy_topics)").fetchall()
            }
            rollup_columns = {
                row[1]
                for row in cur.execute("pragma table_info(person_topic_rollups)").fetchall()
            }
            conn.close()

            self.assertEqual(
                tables,
                {"policy_sources", "policy_topics", "topic_source_mentions", "project_topic_links", "person_topic_rollups"},
            )
            self.assertTrue({"topic_key", "topic_year", "origin_type", "keywords_json"}.issubset(topic_columns))
            self.assertTrue({"person_id", "topic_id", "priority_project_count", "spotlight_score"}.issubset(rollup_columns))

    def test_get_or_create_department_canonicalizes_prefecture_department_variants(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = get_connection(db_path)
            department_id = get_or_create_department(
                conn,
                "佐賀県教育委員会事務局 教育振興課 グローバル人材育成担当",
            )
            department = conn.execute("SELECT name FROM departments WHERE id = ?", (department_id,)).fetchone()
            aliases = conn.execute(
                "SELECT alias_name FROM department_aliases WHERE department_id = ? ORDER BY id ASC",
                (department_id,),
            ).fetchall()
            conn.close()

            self.assertEqual(department["name"], "教育振興課 グローバル人材育成担当")
            self.assertIn(
                "佐賀県教育委員会事務局 教育振興課 グローバル人材育成担当",
                [row["alias_name"] for row in aliases],
            )

    def test_get_or_create_department_prefers_specific_section_over_parent_bureau(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = get_connection(db_path)
            department_id = get_or_create_department(
                conn,
                "佐賀県男女参画・こども局 こども未来課 子育てし大県推進担当",
            )
            department = conn.execute("SELECT name FROM departments WHERE id = ?", (department_id,)).fetchone()
            conn.close()

            self.assertEqual(department["name"], "こども未来課 子育てし大県推進担当")

    def test_get_or_create_department_maps_prefecture_variants_to_same_specific_department(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = get_connection(db_path)
            first_id = get_or_create_department(conn, "佐賀県 地域交流部 交通政策課 地域交通システム室")
            second_id = get_or_create_department(conn, "交通政策課 地域交通システム室")
            department = conn.execute("SELECT name FROM departments WHERE id = ?", (first_id,)).fetchone()
            conn.close()

            self.assertEqual(first_id, second_id)
            self.assertEqual(department["name"], "交通政策課 地域交通システム室")

    def test_get_or_create_department_does_not_map_municipal_culture_department(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = get_connection(db_path)
            department_id = get_or_create_department(conn, "佐賀市 歴史・文化課")
            department = conn.execute("SELECT name FROM departments WHERE id = ?", (department_id,)).fetchone()
            conn.close()

            self.assertEqual(department["name"], "佐賀市 歴史・文化課")

    def test_resolve_department_hierarchy_maps_department_to_top_unit(self):
        hierarchy = resolve_department_hierarchy("佐賀県 地域交流部 交通政策課 地域交通システム室")

        self.assertEqual(hierarchy["top_unit"], "地域交流部")
        self.assertEqual(hierarchy["child_name"], "交通政策課")

    def test_normalize_department_for_match_keeps_specific_section_units(self):
        normalized = normalize_department_for_match("佐賀県 政策部 広報広聴課 広聴担当")

        self.assertEqual(normalized, "広報広聴課広聴担当")

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

    def test_save_project_record_uses_source_department_fallback_when_mentions_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            record = ProjectRecord(
                title="source fallback",
                url="https://example.com/source-fallback",
                source_type="proposal",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-03-31",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[],
                fetched_at="2026-03-31T09:00:00",
                source_list_url="https://www.pref.saga.lg.jp/list00156.html",
                source_department_name="教育振興課",
            )

            save_project_record(record, db_path)

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            appearance = cur.execute(
                "select raw_department_name, raw_person_name, extracted_section from appearances where project_id = 1"
            ).fetchone()
            project = cur.execute(
                "select source_list_url, source_department_name from projects where id = 1"
            ).fetchone()
            conn.close()

            self.assertEqual(project[0], "https://www.pref.saga.lg.jp/list00156.html")
            self.assertEqual(project[1], "教育振興課")
            self.assertEqual(appearance[0], "教育振興課")
            self.assertEqual(appearance[1], "")
            self.assertIn("Source list fallback", appearance[2])

    def test_save_project_record_uses_canonical_department_id_for_source_master(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            record = ProjectRecord(
                title="canonical department",
                url="https://example.com/canonical-dept",
                source_type="proposal",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-03-31",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県教育委員会事務局 教育振興課 グローバル人材育成担当",
                        person_name="",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="snippet",
                        name_quality="unknown",
                        source_confidence=0.5,
                    )
                ],
                fetched_at="2026-03-31T12:00:00",
            )

            save_project_record(record, db_path)

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            department = cur.execute(
                """
                SELECT d.name
                FROM appearances a
                JOIN departments d ON d.id = a.department_id
                WHERE a.project_id = 1
                """
            ).fetchone()
            conn.close()

            self.assertEqual(department[0], "教育振興課 グローバル人材育成担当")

    def test_refresh_department_references_relinks_and_cleans_invalid_departments(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = get_connection(db_path)
            conn.execute(
                """
                INSERT INTO departments (name, normalized_name)
                VALUES ('履行場所', '履行場所')
                """
            )
            invalid_department_id = conn.execute(
                "SELECT id FROM departments WHERE normalized_name = '履行場所'"
            ).fetchone()[0]
            conn.execute(
                """
                INSERT INTO projects (title, url, source_type, summary, purpose, budget, application_deadline,
                                      submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                                      zip_urls_json, fetched_at)
                VALUES ('test', 'https://example.com/project-invalid-dept', 'proposal', '', '', '', '', '', '', '', '', '[]', '[]', '')
                """
            )
            conn.execute(
                """
                INSERT INTO appearances (
                    project_id, department_id, raw_department_name, raw_person_name, role, contact_email, contact_phone, extracted_section
                )
                VALUES (1, ?, '履行場所', '', 'contact', '', '', 'snippet')
                """,
                (invalid_department_id,),
            )
            conn.execute(
                """
                INSERT INTO person_mentions (
                    project_id, mention_index, department_id, raw_department_name, raw_person_name, normalized_person_name,
                    name_quality, role, contact_email, contact_phone, extracted_section, source_confidence, review_status
                )
                VALUES (1, 0, ?, '履行場所', '', '', 'unknown', 'contact', '', '', 'snippet', 0.1, 'pending')
                """,
                (invalid_department_id,),
            )
            conn.commit()
            conn.close()

            counts = refresh_department_references(db_path)

            conn = sqlite3.connect(db_path)
            appearance = conn.execute("SELECT department_id FROM appearances WHERE project_id = 1").fetchone()
            mention = conn.execute("SELECT department_id FROM person_mentions WHERE project_id = 1").fetchone()
            still_exists = conn.execute("SELECT 1 FROM departments WHERE id = ?", (invalid_department_id,)).fetchone()
            conn.close()

            self.assertIsNone(appearance[0])
            self.assertIsNone(mention[0])
            self.assertIsNone(still_exists)

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

    def test_import_transfers_csv_canonicalizes_departments_and_auto_links_person(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            csv_path = Path(tmpdir) / "transfers.csv"
            init_db(db_path)

            early_record = ProjectRecord(
                title="early",
                url="https://example.com/early",
                source_type="press_release",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-03-20",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県教育委員会事務局 教育振興課 グローバル人材育成担当",
                        person_name="椛島 秀樹",
                        person_key="",
                        person_role="contact",
                        contact_email="global@example.jp",
                        contact_phone="0952-25-0000",
                        extracted_section="early",
                        name_quality="full_name",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2026-03-20T12:00:00",
            )
            later_record = ProjectRecord(
                title="later",
                url="https://example.com/later",
                source_type="press_release",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-05-20",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 地域交流部 交通政策課 地域交通システム室",
                        person_name="椛島 秀樹",
                        person_key="",
                        person_role="contact",
                        contact_email="global@example.jp",
                        contact_phone="0952-25-0000",
                        extracted_section="later",
                        name_quality="full_name",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2026-05-20T12:00:00",
            )
            save_project_record(early_record, db_path)
            save_project_record(later_record, db_path)

            csv_path.write_text(
                "\n".join(
                    [
                        "source_type,title,url,published_at,effective_date,person_name,from_department,to_department,from_title,to_title,evidence_snippet,publisher,raw_text",
                        "official_transfer_list,令和8年4月1日付人事異動,https://example.com/transfers,2026-04-01,2026-04-01,椛島 秀樹,佐賀県教育委員会事務局 教育振興課 グローバル人材育成担当,佐賀県 地域交流部 交通政策課 地域交通システム室,主査,主査,教育振興課から交通政策課へ,佐賀県,raw",
                    ]
                ),
                encoding="utf-8",
            )

            counts = import_transfers_csv(csv_path, db_path)

            conn = get_connection(db_path)
            row = conn.execute(
                """
                SELECT
                    te.raw_person_name,
                    d_from.name AS from_department_name,
                    d_to.name AS to_department_name,
                    til.link_status
                FROM transfer_events te
                LEFT JOIN departments d_from ON d_from.id = te.from_department_id
                LEFT JOIN departments d_to ON d_to.id = te.to_department_id
                LEFT JOIN transfer_identity_links til ON til.transfer_event_id = te.id
                """
            ).fetchone()
            conn.close()

            self.assertEqual(counts["sources"], 1)
            self.assertEqual(counts["events"], 1)
            self.assertEqual(counts["transfer_links"], 1)
            self.assertEqual(row["raw_person_name"], "椛島 秀樹")
            self.assertEqual(row["from_department_name"], "教育振興課 グローバル人材育成担当")
            self.assertEqual(row["to_department_name"], "交通政策課 地域交通システム室")
            self.assertEqual(row["link_status"], "auto_matched")

    def test_import_transfers_builds_employee_slot_chain(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            csv_path = Path(tmpdir) / "transfers.csv"
            init_db(db_path)

            csv_path.write_text(
                "\n".join(
                    [
                        "source_type,title,url,published_at,effective_date,person_name,from_department,to_department,from_title,to_title,evidence_snippet,publisher,raw_text",
                        "official_transfer_list,令和7年4月1日付人事異動,https://example.com/transfers-2025,2025-04-01,2025-04-01,椛島 秀樹,教育振興課,交通政策課,主査,主査,教育振興課から交通政策課へ,佐賀県,raw",
                        "official_transfer_list,令和8年4月1日付人事異動,https://example.com/transfers-2026,2026-04-01,2026-04-01,椛島 秀樹,交通政策課,観光課,主査,主査,交通政策課から観光課へ,佐賀県,raw",
                    ]
                ),
                encoding="utf-8",
            )

            counts = import_transfers_csv(csv_path, db_path)

            conn = get_connection(db_path)
            slots = conn.execute(
                """
                SELECT id, raw_department_name, active_from, active_to, previous_slot_id, next_slot_id
                FROM employee_slots
                ORDER BY active_from ASC, id ASC
                """
            ).fetchall()
            conn.close()

            self.assertEqual(counts["employee_slots"], 2)
            self.assertEqual(len(slots), 2)
            self.assertEqual(slots[0]["raw_department_name"], "交通政策課")
            self.assertEqual(slots[0]["active_from"], "2025-04-01")
            self.assertEqual(slots[0]["active_to"], "2026-03-31")
            self.assertEqual(slots[1]["raw_department_name"], "観光課")
            self.assertEqual(slots[1]["active_from"], "2026-04-01")
            self.assertEqual(slots[1]["previous_slot_id"], slots[0]["id"])
            self.assertEqual(slots[0]["next_slot_id"], slots[1]["id"])

    def test_fetch_identity_candidates_prefers_transfer_recent_match(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            csv_path = Path(tmpdir) / "transfers.csv"
            init_db(db_path)

            conn = get_connection(db_path)
            conn.execute(
                "INSERT INTO people (person_key, display_name, normalized_name) VALUES ('hideki-a', '椛島 秀樹', '椛島秀樹')"
            )
            conn.execute(
                "INSERT INTO people (person_key, display_name, normalized_name) VALUES ('hideki-b', '椛島 秀樹', '椛島秀樹')"
            )
            person_a = conn.execute("SELECT id FROM people WHERE person_key = 'hideki-a'").fetchone()[0]
            person_b = conn.execute("SELECT id FROM people WHERE person_key = 'hideki-b'").fetchone()[0]

            education_id = get_or_create_department(conn, "佐賀県教育委員会事務局 教育振興課 グローバル人材育成担当")
            transport_id = get_or_create_department(conn, "佐賀県 地域交流部 交通政策課 地域交通システム室")
            tourism_id = get_or_create_department(conn, "佐賀県 観光課 インバウンド担当")

            projects = [
                ("a-early", "https://example.com/a-early", "2026-03-20", person_a, education_id, "佐賀県教育委員会事務局 教育振興課 グローバル人材育成担当"),
                ("a-later", "https://example.com/a-later", "2026-05-20", person_a, transport_id, "佐賀県 地域交流部 交通政策課 地域交通システム室"),
                ("b-only", "https://example.com/b-only", "2026-05-20", person_b, tourism_id, "佐賀県 観光課 インバウンド担当"),
            ]
            for title, url, published_at, person_id, department_id, raw_department_name in projects:
                conn.execute(
                    """
                    INSERT INTO projects (title, url, source_type, summary, purpose, budget, application_deadline,
                                          submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                                          zip_urls_json, fetched_at)
                    VALUES (?, ?, 'press_release', '', '', '', '', '', ?, '', '', '[]', '[]', ?)
                    """,
                    (title, url, published_at, f"{published_at}T12:00:00"),
                )
                project_id = conn.execute("SELECT id FROM projects WHERE url = ?", (url,)).fetchone()[0]
                conn.execute(
                    """
                    INSERT INTO appearances (
                        project_id, department_id, person_id, raw_department_name, raw_person_name,
                        role, contact_email, contact_phone, extracted_section
                    )
                    VALUES (?, ?, ?, ?, '椛島 秀樹', 'contact', '', '', 'seed')
                    """,
                    (project_id, department_id, person_id, raw_department_name),
                )

            conn.execute(
                """
                INSERT INTO projects (title, url, source_type, summary, purpose, budget, application_deadline,
                                      submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                                      zip_urls_json, fetched_at)
                VALUES ('pending', 'https://example.com/pending-transfer', 'press_release', '', '', '', '', '',
                        '2026-04-20', '', '', '[]', '[]', '2026-04-20T12:00:00')
                """
            )
            pending_project_id = conn.execute(
                "SELECT id FROM projects WHERE url = 'https://example.com/pending-transfer'"
            ).fetchone()[0]
            conn.execute(
                """
                INSERT INTO person_mentions (
                    project_id, mention_index, department_id, raw_department_name, raw_person_name, normalized_person_name,
                    name_quality, role, contact_email, contact_phone, extracted_section, source_confidence, review_status
                )
                VALUES (?, 0, ?, '佐賀県 地域交流部 交通政策課 地域交通システム室', '椛島 秀樹', '椛島秀樹',
                        'full_name', 'contact', '', '', 'pending', 0.8, 'pending')
                """,
                (pending_project_id, transport_id),
            )
            mention_id = conn.execute(
                "SELECT id FROM person_mentions WHERE project_id = ?",
                (pending_project_id,),
            ).fetchone()[0]
            conn.commit()
            conn.close()

            csv_path.write_text(
                "\n".join(
                    [
                        "source_type,title,url,published_at,effective_date,person_name,from_department,to_department,from_title,to_title,evidence_snippet,publisher,raw_text",
                        "official_transfer_list,令和8年4月1日付人事異動,https://example.com/transfers,2026-04-01,2026-04-01,椛島 秀樹,佐賀県教育委員会事務局 教育振興課 グローバル人材育成担当,佐賀県 地域交流部 交通政策課 地域交通システム室,主査,主査,教育振興課から交通政策課へ,佐賀県,raw",
                    ]
                ),
                encoding="utf-8",
            )
            import_transfers_csv(csv_path, db_path)

            conn = get_connection(db_path)
            candidates = fetch_identity_candidates_for_mention(conn, mention_id)
            conn.close()

            self.assertEqual(candidates[0]["id"], person_a)
            self.assertTrue(candidates[0]["transfer_match"])
            self.assertTrue(candidates[0]["transfer_recent_match"])
            self.assertGreater(candidates[0]["candidate_score"], candidates[1]["candidate_score"])

    def test_fetch_project_detail_exposes_transfer_candidates_for_missing_person(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            csv_path = Path(tmpdir) / "transfers.csv"
            init_db(db_path)

            record = ProjectRecord(
                title="seed",
                url="https://example.com/seed-transfer-person",
                source_type="press_release",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-05-20",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 地域交流部 交通政策課 地域交通システム室",
                        person_name="椛島 秀樹",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="seed",
                        name_quality="full_name",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2026-05-20T12:00:00",
            )
            save_project_record(record, db_path)

            csv_path.write_text(
                "\n".join(
                    [
                        "source_type,title,url,published_at,effective_date,person_name,from_department,to_department,from_title,to_title,evidence_snippet,publisher,raw_text",
                        "official_transfer_list,令和8年4月1日付人事異動,https://example.com/transfers,2026-04-01,2026-04-01,椛島 秀樹,佐賀県教育委員会事務局 教育振興課 グローバル人材育成担当,佐賀県 地域交流部 交通政策課 地域交通システム室,主査,主査,教育振興課から交通政策課へ,佐賀県,raw",
                    ]
                ),
                encoding="utf-8",
            )
            import_transfers_csv(csv_path, db_path)

            conn = get_connection(db_path)
            transport_id = get_or_create_department(conn, "佐賀県 地域交流部 交通政策課 地域交通システム室")
            conn.execute(
                """
                INSERT INTO projects (title, url, source_type, summary, purpose, budget, application_deadline,
                                      submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                                      zip_urls_json, fetched_at)
                VALUES ('missing', 'https://example.com/missing-transfer', 'proposal', '', '', '', '', '',
                        '2026-04-20', '', '', '[]', '[]', '2026-04-20T12:00:00')
                """
            )
            project_id = conn.execute(
                "SELECT id FROM projects WHERE url = 'https://example.com/missing-transfer'"
            ).fetchone()[0]
            conn.execute(
                """
                INSERT INTO appearances (
                    project_id, department_id, person_id, raw_department_name, raw_person_name,
                    role, contact_email, contact_phone, extracted_section
                )
                VALUES (?, ?, NULL, '佐賀県 地域交流部 交通政策課 地域交通システム室', '',
                        'contact', '', '', 'missing')
                """,
                (project_id, transport_id),
            )
            conn.commit()
            conn.close()

            detail = fetch_project_detail(project_id, db_path=db_path)

            self.assertIsNotNone(detail)
            self.assertEqual(len(detail["transfer_candidates"]), 1)
            self.assertEqual(detail["transfer_candidates"][0]["display_name"], "椛島 秀樹")
            self.assertEqual(detail["transfer_candidates"][0]["match_type"], "to_department")

    def test_import_transfers_links_exact_name_with_department_overlap(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            csv_path = Path(tmpdir) / "transfers.csv"
            init_db(db_path)

            record = ProjectRecord(
                title="seed-overlap",
                url="https://example.com/seed-overlap",
                source_type="press_release",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2025-04-10",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 地域交流部",
                        person_name="永田 辰浩",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="seed",
                        name_quality="full_name",
                        source_confidence=0.92,
                    )
                ],
                fetched_at="2025-04-10T12:00:00",
            )
            save_project_record(record, db_path)

            csv_path.write_text(
                "\n".join(
                    [
                        "source_type,title,url,published_at,effective_date,person_name,from_department,to_department,from_title,to_title,evidence_snippet,publisher,raw_text",
                        "newspaper_transfer_list,2025人事異動,https://example.com/newspaper,2025-03-19,2025-04-01,永田 辰浩,,佐賀県 地域交流部 さが創生推進課,,課長,永田辰浩が地域交流部さが創生推進課へ,佐賀新聞,raw",
                    ]
                ),
                encoding="utf-8",
            )
            import_transfers_csv(csv_path, db_path)

            conn = get_connection(db_path)
            link = conn.execute(
                """
                SELECT til.link_status, til.confidence, pe.display_name, til.notes
                FROM transfer_identity_links til
                JOIN transfer_events te ON te.id = til.transfer_event_id
                JOIN people pe ON pe.id = til.person_id
                WHERE te.raw_person_name = '永田 辰浩'
                """
            ).fetchone()
            conn.close()

            self.assertIsNotNone(link)
            self.assertEqual(link["display_name"], "永田 辰浩")
            self.assertEqual(link["link_status"], "review_pending")
            self.assertGreaterEqual(link["confidence"], 0.67)

    def test_import_transfers_links_surname_only_person_when_department_aligns(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            csv_path = Path(tmpdir) / "transfers.csv"
            init_db(db_path)

            record = ProjectRecord(
                title="seed-surname-bridge",
                url="https://example.com/seed-surname-bridge",
                source_type="press_release",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2025-05-10",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 産業労働部 ものづくり産業課",
                        person_name="川原",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="seed",
                        name_quality="surname_only",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2025-05-10T12:00:00",
            )
            save_project_record(record, db_path)
            conn = get_connection(db_path)
            mention_id = conn.execute("SELECT id FROM person_mentions LIMIT 1").fetchone()[0]
            conn.close()
            create_person_for_mention(mention_id, db_path)

            csv_path.write_text(
                "\n".join(
                    [
                        "source_type,title,url,published_at,effective_date,person_name,from_department,to_department,from_title,to_title,evidence_snippet,publisher,raw_text",
                        "newspaper_transfer_list,2025人事異動,https://example.com/newspaper,2025-03-19,2025-04-01,川原 靖,ものづくり課,佐賀県 産業労働部 ものづくり産業課,係長,課長,川原靖がものづくり産業課へ,佐賀新聞,raw",
                    ]
                ),
                encoding="utf-8",
            )
            import_transfers_csv(csv_path, db_path)

            conn = get_connection(db_path)
            link = conn.execute(
                """
                SELECT til.link_status, til.confidence, pe.display_name, til.notes
                FROM transfer_identity_links til
                JOIN transfer_events te ON te.id = til.transfer_event_id
                JOIN people pe ON pe.id = til.person_id
                WHERE te.raw_person_name = '川原 靖'
                """
            ).fetchone()
            conn.close()

            self.assertIsNotNone(link)
            self.assertEqual(link["display_name"], "川原")
            self.assertEqual(link["link_status"], "review_pending")
            self.assertGreaterEqual(link["confidence"], 0.5)
            self.assertIn("bridge", link["notes"])

    def test_import_transfers_links_surname_only_person_with_department_overlap_at_lower_threshold(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            csv_path = Path(tmpdir) / "transfers.csv"
            init_db(db_path)

            record = ProjectRecord(
                title="seed-overlap-threshold",
                url="https://example.com/seed-overlap-threshold",
                source_type="press_release",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2025-05-10",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="政策部 政策チーム",
                        person_name="大草",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="seed",
                        name_quality="surname_only",
                        source_confidence=0.92,
                    )
                ],
                fetched_at="2025-05-10T12:00:00",
            )
            save_project_record(record, db_path)
            conn = get_connection(db_path)
            mention_id = conn.execute("SELECT id FROM person_mentions LIMIT 1").fetchone()[0]
            conn.close()
            create_person_for_mention(mention_id, db_path)

            csv_path.write_text(
                "\n".join(
                    [
                        "source_type,title,url,published_at,effective_date,person_name,from_department,to_department,from_title,to_title,evidence_snippet,publisher,raw_text",
                        "newspaper_transfer_list,2025人事異動,https://example.com/newspaper,2025-03-19,2025-04-01,大草 昭雄,,政策部,主査,部長,大草昭雄が政策部へ,佐賀新聞,raw",
                    ]
                ),
                encoding="utf-8",
            )
            import_transfers_csv(csv_path, db_path)

            conn = get_connection(db_path)
            link = conn.execute(
                """
                SELECT til.link_status, til.confidence, pe.display_name
                FROM transfer_identity_links til
                JOIN transfer_events te ON te.id = til.transfer_event_id
                JOIN people pe ON pe.id = til.person_id
                WHERE te.raw_person_name = '大草 昭雄'
                """
            ).fetchone()
            conn.close()

            self.assertIsNotNone(link)
            self.assertEqual(link["display_name"], "大草")
            self.assertEqual(link["link_status"], "review_pending")
            self.assertGreaterEqual(link["confidence"], 0.42)

    def test_slot_candidates_distinguish_same_surname_by_department_and_time(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            csv_path = Path(tmpdir) / "transfers.csv"
            init_db(db_path)

            record = ProjectRecord(
                title="seed-slot-candidate",
                url="https://example.com/seed-slot-candidate",
                source_type="press_release",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2025-05-10",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 産業労働部 産業政策課",
                        person_name="田中",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="seed",
                        name_quality="surname_only",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2025-05-10T12:00:00",
            )
            save_project_record(record, db_path)

            csv_path.write_text(
                "\n".join(
                    [
                        "source_type,title,url,published_at,effective_date,person_name,from_department,to_department,from_title,to_title,evidence_snippet,publisher,raw_text",
                        "newspaper_transfer_list,2025人事異動,https://example.com/newspaper,2025-03-19,2025-04-01,田中 太郎,,佐賀県 産業労働部 産業政策課,,主査,田中太郎が産業政策課へ,佐賀新聞,raw",
                        "newspaper_transfer_list,2025人事異動,https://example.com/newspaper,2025-03-19,2025-04-01,田中 花子,,佐賀県 地域交流部 観光課,,主査,田中花子が観光課へ,佐賀新聞,raw",
                    ]
                ),
                encoding="utf-8",
            )
            import_transfers_csv(csv_path, db_path)

            conn = get_connection(db_path)
            mention_id = conn.execute("SELECT id FROM person_mentions LIMIT 1").fetchone()[0]
            slot_candidates = fetch_slot_candidates_for_mention(conn, mention_id)
            conn.close()

            self.assertEqual(len(slot_candidates), 1)
            self.assertEqual(slot_candidates[0]["display_name"], "田中 太郎")
            self.assertEqual(slot_candidates[0]["raw_department_name"], "佐賀県 産業労働部 産業政策課")
            self.assertGreaterEqual(slot_candidates[0]["candidate_score"], 0.55)

    def test_slot_timeline_recommendations_prefer_continuous_assignment(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = get_connection(db_path)
            industry_id = get_or_create_department(conn, "佐賀県 産業労働部 産業政策課")
            for title, url, published_at in [
                ("m1", "https://example.com/m1", "2025-05-10"),
                ("m2", "https://example.com/m2", "2025-05-20"),
            ]:
                conn.execute(
                    """
                    INSERT INTO projects (title, url, source_type, summary, purpose, budget, application_deadline,
                                          submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                                          zip_urls_json, fetched_at)
                    VALUES (?, ?, 'press_release', '', '', '', '', '', ?, '', '', '[]', '[]', ?)
                    """,
                    (title, url, published_at, f"{published_at}T12:00:00"),
                )
            project_ids = [row[0] for row in conn.execute("SELECT id FROM projects ORDER BY id ASC").fetchall()]
            for project_id in project_ids:
                conn.execute(
                    """
                    INSERT INTO person_mentions (
                        project_id, mention_index, department_id, raw_department_name, raw_person_name, normalized_person_name,
                        name_quality, role, contact_email, contact_phone, extracted_section, source_confidence, review_status
                    )
                    VALUES (?, 0, ?, '佐賀県 産業労働部 産業政策課', '田中', '田中',
                            'surname_only', 'contact', '', '', 'seed', 0.95, 'pending')
                    """,
                    (project_id, industry_id),
                )
            mention_ids = [row[0] for row in conn.execute("SELECT id FROM person_mentions ORDER BY id ASC").fetchall()]

            conn.execute(
                """
                INSERT INTO employee_slots (
                    slot_key, normalized_person_name, display_name, name_quality, department_id, raw_department_name,
                    title_raw, active_from, active_to, slot_confidence
                )
                VALUES
                    ('slot-a', '田中', '田中 太郎', 'full_name', ?, '佐賀県 産業労働部 産業政策課', '主査', '2025-04-01', '', 0.8),
                    ('slot-b', '田中', '田中 花子', 'full_name', ?, '佐賀県 産業労働部 産業政策課', '主査', '2025-04-01', '', 0.8)
                """,
                (industry_id, industry_id),
            )
            slot_ids = [row[0] for row in conn.execute("SELECT id FROM employee_slots ORDER BY id ASC").fetchall()]

            conn.executemany(
                """
                INSERT INTO slot_candidates (person_mention_id, employee_slot_id, candidate_score, matched_by, notes)
                VALUES (?, ?, ?, 'seed', '')
                """,
                [
                    (mention_ids[0], slot_ids[0], 0.75),
                    (mention_ids[0], slot_ids[1], 0.60),
                    (mention_ids[1], slot_ids[0], 0.70),
                    (mention_ids[1], slot_ids[1], 0.74),
                ],
            )
            conn.commit()

            recommendations = compute_slot_timeline_recommendations(conn, mention_ids)
            conn.close()

            self.assertEqual(recommendations[mention_ids[0]]["employee_slot_id"], slot_ids[0])
            self.assertEqual(recommendations[mention_ids[1]]["employee_slot_id"], slot_ids[0])
            self.assertGreater(
                recommendations[mention_ids[1]]["timeline_total_score"],
                recommendations[mention_ids[1]]["candidate_score"],
            )

    def test_pending_identity_reviews_include_slot_candidates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            csv_path = Path(tmpdir) / "transfers.csv"
            init_db(db_path)

            record = ProjectRecord(
                title="seed-review-slot",
                url="https://example.com/seed-review-slot",
                source_type="press_release",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2025-05-10",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 産業労働部 産業政策課",
                        person_name="田中",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="seed",
                        name_quality="surname_only",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2025-05-10T12:00:00",
            )
            save_project_record(record, db_path)

            csv_path.write_text(
                "\n".join(
                    [
                        "source_type,title,url,published_at,effective_date,person_name,from_department,to_department,from_title,to_title,evidence_snippet,publisher,raw_text",
                        "newspaper_transfer_list,2025人事異動,https://example.com/newspaper,2025-03-19,2025-04-01,田中 太郎,,佐賀県 産業労働部 産業政策課,,主査,田中太郎が産業政策課へ,佐賀新聞,raw",
                    ]
                ),
                encoding="utf-8",
            )
            import_transfers_csv(csv_path, db_path)

            reviews = fetch_pending_identity_reviews(limit=20, db_path=db_path)
            review = next(item for item in reviews if item["title"] == "seed-review-slot")

            self.assertTrue(review["slot_candidates"])
            self.assertEqual(review["slot_candidates"][0]["display_name"], "田中 太郎")
            self.assertTrue(review["slot_candidates"][0]["timeline_recommended"])
            self.assertEqual(review["timeline_recommendation"]["display_name"], "田中 太郎")

    def test_link_person_mention_to_timeline_recommendation_uses_existing_person(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            csv_path = Path(tmpdir) / "transfers.csv"
            init_db(db_path)

            seed = ProjectRecord(
                title="seed-existing-person",
                url="https://example.com/seed-existing-person",
                source_type="press_release",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2025-04-10",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 産業労働部 産業政策課",
                        person_name="安冨 喬博",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="seed",
                        name_quality="full_name",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2025-04-10T12:00:00",
            )
            save_project_record(seed, db_path)

            target = ProjectRecord(
                title="target-existing-person",
                url="https://example.com/target-existing-person",
                source_type="press_release",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2025-05-10",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 産業労働部 産業政策課",
                        person_name="安冨",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="target",
                        name_quality="surname_only",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2025-05-10T12:00:00",
            )
            save_project_record(target, db_path)

            csv_path.write_text(
                "\n".join(
                    [
                        "source_type,title,url,published_at,effective_date,person_name,from_department,to_department,from_title,to_title,evidence_snippet,publisher,raw_text",
                        "newspaper_transfer_list,2025人事異動,https://example.com/newspaper,2025-03-19,2025-04-01,安冨 喬博,,佐賀県 産業労働部 産業政策課,,主査,安冨喬博が産業政策課へ,佐賀新聞,raw",
                    ]
                ),
                encoding="utf-8",
            )
            import_transfers_csv(csv_path, db_path)

            conn = get_connection(db_path)
            mention_id = conn.execute(
                "SELECT id FROM person_mentions WHERE raw_person_name = '安冨'"
            ).fetchone()[0]
            review = next(item for item in fetch_pending_identity_reviews(limit=20, db_path=db_path) if item["id"] == mention_id)
            employee_slot_id = review["timeline_recommendation"]["employee_slot_id"]
            conn.close()

            linked_person_id = link_person_mention_to_timeline_recommendation(mention_id, employee_slot_id, db_path=db_path)

            conn = get_connection(db_path)
            link = conn.execute(
                """
                SELECT pil.link_status, pe.display_name
                FROM person_identity_links pil
                JOIN people pe ON pe.id = pil.person_id
                WHERE pil.person_mention_id = ?
                """,
                (mention_id,),
            ).fetchone()
            conn.close()

            self.assertIsNotNone(linked_person_id)
            self.assertEqual(link["link_status"], "reviewed_match")
            self.assertEqual(link["display_name"], "安冨 喬博")

    def test_link_person_mention_to_timeline_recommendation_creates_person_from_slot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = get_connection(db_path)
            industry_id = get_or_create_department(conn, "佐賀県 産業労働部 産業政策課")
            conn.execute(
                """
                INSERT INTO projects (title, url, source_type, summary, purpose, budget, application_deadline,
                                      submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                                      zip_urls_json, fetched_at)
                VALUES ('target-new-person', 'https://example.com/target-new-person', 'press_release', '', '', '', '', '',
                        '2025-05-10', '', '', '[]', '[]', '2025-05-10T12:00:00')
                """
            )
            project_id = conn.execute(
                "SELECT id FROM projects WHERE url = 'https://example.com/target-new-person'"
            ).fetchone()[0]
            conn.execute(
                """
                INSERT INTO person_mentions (
                    project_id, mention_index, department_id, raw_department_name, raw_person_name, normalized_person_name,
                    name_quality, role, contact_email, contact_phone, extracted_section, source_confidence, review_status
                )
                VALUES (?, 0, ?, '佐賀県 産業労働部 産業政策課', '田中', '田中',
                        'surname_only', 'contact', '', '', 'seed', 0.95, 'pending')
                """,
                (project_id, industry_id),
            )
            mention_id = conn.execute("SELECT id FROM person_mentions").fetchone()[0]
            conn.execute(
                """
                INSERT INTO employee_slots (
                    slot_key, normalized_person_name, display_name, name_quality, department_id, raw_department_name,
                    title_raw, active_from, active_to, slot_confidence
                )
                VALUES ('slot-create', '田中太郎', '田中 太郎', 'full_name', ?, '佐賀県 産業労働部 産業政策課',
                        '主査', '2025-04-01', '', 0.88)
                """,
                (industry_id,),
            )
            slot_id = conn.execute("SELECT id FROM employee_slots").fetchone()[0]
            conn.execute(
                """
                INSERT INTO slot_candidates (person_mention_id, employee_slot_id, candidate_score, matched_by, notes)
                VALUES (?, ?, 0.92, 'slot_surname_department_time', 'seed')
                """,
                (mention_id, slot_id),
            )
            conn.commit()
            conn.close()

            person_id = link_person_mention_to_timeline_recommendation(mention_id, slot_id, db_path=db_path)

            conn = get_connection(db_path)
            person = conn.execute("SELECT display_name FROM people WHERE id = ?", (person_id,)).fetchone()
            slot = conn.execute("SELECT person_id FROM employee_slots WHERE id = ?", (slot_id,)).fetchone()
            link = conn.execute(
                "SELECT link_status, person_id FROM person_identity_links WHERE person_mention_id = ?",
                (mention_id,),
            ).fetchone()
            conn.close()

            self.assertIsNotNone(person_id)
            self.assertEqual(person["display_name"], "田中 太郎")
            self.assertEqual(slot["person_id"], person_id)
            self.assertEqual(link["link_status"], "reviewed_match")
            self.assertEqual(link["person_id"], person_id)

    def test_fetch_person_detail_includes_transfer_history(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            csv_path = Path(tmpdir) / "transfers.csv"
            init_db(db_path)

            record = ProjectRecord(
                title="seed-history",
                url="https://example.com/seed-history",
                source_type="press_release",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-05-20",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 地域交流部 交通政策課 地域交通システム室",
                        person_name="椛島 秀樹",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="seed",
                        name_quality="full_name",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2026-05-20T12:00:00",
            )
            save_project_record(record, db_path)

            csv_path.write_text(
                "\n".join(
                    [
                        "source_type,title,url,published_at,effective_date,person_name,from_department,to_department,from_title,to_title,evidence_snippet,publisher,raw_text",
                        "official_transfer_list,令和8年4月1日付人事異動,https://example.com/transfers,2026-04-01,2026-04-01,椛島 秀樹,佐賀県教育委員会事務局 教育振興課 グローバル人材育成担当,佐賀県 地域交流部 交通政策課 地域交通システム室,主査,主査,教育振興課から交通政策課へ,佐賀県,raw",
                    ]
                ),
                encoding="utf-8",
            )
            import_transfers_csv(csv_path, db_path)

            people = fetch_people(limit=10, db_path=db_path)
            detail = fetch_person_detail(people[0]["person_key"], db_path=db_path)

            self.assertIsNotNone(detail)
            self.assertEqual(len(detail["transfer_history"]), 1)
            self.assertEqual(detail["transfer_history"][0]["to_department_raw"], "佐賀県 地域交流部 交通政策課 地域交通システム室")
            self.assertEqual(detail["transfer_history"][0]["source_type"], "official_transfer_list")

    def test_fetch_person_detail_summarizes_current_and_previous_departments(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            csv_path = Path(tmpdir) / "transfers.csv"
            init_db(db_path)

            record = ProjectRecord(
                title="seed-summary",
                url="https://example.com/seed-summary",
                source_type="press_release",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-05-20",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 地域交流部 交通政策課 地域交通システム室",
                        person_name="椛島 秀樹",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="seed",
                        name_quality="full_name",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2026-05-20T12:00:00",
            )
            save_project_record(record, db_path)

            csv_path.write_text(
                "\n".join(
                    [
                        "source_type,title,url,published_at,effective_date,person_name,from_department,to_department,from_title,to_title,evidence_snippet,publisher,raw_text",
                        "official_transfer_list,令和8年4月1日付人事異動,https://example.com/transfers,2026-04-01,2026-04-01,椛島 秀樹,佐賀県教育委員会事務局 教育振興課 グローバル人材育成担当,佐賀県 地域交流部 交通政策課 地域交通システム室,主査,主査,教育振興課から交通政策課へ,佐賀県,raw",
                    ]
                ),
                encoding="utf-8",
            )
            import_transfers_csv(csv_path, db_path)

            people = fetch_people(limit=10, db_path=db_path)
            detail = fetch_person_detail(people[0]["person_key"], db_path=db_path)

            self.assertIsNotNone(detail)
            self.assertEqual(detail["movement_summary"]["current_department"]["name"], "佐賀県 地域交流部 交通政策課 地域交通システム室")
            self.assertEqual(detail["movement_summary"]["previous_departments"][0]["name"], "佐賀県教育委員会事務局 教育振興課 グローバル人材育成担当")
            self.assertEqual(detail["movement_summary"]["latest_transfer"]["effective_date"], "2026-04-01")

    def test_fetch_person_detail_includes_related_people_connections(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            team_record = ProjectRecord(
                title="shared-team-project",
                url="https://example.com/shared-team-project",
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
                        extracted_section="team",
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
                        extracted_section="team",
                        name_quality="surname_only",
                        source_confidence=0.95,
                    ),
                ],
                fetched_at="2026-03-30T12:00:00",
            )
            solo_record = ProjectRecord(
                title="other-project",
                url="https://example.com/other-project",
                source_type="press_release",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-04-10",
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
                        extracted_section="solo",
                        name_quality="surname_only",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2026-04-10T12:00:00",
            )
            save_project_record(team_record, db_path)
            save_project_record(solo_record, db_path)

            conn = get_connection(db_path)
            mention_ids = [row["id"] for row in conn.execute("SELECT id FROM person_mentions ORDER BY id ASC").fetchall()]
            conn.close()
            create_person_for_mention(mention_ids[0], db_path)
            create_person_for_mention(mention_ids[1], db_path)
            create_person_for_mention(mention_ids[2], db_path)

            people = fetch_people(limit=10, db_path=db_path)
            target = next(person for person in people if person["display_name"] == "川久保")
            detail = fetch_person_detail(target["person_key"], db_path=db_path)

            self.assertIsNotNone(detail)
            related_names = [item["display_name"] for item in detail["related_people"]]
            self.assertIn("緒方", related_names)
            ogata = next(item for item in detail["related_people"] if item["display_name"] == "緒方")
            self.assertEqual(ogata["shared_project_count"], 1)
            self.assertGreaterEqual(ogata["shared_department_count"], 1)
            self.assertEqual(ogata["latest_shared_project_title"], "shared-team-project")

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

    def test_resolve_public_appearance_uses_source_department_fallback(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = get_connection(db_path)
            conn.execute(
                """
                INSERT INTO projects (title, url, source_type, source_list_url, source_department_name, summary, purpose, budget,
                                      application_deadline, submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                                      zip_urls_json, fetched_at)
                VALUES ('fallback', 'https://example.com/fallback', 'press_release', 'https://www.pref.saga.lg.jp/list00004.html',
                        '広報広聴課', '', '', '', '', '', '', '', '', '[]', '[]', '')
                """
            )
            project_row = conn.execute("select * from projects where url = 'https://example.com/fallback'").fetchone()
            resolved = resolve_public_appearance(conn, project_row, None)
            conn.close()

            self.assertIsNotNone(resolved)
            self.assertEqual(resolved["display_department_name"], "広報広聴課")
            self.assertEqual(resolved["department_status"], "inherited_source_list")

    def test_resolve_public_appearance_infers_person_from_unique_employee_slot(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = get_connection(db_path)
            department_id = get_or_create_department(conn, "行政デジタル推進課")
            conn.execute(
                """
                INSERT INTO employee_slots (
                    slot_key, normalized_person_name, display_name, name_quality, department_id,
                    raw_department_name, title_raw, active_from, active_to, slot_confidence
                )
                VALUES ('slot-1', '武富有平', '武富 有平', 'full_name', ?, '行政デジタル推進課',
                        '主査', '2025-04-01', '9999-12-31', 0.9)
                """,
                (department_id,),
            )
            conn.execute(
                """
                INSERT INTO projects (title, url, source_type, source_department_name, summary, purpose, budget, application_deadline,
                                      submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                                      zip_urls_json, fetched_at)
                VALUES ('slot-infer', 'https://example.com/slot-infer', 'press_release', '行政デジタル推進課',
                        '', '', '', '', '', '2026-03-31', '', '', '[]', '[]', '2026-03-31T12:00:00')
                """
            )
            project_id = conn.execute("select id from projects where url = 'https://example.com/slot-infer'").fetchone()[0]
            conn.execute(
                """
                INSERT INTO appearances (
                    project_id, department_id, raw_department_name, raw_person_name, role, contact_email, contact_phone, extracted_section
                )
                VALUES (?, ?, '行政デジタル推進課', '', 'contact', '', '', 'slot-infer')
                """,
                (project_id, department_id),
            )
            conn.commit()

            project_row = conn.execute("select * from projects where id = ?", (project_id,)).fetchone()
            appearance_row = conn.execute("select * from appearances where project_id = ?", (project_id,)).fetchone()
            resolved = resolve_public_appearance(conn, project_row, appearance_row)
            conn.close()

            self.assertIsNotNone(resolved)
            self.assertEqual(resolved["display_person_name"], "武富 有平")
            self.assertEqual(resolved["person_status"], "inferred_transfer_slot")

    def test_resolve_public_appearance_does_not_infer_when_employee_slots_are_ambiguous(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = get_connection(db_path)
            department_id = get_or_create_department(conn, "行政デジタル推進課")
            conn.executemany(
                """
                INSERT INTO employee_slots (
                    slot_key, normalized_person_name, display_name, name_quality, department_id,
                    raw_department_name, title_raw, active_from, active_to, slot_confidence
                )
                VALUES (?, ?, ?, 'full_name', ?, '行政デジタル推進課', '主査', '2025-04-01', '9999-12-31', 0.9)
                """,
                [
                    ("slot-a", "武富有平", "武富 有平", department_id),
                    ("slot-b", "山田太郎", "山田 太郎", department_id),
                ],
            )
            conn.execute(
                """
                INSERT INTO projects (title, url, source_type, source_department_name, summary, purpose, budget, application_deadline,
                                      submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                                      zip_urls_json, fetched_at)
                VALUES ('slot-ambiguous', 'https://example.com/slot-ambiguous', 'press_release', '行政デジタル推進課',
                        '', '', '', '', '', '2026-03-31', '', '', '[]', '[]', '2026-03-31T12:00:00')
                """
            )
            project_id = conn.execute("select id from projects where url = 'https://example.com/slot-ambiguous'").fetchone()[0]
            conn.execute(
                """
                INSERT INTO appearances (
                    project_id, department_id, raw_department_name, raw_person_name, role, contact_email, contact_phone, extracted_section
                )
                VALUES (?, ?, '行政デジタル推進課', '', 'contact', '', '', 'slot-ambiguous')
                """,
                (project_id, department_id),
            )
            conn.commit()

            project_row = conn.execute("select * from projects where id = ?", (project_id,)).fetchone()
            appearance_row = conn.execute("select * from appearances where project_id = ?", (project_id,)).fetchone()
            resolved = resolve_public_appearance(conn, project_row, appearance_row)
            conn.close()

            self.assertIsNotNone(resolved)
            self.assertEqual(resolved["display_person_name"], "")
            self.assertEqual(resolved["person_status"], "missing")

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

    def test_titles_look_related_handles_selection_phrase_variant(self):
        left = "令和7年度国際理解キャンプ業務委託に係る企画コンペの結果をお知らせします"
        right = "令和7年度国際理解キャンプ業務委託する事業者を選定する企画コンペを実施します"

        self.assertEqual(
            normalize_project_title_for_pairing(left),
            normalize_project_title_for_pairing(right),
        )
        self.assertTrue(titles_look_related(left, right))

    def test_resolve_public_appearance_inherits_from_related_selection_page(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            source_record = ProjectRecord(
                title="令和7年度国際理解キャンプ業務委託する事業者を選定する企画コンペを実施します",
                url="https://example.com/camp-source",
                source_type="proposal",
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
                        department_name="佐賀県教育委員会事務局 教育振興課 グローバル人材育成担当",
                        person_name="椛島 秀樹",
                        person_key="",
                        person_role="contact",
                        contact_email="global@example.jp",
                        contact_phone="0952-25-0000",
                        extracted_section="source",
                        name_quality="full_name",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2026-03-30T12:00:00",
            )
            save_project_record(source_record, db_path)

            conn = get_connection(db_path)
            conn.execute(
                """
                INSERT INTO projects (title, url, source_type, summary, purpose, budget, application_deadline,
                                      submission_deadline, published_at, raw_text, html_text, pdf_urls_json,
                                      zip_urls_json, fetched_at)
                VALUES ('令和7年度国際理解キャンプ業務委託に係る企画コンペの結果をお知らせします',
                        'https://example.com/camp-result', 'proposal', '', '', '', '', '', '', '', '', '[]', '[]', '')
                """
            )
            result_id = conn.execute("select id from projects where url = 'https://example.com/camp-result'").fetchone()[0]
            project_row = conn.execute("select * from projects where id = ?", (result_id,)).fetchone()

            resolved = resolve_public_appearance(conn, project_row, None)
            conn.close()

            self.assertIsNotNone(resolved)
            self.assertEqual(resolved["display_person_name"], "椛島 秀樹")
            self.assertEqual(resolved["person_status"], "inherited_related_project")

    def test_save_project_record_inherits_related_project_when_mentions_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            source_record = ProjectRecord(
                title="令和7年度国際理解キャンプ業務委託する事業者を選定する企画コンペを実施します",
                url="https://example.com/camp-source-save",
                source_type="proposal",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-03-31",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県教育委員会事務局 教育振興課 グローバル人材育成担当",
                        person_name="椛島 秀樹",
                        person_key="",
                        person_role="contact",
                        contact_email="global@example.jp",
                        contact_phone="0952-25-0000",
                        extracted_section="source",
                        name_quality="full_name",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2026-03-31T09:00:00",
                source_list_url="https://www.pref.saga.lg.jp/list00156.html",
                source_department_name="教育振興課",
            )
            save_project_record(source_record, db_path)

            result_record = ProjectRecord(
                title="令和7年度国際理解キャンプ業務委託に係る企画コンペの結果をお知らせします",
                url="https://example.com/camp-result-save",
                source_type="proposal",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-03-31",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[],
                fetched_at="2026-03-31T10:00:00",
                source_list_url="https://www.pref.saga.lg.jp/list00156.html",
                source_department_name="教育振興課",
            )
            save_project_record(result_record, db_path)

            conn = sqlite3.connect(db_path)
            cur = conn.cursor()
            appearance = cur.execute(
                "select raw_department_name, raw_person_name from appearances where project_id = 2"
            ).fetchone()
            conn.close()

            self.assertEqual(appearance[0], "佐賀県教育委員会事務局 教育振興課 グローバル人材育成担当")
            self.assertEqual(appearance[1], "椛島 秀樹")

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

    def test_surname_only_mentions_stay_out_of_people_but_appear_in_roster_candidates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            record = ProjectRecord(
                title="surname-candidate",
                url="https://example.com/surname-candidate",
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
                        person_name="田中",
                        person_key="",
                        person_role="contact",
                        contact_email="kouhou@example.jp",
                        contact_phone="0952-25-7351",
                        extracted_section="snippet",
                        name_quality="surname_only",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2026-03-30T12:00:00",
            )
            save_project_record(record, db_path)

            people = fetch_people(limit=20, db_path=db_path)
            roster = fetch_staff_roster(db_path=db_path)

            self.assertEqual(people, [])
            self.assertTrue(
                any(
                    item["display_name"] == "田中"
                    and item["department_name"] == "佐賀県 政策部 広報広聴課"
                    for item in roster["mention_candidates"]
                )
            )

    def test_staff_roster_hides_single_weak_surname_only_mentions(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            record = ProjectRecord(
                title="weak-surname-candidate",
                url="https://example.com/weak-surname-candidate",
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
                        department_name="場 所",
                        person_name="西口",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="snippet",
                        name_quality="surname_only",
                        source_confidence=0.75,
                    )
                ],
                fetched_at="2026-03-30T12:00:00",
            )
            save_project_record(record, db_path)

            roster = fetch_staff_roster(db_path=db_path)

            self.assertFalse(any(item["display_name"] == "西口" for item in roster["mention_candidates"]))

    def test_staff_roster_includes_transfer_candidates_with_existing_person_hint(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            csv_path = Path(tmpdir) / "transfers.csv"
            init_db(db_path)

            record = ProjectRecord(
                title="seed-roster-transfer",
                url="https://example.com/seed-roster-transfer",
                source_type="press_release",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2025-04-10",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 地域交流部",
                        person_name="永田 辰浩",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="seed",
                        name_quality="full_name",
                        source_confidence=0.92,
                    )
                ],
                fetched_at="2025-04-10T12:00:00",
            )
            save_project_record(record, db_path)

            csv_path.write_text(
                "\n".join(
                    [
                        "source_type,title,url,published_at,effective_date,person_name,from_department,to_department,from_title,to_title,evidence_snippet,publisher,raw_text",
                        "newspaper_transfer_list,2025人事異動,https://example.com/newspaper,2025-03-19,2025-04-01,永田 辰浩,,佐賀県 地域交流部 さが創生推進課,,課長,永田辰浩が地域交流部さが創生推進課へ,佐賀新聞,raw",
                    ]
                ),
                encoding="utf-8",
            )
            import_transfers_csv(csv_path, db_path)

            roster = fetch_staff_roster(db_path=db_path)
            transfer_candidate = next(
                item for item in roster["transfer_candidates"] if item["display_name"] == "永田 辰浩"
            )

            self.assertEqual(transfer_candidate["candidate_person_name"], "永田 辰浩")
            self.assertGreaterEqual(transfer_candidate["candidate_confidence"], 0.55)
            self.assertTrue(
                any(person["display_name"] == "永田 辰浩" for person in roster["confirmed_people"])
            )

    def test_department_theme_tokens_extract_domain_signal(self):
        traffic_tokens = department_theme_tokens("佐賀県 地域交流部 交通政策課")
        industry_tokens = department_theme_tokens("佐賀県 産業労働部 産業政策課")

        self.assertIn("交通", traffic_tokens)
        self.assertIn("産業", industry_tokens)
        self.assertNotEqual(traffic_tokens, industry_tokens)

    def test_score_slot_candidate_penalizes_surname_bridge_across_unrelated_department_themes(self):
        mention = {
            "normalized_person_name": "田中",
            "raw_person_name": "田中",
            "name_quality": "surname_only",
            "raw_department_name": "佐賀県 地域交流部 交通政策課",
            "department_id": None,
            "project_published_at": "2026-05-10",
            "project_fetched_at": "2026-05-10T12:00:00",
            "source_confidence": 0.9,
        }
        unrelated_slot = {
            "normalized_person_name": "田中太郎",
            "display_name": "田中 太郎",
            "raw_department_name": "佐賀県 産業労働部 産業政策課",
            "department_id": None,
            "active_from": "2026-04-01",
            "active_to": "",
            "person_id": None,
        }
        related_slot = {
            "normalized_person_name": "田中太郎",
            "display_name": "田中 太郎",
            "raw_department_name": "佐賀県 地域交流部 地域交通システム室",
            "department_id": None,
            "active_from": "2026-04-01",
            "active_to": "",
            "person_id": None,
        }

        unrelated = score_slot_candidate(mention, unrelated_slot)
        related = score_slot_candidate(mention, related_slot)

        self.assertLess(unrelated["score"], 0.42)
        self.assertGreater(related["score"], unrelated["score"])
        self.assertTrue(related["department_theme_match"])

    def test_score_person_identity_candidate_rewards_policy_topic_continuity(self):
        mention = {
            "normalized_person_name": "田中太郎",
            "raw_person_name": "田中 太郎",
            "raw_department_name": "佐賀県 地域交流部 交通政策課",
            "department_id": None,
            "project_title": "地域交通再編プロジェクト",
            "project_summary": "",
            "project_purpose": "",
            "project_published_at": "2025-05-10",
            "project_fetched_at": "2025-05-10T12:00:00",
            "policy_topic_names": "地域交通|公共交通",
            "priority_policy_topic_names": "地域交通",
            "name_quality": "full_name",
            "contact_email": "",
            "contact_phone": "",
        }
        person = {
            "display_name": "田中 太郎",
            "normalized_name": "田中太郎",
        }
        matching_context = {
            "emails": set(),
            "phones": set(),
            "departments": set(),
            "department_top_units": {"地域交流部"},
            "department_theme_profiles": {"交通"},
            "project_theme_profiles": {"交通"},
            "policy_topic_names": {"地域交通", "公共交通"},
            "priority_policy_topic_names": {"地域交通"},
            "policy_topic_tokens": {"地域交通", "公共交通", "交通"},
            "policy_topic_years": {"地域交通": {2025}, "公共交通": {2024, 2025}},
            "department_ids": set(),
            "activity_dates": [],
            "transfer_events": [],
            "project_count": 3,
        }
        conflicting_context = {
            **matching_context,
            "department_top_units": {"産業労働部"},
            "department_theme_profiles": {"産業"},
            "project_theme_profiles": {"企業"},
            "policy_topic_names": {"企業誘致", "産業振興"},
            "priority_policy_topic_names": {"企業誘致"},
            "policy_topic_tokens": {"企業誘致", "産業振興", "企業", "産業"},
            "policy_topic_years": {"企業誘致": {2025}},
        }

        matched = score_person_identity_candidate(mention, person, matching_context)
        conflicted = score_person_identity_candidate(mention, person, conflicting_context)

        self.assertTrue(matched["policy_topic_match"])
        self.assertTrue(matched["policy_topic_recent_match"])
        self.assertTrue(matched["priority_policy_topic_match"])
        self.assertGreater(matched["score"], conflicted["score"])

    def test_score_slot_candidate_rewards_policy_topic_continuity(self):
        mention = {
            "normalized_person_name": "田中",
            "raw_person_name": "田中",
            "name_quality": "surname_only",
            "raw_department_name": "",
            "department_id": None,
            "project_title": "県内再編事業",
            "project_summary": "",
            "project_purpose": "",
            "project_published_at": "2025-05-10",
            "project_fetched_at": "2025-05-10T12:00:00",
            "policy_topic_names": "地域交通|公共交通",
            "priority_policy_topic_names": "地域交通",
            "source_confidence": 0.9,
        }
        matched_slot = {
            "normalized_person_name": "田中太郎",
            "display_name": "田中 太郎",
            "raw_department_name": "",
            "department_id": None,
            "active_from": "2025-04-01",
            "active_to": "",
            "person_id": 1,
            "person_policy_topic_names": "地域交通|公共交通",
            "person_priority_policy_topic_names": "地域交通",
            "person_policy_topic_years": "地域交通@2025,公共交通@2024",
        }
        unmatched_slot = {
            "normalized_person_name": "田中太郎",
            "display_name": "田中 太郎",
            "raw_department_name": "",
            "department_id": None,
            "active_from": "2025-04-01",
            "active_to": "",
            "person_id": 2,
            "person_policy_topic_names": "企業誘致|産業振興",
            "person_priority_policy_topic_names": "企業誘致",
            "person_policy_topic_years": "企業誘致@2025",
        }

        matched = score_slot_candidate(mention, matched_slot)
        unmatched = score_slot_candidate(mention, unmatched_slot)

        self.assertTrue(matched["policy_topic_match"])
        self.assertTrue(matched["policy_topic_recent_match"])
        self.assertTrue(matched["priority_policy_topic_match"])
        self.assertGreater(matched["score"], unmatched["score"])

    def test_fetch_staff_roster_includes_active_transfer_slot_roster(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            conn = get_connection(db_path)
            department_id = get_or_create_department(conn, "佐賀県 地域交流部 交通政策課")
            conn.execute(
                """
                INSERT INTO people (person_key, display_name, normalized_name)
                VALUES ('tanaka-taro', '田中 太郎', '田中太郎')
                """
            )
            person_id = conn.execute("SELECT id FROM people WHERE person_key = 'tanaka-taro'").fetchone()[0]
            conn.execute(
                """
                INSERT INTO employee_slots (
                    slot_key, normalized_person_name, display_name, name_quality, department_id,
                    raw_department_name, title_raw, active_from, active_to, person_id, slot_confidence
                )
                VALUES (
                    'slot-roster-1', '田中太郎', '田中 太郎', 'full_name', ?,
                    '佐賀県 地域交流部 交通政策課', '主査', '2026-04-01', '', ?, 0.91
                )
                """,
                (department_id, person_id),
            )
            conn.commit()
            conn.close()

            roster = fetch_staff_roster(db_path=db_path)

            self.assertTrue(
                any(
                    item["display_name"] == "田中 太郎"
                    and item["current_department"] == "佐賀県 地域交流部 交通政策課"
                    for item in roster["slot_roster"]
                )
            )
            self.assertTrue(
                any(
                    any(slot["display_name"] == "田中 太郎" for slot in group["slot_roster"])
                    for group in roster["department_groups"]
                )
            )

    def test_fetch_staff_roster_builds_department_profile_keywords(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            record = ProjectRecord(
                title="地域交通システム実証業務委託",
                url="https://example.com/traffic-profile",
                source_type="proposal",
                summary="地域交通の再編と交通データ活用を進める",
                purpose="交通システムの実証と再編方針を作る",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-04-15",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 地域交流部 交通政策課",
                        person_name="田中 太郎",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="traffic",
                        name_quality="full_name",
                        source_confidence=0.96,
                    )
                ],
                fetched_at="2026-04-15T12:00:00",
            )
            save_project_record(record, db_path)

            roster = fetch_staff_roster(db_path=db_path)
            region_group = next(group for group in roster["department_groups"] if group["top_unit"] == "地域交流部")

            self.assertIn("交通政策課", region_group["child_departments"])
            self.assertTrue(any(token in region_group["focus_keywords"] for token in ("交通", "システム", "実証")))
            self.assertIn("田中 太郎", region_group["sample_people"])

    def test_fetch_department_profiles_returns_metadata_and_people(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            record = ProjectRecord(
                title="地域交通システム再編方針策定業務",
                url="https://example.com/department-profile",
                source_type="proposal",
                summary="交通システムの再編と観光動線の見直し",
                purpose="地域交通の再編方針を整理する",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-04-20",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 地域交流部 交通政策課",
                        person_name="田中 太郎",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="department-profile",
                        name_quality="full_name",
                        source_confidence=0.94,
                    )
                ],
                fetched_at="2026-04-20T12:00:00",
            )
            save_project_record(record, db_path)

            profiles = fetch_department_profiles(db_path=db_path)
            traffic = next(item for item in profiles if "交通政策課" in item["name"])

            self.assertEqual(traffic["top_unit"], "地域交流部")
            self.assertTrue(any(token in traffic["focus_keywords"] for token in ("交通", "システム", "再編")))
            self.assertIn("田中 太郎", traffic["visible_people"])
            self.assertEqual(traffic["recent_projects"][0]["title"], "地域交通システム再編方針策定業務")

    def test_group_department_profiles_by_top_unit_aggregates_themes_and_people(self):
        grouped = group_department_profiles_by_top_unit(
            [
                {
                    "name": "交通政策課",
                    "top_unit": "地域交流部",
                    "top_url": "https://example.com/top",
                    "project_count": 4,
                    "appearance_count": 2,
                    "mention_count": 1,
                    "focus_keywords": ["交通", "観光", "再編"],
                    "visible_people": ["田中 太郎", "佐藤 花子"],
                },
                {
                    "name": "観光課",
                    "top_unit": "地域交流部",
                    "top_url": "https://example.com/top",
                    "project_count": 3,
                    "appearance_count": 1,
                    "mention_count": 1,
                    "focus_keywords": ["観光", "宿泊", "周遊"],
                    "visible_people": ["佐藤 花子", "中村 次郎"],
                },
                {
                    "name": "産業政策課",
                    "top_unit": "産業労働部",
                    "top_url": "",
                    "project_count": 2,
                    "appearance_count": 0,
                    "mention_count": 1,
                    "focus_keywords": ["産業", "雇用"],
                    "visible_people": ["高橋 一郎"],
                },
            ]
        )

        self.assertEqual(grouped[0]["top_unit"], "地域交流部")
        self.assertEqual(grouped[0]["department_count"], 2)
        self.assertEqual(grouped[0]["project_count"], 7)
        self.assertIn("観光", grouped[0]["representative_themes"])
        self.assertIn("田中 太郎", grouped[0]["sample_people"])
        self.assertEqual(grouped[0]["departments"][0]["name"], "交通政策課")

    def test_fetch_network_snapshot_filters_to_selected_top_unit(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            transport_record = ProjectRecord(
                title="地域交通システム再編方針策定業務",
                url="https://example.com/network-transport",
                source_type="proposal",
                summary="交通再編",
                purpose="交通網を見直す",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-04-20",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 地域交流部 交通政策課",
                        person_name="田中 太郎",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="transport",
                        name_quality="full_name",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2026-04-20T12:00:00",
            )
            industry_record = ProjectRecord(
                title="産業成長支援業務",
                url="https://example.com/network-industry",
                source_type="proposal",
                summary="産業振興",
                purpose="産業成長を支援する",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-04-21",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 産業労働部 産業政策課",
                        person_name="山口 次郎",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="industry",
                        name_quality="full_name",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2026-04-21T12:00:00",
            )
            save_project_record(transport_record, db_path)
            save_project_record(industry_record, db_path)

            payload = fetch_network_snapshot(
                db_path=db_path,
                top_unit_filter="地域交流部",
                project_limit=20,
            )

            self.assertEqual(payload["selected_top_unit"], "地域交流部")
            self.assertEqual(payload["stats"]["project_count"], 1)
            self.assertEqual(payload["stats"]["department_count"], 1)
            self.assertGreaterEqual(payload["stats"]["topic_count"], 1)
            self.assertEqual(len(payload["clusters"]), 1)
            self.assertEqual(payload["clusters"][0]["top_unit"], "地域交流部")
            self.assertTrue(all(node["group"] == "地域交流部" for node in payload["graph"]["nodes"]))
            self.assertTrue(any(node["type"] == "topic" for node in payload["graph"]["nodes"]))

    def test_save_project_record_creates_project_topics_and_person_topic_rollups(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            record = ProjectRecord(
                title="子育て支援デジタル相談体制構築業務",
                url="https://example.com/topic-rollup-project",
                source_type="proposal",
                summary="子育て支援と相談体制をデジタルで強化する",
                purpose="こども家庭の相談導線を整備する",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-05-01",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 こども未来課 子育てし大県推進担当",
                        person_name="山田 花子",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="topic-rollup",
                        name_quality="full_name",
                        source_confidence=0.96,
                    )
                ],
                fetched_at="2026-05-01T12:00:00",
            )
            save_project_record(record, db_path)

            detail = fetch_project_detail(1, db_path)
            people = fetch_people(limit=10, db_path=db_path)
            person_detail = fetch_person_detail(people[0]["person_key"], db_path)

            self.assertTrue(detail["topic_links"])
            self.assertTrue(any(item["name"] in {"子育て", "相談", "デジタル"} for item in detail["topic_links"]))
            self.assertTrue(person_detail["topic_rollups"])
            self.assertTrue(any(item["topic_name"] in {"子育て", "相談", "デジタル"} for item in person_detail["topic_rollups"]))

    def test_import_policy_sources_csv_links_priority_topics_to_projects(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            csv_path = Path(tmpdir) / "policy_sources.csv"
            init_db(db_path)

            record = ProjectRecord(
                title="地域交通システム再編方針策定業務",
                url="https://example.com/policy-linked-project",
                source_type="proposal",
                summary="地域交通と観光周遊を再編する",
                purpose="持続可能な地域交通を整える",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-04-20",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 地域交流部 交通政策課",
                        person_name="田中 太郎",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="policy-source-link",
                        name_quality="full_name",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2026-04-20T12:00:00",
            )
            save_project_record(record, db_path)

            csv_path.write_text(
                "\n".join(
                    [
                        "source_type,title,url,published_at,summary,raw_text,topic_names",
                        'governor_press,"令和8年度重点施策記者会見",https://example.com/policy-source,2026-04-01,"地域交通と観光周遊を強化する","県として地域交通と観光の連携を重点化する","地域交通|観光周遊"',
                    ]
                ),
                encoding="utf-8",
            )

            counts = import_policy_sources_csv(csv_path, db_path)
            detail = fetch_project_detail(1, db_path)
            topic_index = fetch_policy_topic_index(db_path=db_path)

            self.assertEqual(counts["sources"], 1)
            self.assertTrue(any(item["is_priority"] for item in detail["topic_links"]))
            self.assertTrue(any(item["name"] == "地域交通" and item["origin_type"] == "policy_source" for item in detail["topic_links"]))
            self.assertTrue(any(item["name"] == "地域交通" for item in topic_index))

    def test_fetch_policy_topic_index_returns_people_and_projects(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            record = ProjectRecord(
                title="観光周遊促進業務",
                url="https://example.com/topic-index-project",
                source_type="proposal",
                summary="観光周遊と宿泊消費を伸ばす",
                purpose="県内周遊を促進する",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-06-02",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 地域交流部 観光課",
                        person_name="佐藤 花子",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="topic-index",
                        name_quality="full_name",
                        source_confidence=0.95,
                    )
                ],
                fetched_at="2026-06-02T12:00:00",
            )
            save_project_record(record, db_path)

            topics = fetch_policy_topic_index(db_path=db_path)

            self.assertTrue(topics)
            self.assertTrue(topics[0]["projects"])
            self.assertTrue(topics[0]["people"])

    def test_compute_slot_timeline_recommendations_prefers_distinct_slots_for_same_year_cross_department_surnames(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"
            init_db(db_path)

            first_record = ProjectRecord(
                title="cross-dept-1",
                url="https://example.com/cross-dept-1",
                source_type="press_release",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-04-10",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 地域交流部 交通政策課",
                        person_name="田中",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="first",
                        name_quality="surname_only",
                        source_confidence=0.88,
                    )
                ],
                fetched_at="2026-04-10T12:00:00",
            )
            second_record = ProjectRecord(
                title="cross-dept-2",
                url="https://example.com/cross-dept-2",
                source_type="press_release",
                summary="",
                purpose="",
                budget="",
                application_deadline="",
                submission_deadline="",
                published_at="2026-08-02",
                raw_text="raw",
                html_text="html",
                pdf_urls=[],
                zip_urls=[],
                person_mentions=[
                    PersonMention(
                        department_name="佐賀県 産業労働部 産業政策課",
                        person_name="田中",
                        person_key="",
                        person_role="contact",
                        contact_email="",
                        contact_phone="",
                        extracted_section="second",
                        name_quality="surname_only",
                        source_confidence=0.88,
                    )
                ],
                fetched_at="2026-08-02T12:00:00",
            )
            save_project_record(first_record, db_path)
            save_project_record(second_record, db_path)

            conn = get_connection(db_path)
            mention_rows = conn.execute(
                "SELECT id, raw_department_name FROM person_mentions ORDER BY id ASC"
            ).fetchall()
            first_mention_id = int(mention_rows[0]["id"])
            second_mention_id = int(mention_rows[1]["id"])

            first_department_id = get_or_create_department(conn, "佐賀県 地域交流部 交通政策課")
            second_department_id = get_or_create_department(conn, "佐賀県 産業労働部 産業政策課")
            conn.executemany(
                """
                INSERT INTO employee_slots (
                    slot_key, normalized_person_name, display_name, name_quality, department_id,
                    raw_department_name, title_raw, active_from, active_to, slot_confidence
                )
                VALUES (?, ?, ?, 'full_name', ?, ?, '主事', '2026-04-01', '', 0.9)
                """,
                [
                    (
                        "slot-tanaka-a",
                        "田中太郎",
                        "田中 太郎",
                        first_department_id,
                        "佐賀県 地域交流部 交通政策課",
                    ),
                    (
                        "slot-tanaka-b",
                        "田中次郎",
                        "田中 次郎",
                        second_department_id,
                        "佐賀県 産業労働部 産業政策課",
                    ),
                ],
            )
            slot_rows = conn.execute(
                "SELECT id, slot_key FROM employee_slots ORDER BY id ASC"
            ).fetchall()
            slot_a_id = int(slot_rows[0]["id"])
            slot_b_id = int(slot_rows[1]["id"])

            conn.executemany(
                """
                INSERT INTO slot_candidates (
                    person_mention_id, employee_slot_id, candidate_score, matched_by, notes
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                [
                    (first_mention_id, slot_a_id, 0.63, "seed", "交通政策課の田中候補"),
                    (first_mention_id, slot_b_id, 0.61, "seed", "別部署の田中候補"),
                    (second_mention_id, slot_a_id, 0.65, "seed", "別部署の田中候補"),
                    (second_mention_id, slot_b_id, 0.64, "seed", "産業政策課の田中候補"),
                ],
            )
            conn.commit()

            recommendations = compute_slot_timeline_recommendations(
                conn,
                [first_mention_id, second_mention_id],
            )
            conn.close()

            self.assertEqual(recommendations[first_mention_id]["employee_slot_id"], slot_a_id)
            self.assertEqual(recommendations[second_mention_id]["employee_slot_id"], slot_b_id)

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
