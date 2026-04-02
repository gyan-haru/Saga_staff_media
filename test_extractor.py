import urllib.error
import unittest
from unittest import mock

import extractor as extractor_module
from extractor import (
    clean_department_name,
    detect_published_at,
    extract_pdf_text,
    fetch_url_bytes,
    find_deadline,
    find_department_and_person,
    find_person_mentions,
    format_date_iso,
    is_valid_department_name,
    is_valid_person_name,
    parse_japanese_date,
    replace_kanji_numbers_in_text,
    summarize_text,
)


class ExtractorTestCase(unittest.TestCase):
    def test_fetch_url_bytes_retries_connection_reset(self):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b"ok"

        with mock.patch(
            "extractor.urllib.request.urlopen",
            side_effect=[
                urllib.error.URLError(OSError(54, "Connection reset by peer")),
                FakeResponse(),
            ],
        ) as mocked_urlopen:
            payload = fetch_url_bytes("https://example.com/test.pdf", attempts=2, timeout=1)

        self.assertEqual(payload, b"ok")
        self.assertEqual(mocked_urlopen.call_count, 2)

    def test_extract_pdf_text_returns_empty_for_broken_pdf(self):
        with mock.patch("extractor.fetch_url_bytes", return_value=b"broken-pdf"), mock.patch("builtins.print"):
            if extractor_module.fitz is not None:
                with mock.patch("extractor.fitz.open", side_effect=RuntimeError("broken pdf")):
                    text = extract_pdf_text("https://example.com/broken.pdf")
            else:
                with mock.patch("extractor.PdfReader", side_effect=RuntimeError("broken pdf")):
                    text = extract_pdf_text("https://example.com/broken.pdf")

        self.assertEqual(text, "")

    def test_extract_pdf_text_falls_back_to_pypdf_when_fitz_is_unavailable(self):
        class FakePage:
            def extract_text(self):
                return "fallback text"

        class FakeReader:
            def __init__(self, _stream):
                self.pages = [FakePage()]

        with mock.patch("extractor.fetch_url_bytes", return_value=b"pdf-bytes"), mock.patch(
            "extractor.fitz", None
        ), mock.patch("extractor.PdfReader", FakeReader):
            text = extract_pdf_text("https://example.com/fallback.pdf")

        self.assertEqual(text, "fallback text")

    def test_press_release_header_extracts_department_and_person(self):
        text = """
        最終更新日:
        2026年2月9日
        河川砂防課 城原川ダム等対策室
        担当者 廣田
        内線 2822 直通 952-25-7183
        E-mail kasensabou@pref.saga.lg.jp
        """

        department, person, role, snippet = find_department_and_person(text)

        self.assertEqual(department, "河川砂防課 城原川ダム等対策室")
        self.assertEqual(person, "廣田")
        self.assertEqual(role, "contact")
        self.assertIn("担当者 廣田", snippet)

    def test_contact_block_without_person_keeps_department_only(self):
        text = """
        5 問い合わせ先
        佐賀県 県土整備部 まちづくり課 公園担当
        所在地 〒840-8570 佐賀市城内1丁目1番59号
        電話番号 952-25-7159
        メールアドレス machizukuri@pref.saga.lg.jp
        """

        department, person, role, _snippet = find_department_and_person(text)

        self.assertEqual(department, "佐賀県 県土整備部 まちづくり課 公園担当")

    def test_clean_department_name_strips_submission_prefix_and_keeps_specific_unit(self):
        department = clean_department_name("申込書提出先:佐賀県総務部税政課税務政策・ふるさと納税担当")

        self.assertEqual(department, "佐賀県総務部税政課税務政策・ふるさと納税担当")

    def test_clean_department_name_collapses_duplicate_department_suffix(self):
        department = clean_department_name("道路課課 道路安全推進室")

        self.assertEqual(department, "道路課 道路安全推進室")

    def test_clean_department_name_strips_contact_bracket_prefix(self):
        department = clean_department_name("問い合わせ先】佐賀県障害福祉課就労支援室")

        self.assertEqual(department, "佐賀県障害福祉課就労支援室")

    def test_clean_department_name_strips_wrapped_contact_bracket_prefix(self):
        department = clean_department_name("【問い合わせ先】佐賀県障害福祉課就労支援室")

        self.assertEqual(department, "佐賀県障害福祉課就労支援室")

    def test_is_valid_department_name_rejects_address_like_room(self):
        self.assertFalse(is_valid_department_name("事務所住所:佐賀市唐人2-5-15 まちなかオフィスTOJIN 館2号室"))

    def test_department_line_with_person_at_end_extracts_person(self):
        text = """
        16 書類等提出先及び問い合わせ先
        佐賀県男女参画・こども局 こども未来課 子育てし大県推進担当 藤井
        """

        department, person, _role, _snippet = find_department_and_person(text)

        self.assertEqual(department, "佐賀県男女参画・こども局 こども未来課 子育てし大県推進担当")
        self.assertEqual(person, "藤井")

    def test_midline_tantousha_label_does_not_leak_into_department(self):
        text = """
        令和7年11月18日
        アスリート育成支援チーム 育成支援担当 担当者 山田、峯
        内線 2708 直通 952-25-7528
        E-mail: athlete-shien@pref.saga.lg.jp
        """

        department, person, _role, _snippet = find_department_and_person(text)

        self.assertEqual(department, "アスリート育成支援チーム 育成支援担当")
        self.assertEqual(person, "山田")

    def test_parenthetical_person_annotation_extracts_person(self):
        text = """
        3 手続等に関する事項
        (1)担当課 佐賀県 政策部 広報広聴課 広聴担当(担当:川久保、緒方)
        住所 840-8570 佐賀県佐賀市城内1-1-59
        電話番号0952-25-7351
        電子メールアドレス kouhou-kouchou@pref.saga.lg.jp
        """

        department, person, _role, _snippet = find_department_and_person(text)

        self.assertEqual(department, "佐賀県 政策部 広報広聴課 広聴担当")
        self.assertEqual(person, "川久保")

    def test_parenthetical_person_annotation_on_department_line_extracts_person(self):
        text = """
        (3)申込先
        佐賀県産業労働部産業政策課(担当:関口)
        Mail:sangyouseisaku@pref.saga.lg.jp
        """

        department, person, _role, _snippet = find_department_and_person(text)

        self.assertEqual(department, "佐賀県産業労働部産業政策課")
        self.assertEqual(person, "関口")

    def test_find_person_mentions_keeps_multiple_people_on_same_line(self):
        text = """
        令和7年11月18日
        アスリート育成支援チーム 育成支援担当 担当者 山田、峯
        内線 2708 直通 952-25-7528
        E-mail: athlete-shien@pref.saga.lg.jp
        """

        mentions = find_person_mentions(text)

        self.assertEqual(len(mentions), 2)
        self.assertEqual([mention.person_name for mention in mentions], ["山田", "峯"])
        self.assertTrue(all(mention.department_name == "アスリート育成支援チーム 育成支援担当" for mention in mentions))

    def test_find_person_mentions_extracts_multiple_people_from_annotation(self):
        text = """
        問い合わせ先
        佐賀県政策部広報広聴課広聴担当(担当:川久保、緒方)
        電話:0952-25-7351
        """

        mentions = find_person_mentions(text)

        self.assertEqual(len(mentions), 2)
        self.assertEqual([mention.person_name for mention in mentions], ["川久保", "緒方"])
        self.assertTrue(all(mention.contact_phone == "0952-25-7351" for mention in mentions))

    def test_wrapped_department_person_line_extracts_inner_department(self):
        text = """
        佐賀県産業人材確保プロジェクト推進会議事務局
        (佐賀県 産業労働部 産業人材課 産業人材担当 中村)
        TEL :0952-25-7310
        """

        department, person, _role, _snippet = find_department_and_person(text)

        self.assertEqual(department, "佐賀県 産業労働部 産業人材課 産業人材担当")
        self.assertEqual(person, "中村")

    def test_department_suffix_line_does_not_invent_person_from_tantou(self):
        text = """
        3 提出場所
        佐賀県地域交流部多文化共生さが推進課 企画・交流担当
        (佐賀市城内1丁目1番59号 新館7階)
        """

        department, person, role, _snippet = find_department_and_person(text)

        self.assertEqual(department, "佐賀県地域交流部多文化共生さが推進課 企画・交流担当")
        self.assertEqual(person, "")
        self.assertEqual(role, "contact")

    def test_history_department_line_does_not_extract_fake_person(self):
        text = """
        (7)問い合わせ先について
        佐賀市 歴史・文化課
        TEL 952-40-7105
        """

        department, person, role, _snippet = find_department_and_person(text)

        self.assertEqual(department, "佐賀市 歴史・文化課")
        self.assertEqual(person, "")
        self.assertEqual(role, "contact")

    def test_contact_block_keeps_full_name_after_department_line(self):
        text = """
        3 問い合わせ、各手続の提出先
        佐賀県健康福祉部障害福祉課
        就労支援室長 原田 将
        TEL:0952-25-7401
        """

        mentions = find_person_mentions(text)

        self.assertEqual(len(mentions), 1)
        self.assertEqual(mentions[0].department_name, "佐賀県健康福祉部障害福祉課")
        self.assertEqual(mentions[0].person_name, "原田 将")

    def test_contact_block_stops_before_appendix_noise(self):
        text = """
        6 問合せ先、担当
        佐賀県 文化・観光局 文化課 [担当]佐賀復権推進チーム
        TEL 952-25-7236 FAX 952-25-7179 メール culture_art@pref.saga.lg.jp
        (別表)
        佐賀県伝承芸能保存活用事業(まつりびと) 撮影対象芸能リスト(H25~R7年度)
        """

        department, person, role, _snippet = find_department_and_person(text)

        self.assertEqual(department, "佐賀県 文化・観光局 文化課 佐賀復権推進チーム")
        self.assertEqual(person, "")
        self.assertEqual(role, "contact")

    def test_inquiry_prefix_line_strips_to_department_only(self):
        text = """
        5 本件に係る問い合わせ先
        医務課 地域医療担当 TEL:0952-25-7033
        """

        department, person, role, _snippet = find_department_and_person(text)

        self.assertEqual(department, "医務課 地域医療担当")
        self.assertEqual(person, "")
        self.assertEqual(role, "contact")

    def test_inline_department_only_line_keeps_department_without_fake_person(self):
        text = """
        問合せ先
        佐賀県県民環境部 県民協働課 協働社会推進担当
        電話 0952-25-7374
        E-mail kenminkyoudou@pref.saga.lg.jp
        """

        mentions = find_person_mentions(text)

        self.assertEqual(len(mentions), 1)
        self.assertEqual(mentions[0].department_name, "佐賀県県民環境部 県民協働課 協働社会推進担当")
        self.assertEqual(mentions[0].person_name, "")

    def test_table_header_line_is_not_mistaken_for_department(self):
        text = """
        本 文:会社名等、担当部署名、参加者氏名、電話番号を記載
        担当者名 電話番号 ファックス番号 メールアドレス
        """

        department, person, role, _snippet = find_department_and_person(text)

        self.assertEqual(department, "")
        self.assertEqual(person, "")
        self.assertEqual(role, "")

    def test_inline_department_with_multiple_people_extracts_both(self):
        text = """
        問い合わせ先
        佐賀県県民環境部脱炭素社会推進課 企画担当 井上、光枝
        電話:0952-25-7079
        """

        mentions = find_person_mentions(text)

        self.assertEqual([mention.person_name for mention in mentions], ["井上", "光枝"])
        self.assertTrue(all(mention.department_name == "佐賀県県民環境部脱炭素社会推進課 企画担当" for mention in mentions))

    def test_extract_person_mentions_supports_compatibility_kanji(self):
        text = """
        6 問い合わせ先
        佐賀県流通・貿易課
        担当 宮﨑、奥薗、浦、1丸
        TEL 0952-25-7252
        """

        mentions = find_person_mentions(text)

        self.assertEqual([mention.person_name for mention in mentions], ["宮﨑", "奥薗", "浦"])

    def test_find_person_mentions_extracts_role_only_full_name(self):
        text = """
        担当者:教頭 平方 伸之
        E-mail: sagakougyoukoukou@education.saga.jp
        """

        mentions = find_person_mentions(text)

        self.assertEqual(len(mentions), 1)
        self.assertEqual(mentions[0].person_name, "平方 伸之")

    def test_find_person_mentions_extracts_multiple_people_with_role_prefixes(self):
        text = """
        担当者 副校長 中村 浩子、教諭 樋口 英司
        E-mail: karatsu@example.jp
        """

        mentions = find_person_mentions(text)

        self.assertEqual([mention.person_name for mention in mentions], ["中村 浩子", "樋口 英司"])

    def test_extract_department_and_people_from_compound_line_keeps_group_without_extra_suffix(self):
        text = "担当:教育DX推進グループ係長 岩谷祥史 氏"

        mentions = find_person_mentions(text + "\n電話 0952-25-0000")

        self.assertEqual(len(mentions), 1)
        self.assertEqual(mentions[0].department_name, "教育DX推進グループ")
        self.assertEqual(mentions[0].person_name, "岩谷祥史")

    def test_location_line_does_not_extract_web_event_as_person(self):
        text = """
        参加方法
        場所 Web開催
        申込先 佐賀県産業労働部産業政策課
        Mail:sangyouseisaku@pref.saga.lg.jp
        """

        department, person, _role, _snippet = find_department_and_person(text)

        self.assertEqual(department, "佐賀県産業労働部産業政策課")
        self.assertNotEqual(person, "Web開催")

    def test_invalid_person_name_rejects_department_fragments(self):
        for candidate in ("玄海創生", "文化財保護", "時までの", "時までに書留", "calogeras"):
            self.assertFalse(is_valid_person_name(candidate))

    def test_invalid_person_name_rejects_location_and_facility_strings(self):
        for candidate in ("佐賀県佐賀市", "佐賀市城内", "佐賀県立名護屋城博物館", "県立図書館"):
            self.assertFalse(is_valid_person_name(candidate))

    def test_invalid_person_name_rejects_group_area_and_sentence_fragments(self):
        for candidate in ("設備建設グループ", "その周辺エリア", "スポーツマネジメント学科", "県下全域", "運営することにより"):
            self.assertFalse(is_valid_person_name(candidate))

    def test_opening_place_line_does_not_extract_facility_as_person(self):
        text = """
        開札場所 佐賀県立名護屋城博物館
        """

        department, person, role, _snippet = find_department_and_person(text)

        self.assertEqual(department, "")
        self.assertEqual(person, "")
        self.assertEqual(role, "")

    def test_execution_place_line_does_not_extract_city_as_person(self):
        text = """
        (4)履行場所 佐賀県佐賀市、唐津市、伊万里市及び玄海町他
        """

        department, person, role, _snippet = find_department_and_person(text)

        self.assertEqual(department, "")
        self.assertEqual(person, "")
        self.assertEqual(role, "")

    def test_group_contact_line_keeps_department_without_fake_person(self):
        text = """
        九州電力送配電株式会社佐賀配電事業所 設備建設グループ
        電話0952-33-1171
        """

        department, person, role, _snippet = find_department_and_person(text)

        self.assertEqual(department, "九州電力送配電株式会社佐賀配電事業所 設備建設グループ")
        self.assertEqual(person, "")
        self.assertEqual(role, "contact")

    def test_business_place_line_does_not_extract_area_phrase(self):
        text = """
        (2)業務場所 主として名護屋城跡及び名護屋城博物館、その周辺エリア
        """

        department, person, role, _snippet = find_department_and_person(text)

        self.assertEqual(department, "")
        self.assertEqual(person, "")
        self.assertEqual(role, "")

    def test_sentence_fragment_does_not_extract_fake_person(self):
        text = """
        率化サポートセンター (以下 「センター」という。)」を設置し、運営することにより、事業
        """

        department, person, role, _snippet = find_department_and_person(text)

        self.assertEqual(department, "")
        self.assertEqual(person, "")
        self.assertEqual(role, "")

    def test_two_token_department_line_does_not_invent_person(self):
        text = """
        連絡先
        佐賀県出納局総務事務センター 給与
        Mail:example@pref.saga.lg.jp
        """

        department, person, role, _snippet = find_department_and_person(text)

        self.assertEqual(department, "佐賀県出納局総務事務センター 給与")
        self.assertEqual(person, "")
        self.assertEqual(role, "contact")

    def test_parse_japanese_date_supports_spaces(self):
        parsed = parse_japanese_date("令和 8 年 3 月 27 日")
        self.assertIsNotNone(parsed)
        self.assertEqual(parsed.date().isoformat(), "2026-03-27")

    def test_parse_japanese_date_returns_none_for_invalid_month(self):
        self.assertIsNone(parse_japanese_date("2026/31/10"))
        self.assertEqual(format_date_iso("2026/31/10"), "")

    def test_format_date_iso_infers_year_for_month_day_deadline(self):
        self.assertEqual(format_date_iso("3月 2日（月曜日）午後4時", default_year=2026), "2026-03-02")

    def test_find_deadline_extracts_month_day_from_schedule_block(self):
        text = """
        スケジュール
        令和8年2月17日 公募開始
        3月2日（月曜日）午後4時 参加申込書提出締切
        3月10日（火曜日）午後5時 企画提案書提出締切
        """

        application_deadline = find_deadline(text, ["参加申込書", "参加申込締切"])
        submission_deadline = find_deadline(text, ["企画提案書提出締切", "提案書提出"])

        self.assertEqual(format_date_iso(application_deadline, default_year=2026), "2026-03-02")
        self.assertEqual(format_date_iso(submission_deadline, default_year=2026), "2026-03-10")

    def test_detect_published_at_prefers_labeled_date(self):
        text = """
        最終更新日:
        2026年2月9日
        河川砂防課 城原川ダム等対策室
        担当者 廣田
        """

        self.assertEqual(detect_published_at(text), "2026-02-09")

    def test_kanji_number_replacement_keeps_person_names(self):
        self.assertEqual(replace_kanji_numbers_in_text("担当者 一郎"), "担当者 一郎")
        self.assertEqual(replace_kanji_numbers_in_text("令和八年三月二十七日"), "令和8年3月27日")

    def test_summarize_text_skips_metadata_lines(self):
        text = """
        記事タイトル
        最終更新日:
        2026年2月9日
        担当者 廣田
        実証実験を県内で開始します。
        """

        self.assertEqual(summarize_text(text, title="記事タイトル"), "実証実験を県内で開始します。")


if __name__ == "__main__":
    unittest.main()
