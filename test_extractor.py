import unittest

from extractor import (
    detect_published_at,
    find_department_and_person,
    find_person_mentions,
    is_valid_person_name,
    parse_japanese_date,
    replace_kanji_numbers_in_text,
    summarize_text,
)


class ExtractorTestCase(unittest.TestCase):
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
        self.assertEqual(person, "")
        self.assertEqual(role, "contact")

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

    def test_extract_person_mentions_supports_compatibility_kanji(self):
        text = """
        6 問い合わせ先
        佐賀県流通・貿易課
        担当 宮﨑、奥薗、浦、1丸
        TEL 0952-25-7252
        """

        mentions = find_person_mentions(text)

        self.assertEqual([mention.person_name for mention in mentions], ["宮﨑", "奥薗", "浦"])

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
