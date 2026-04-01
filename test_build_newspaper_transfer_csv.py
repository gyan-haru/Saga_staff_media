import unittest

from build_newspaper_transfer_csv import (
    discover_article_links,
    extract_article_rows,
    parse_transfer_line,
)


class BuildNewspaperTransferCsvTestCase(unittest.TestCase):
    def test_discover_article_links_finds_transfer_articles(self):
        html = """
        <html>
          <body>
            <a href="/articles/-/1428413">＜佐賀県職員人事異動 2025年4月1日＞部長級、副部長級</a>
            <a href="/articles/-/9999999">一般記事</a>
            <a href="/articles/-/1428422">＜佐賀県職員人事異動 2025年4月1日＞課長級</a>
          </body>
        </html>
        """

        links = discover_article_links(html, "https://www.saga-s.co.jp/feature/ud/jinji_2025")

        self.assertEqual(
            [item["url"] for item in links],
            [
                "https://www.saga-s.co.jp/articles/-/1428413",
                "https://www.saga-s.co.jp/articles/-/1428422",
            ],
        )

    def test_parse_transfer_line_expands_same_reference(self):
        first, previous, same_prefix = parse_transfer_line(
            "政策部部長（総務副部長）前田 直紀",
            "部長級",
            "＜佐賀県職員人事異動 2025年4月1日＞部長級、副部長級",
            "",
            "",
        )
        second, previous, same_prefix = parse_transfer_line(
            "同政策統括監（県土整備部長）横尾 秀憲",
            "部長級",
            "＜佐賀県職員人事異動 2025年4月1日＞部長級、副部長級",
            previous,
            same_prefix,
        )
        third, _, _ = parse_transfer_line(
            "同（佐賀市）稲又 宏之",
            "副部長級",
            "＜佐賀県職員人事異動 2025年4月1日＞部長級、副部長級",
            "県土整備部副部長",
            "県土整備部",
        )

        self.assertIsNotNone(first)
        self.assertEqual(first["to_department"], "政策部")
        self.assertEqual(first["to_title"], "部長")
        self.assertEqual(first["from_department"], "総務部")
        self.assertEqual(first["from_title"], "副部長")

        self.assertIsNotNone(second)
        self.assertEqual(second["to_department"], "政策部")
        self.assertEqual(second["to_title"], "政策統括監")
        self.assertEqual(second["from_department"], "県土整備部")
        self.assertEqual(second["from_title"], "部長")

        self.assertIsNotNone(third)
        self.assertEqual(third["to_department"], "県土整備部")
        self.assertEqual(third["from_department"], "佐賀市")

    def test_parse_transfer_line_uses_last_department_boundary_fallback(self):
        parsed, _, _ = parse_transfer_line(
            "同さがデザインディレクター(政策企画主幹)近野 繭子",
            "課長級",
            "＜佐賀県職員人事異動 2025年4月1日＞課長級",
            "政策部政策統括監",
            "政策部",
        )

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["to_department"], "政策部")
        self.assertEqual(parsed["to_title"], "さがデザインディレクター")

    def test_parse_transfer_line_handles_academy_and_school_specialists(self):
        parsed, previous, same_prefix = parse_transfer_line(
            "虹の松原学園主任児童自立支援専門員(こども家庭主任主査)斉藤 考生",
            "係長級(事務)",
            "＜佐賀県職員人事異動 2025年4月1日付＞係長級(事務)",
            "",
            "",
        )
        follow, _, _ = parse_transfer_line(
            "同主査(産技学院技師)肥山 隼人",
            "係長級(技術)",
            "＜佐賀県職員人事異動 2025年4月1日付＞係長級(技術)",
            "産業技術学院主任職業指導員",
            "産業技術学院",
        )

        self.assertIsNotNone(parsed)
        self.assertEqual(parsed["to_department"], "虹の松原学園")
        self.assertEqual(parsed["to_title"], "主任児童自立支援専門員")
        self.assertTrue(parsed["from_title"].endswith("主任主査"))

        self.assertIsNotNone(follow)
        self.assertEqual(follow["to_department"], "産業技術学院")
        self.assertEqual(follow["to_title"], "主査")
        self.assertEqual(follow["from_department"], "産技学院")
        self.assertEqual(follow["from_title"], "技師")

    def test_extract_article_rows_reads_article_body_lists(self):
        html = """
        <html>
          <head>
            <meta property="article:published_time" content="2025-03-19T17:00:00+09:00">
          </head>
          <body>
            <article>
              <h1>＜佐賀県職員人事異動 2025年4月1日＞部長級、副部長級</h1>
              <div class="article-body cXenseParse">
                <p>2025年4月1日付の佐賀県職員人事（部長級、副部長級）情報を掲載しています。</p>
                <h3>部長級</h3>
                <ul>
                  <li>政策部部長（総務副部長）前田 直紀</li>
                  <li>同政策統括監（県土整備部長）横尾 秀憲</li>
                </ul>
              </div>
            </article>
          </body>
        </html>
        """

        parsed = extract_article_rows(html, "https://www.saga-s.co.jp/articles/-/1428413")

        self.assertEqual(parsed["title"], "<佐賀県職員人事異動 2025年4月1日>部長級、副部長級")
        self.assertEqual(parsed["published_at"], "2025-03-19")
        self.assertEqual(parsed["effective_date"], "2025-04-01")
        self.assertEqual(len(parsed["rows"]), 2)
        self.assertEqual(parsed["rows"][0]["person_name"], "前田 直紀")
        self.assertEqual(parsed["rows"][1]["to_department"], "政策部")


if __name__ == "__main__":
    unittest.main()
