# 人事異動CSV 置き場

このフォルダは、佐賀県庁職員の人事異動データを年単位で管理するための置き場です。

おすすめ運用:

1. 年ごとに 1ファイルずつ CSV を置く
2. 列は `transfer_template.csv` に合わせる
3. 県公式の異動表がある年は `official_YYYY.csv`
4. 佐賀新聞などの補助ソースは `newspaper_YYYY.csv`
5. 取り込みは `python import_transfer_directory.py` を使う

例:

- `official_2026.csv`
- `official_2025.csv`
- `newspaper_2024.csv`

このリポジトリには、直近5年分の空ファイルを先に置いてあります。
まずは `official_2024.csv` などから埋めていけば、そのまま年次DBとして育てられます。

元ソースの一覧は [source_index.csv](/Users/harrn/Desktop/saga_staff_media/data/transfers/source_index.csv) で管理します。
URLが分かったら、まずここに `year / source_type / label / url / status / notes` を追記してから転記作業に入ると迷いにくいです。

最低限ほしい列:

- `source_type`
- `title`
- `url`
- `published_at`
- `effective_date`
- `person_name`
- `from_department`
- `to_department`
- `from_title`
- `to_title`
- `evidence_snippet`

取り込み:

```bash
python import_transfer_directory.py
```

検証:

```bash
python validate_transfer_csv.py --dir data/transfers
```
