# 年次の人事異動更新フロー

## 1. ねらい

人事異動データは、毎日クロールする対象ではなく、年に1回から数回のまとまった更新で十分です。
そのため、このプロジェクトでは `Codex管理のCSV` を正本にして、必要な時にまとめてDBへ反映する運用を推奨します。

## 2. 更新に必要な情報

最低限必要なのは次の列です。

- `effective_date`
  発令日
- `person_name`
  氏名
- `from_department`
  異動前部署
- `to_department`
  異動後部署

あると精度が上がるもの:

- `from_title`
- `to_title`
- `url`
- `publisher`
- `evidence_snippet`

## 3. どこに置くか

CSV は [data/transfers](/Users/harrn/Desktop/saga_staff_media/data/transfers) に年ごとに置く。
元ソースのURLや状態は [source_index.csv](/Users/harrn/Desktop/saga_staff_media/data/transfers/source_index.csv) に記録する。

例:

- `official_2026.csv`
- `official_2025.csv`
- `newspaper_2024.csv`

列は [transfer_template.csv](/Users/harrn/Desktop/saga_staff_media/data/transfers/transfer_template.csv) に合わせる。

## 4. 取り込みコマンド

単体:

```bash
python import_transfers.py data/transfers/official_2026.csv
```

フォルダ一括:

```bash
python import_transfer_directory.py
```

## 5. 運用イメージ

1. 年度末や4月の人事異動表を CSV 化する
2. 使うURLや媒体を `data/transfers/source_index.csv` に登録する
3. `data/transfers/official_2024.csv` のような年次ファイルに保存する
4. `python validate_transfer_csv.py --dir data/transfers` で列や日付を検証する
5. `python import_transfer_directory.py` を実行する
6. 人物ページで `現在の部署候補`、`これまでの部署`、`人事異動履歴` を確認する
7. 案件ページの `人事異動DBからの候補` を見て、担当者不明を補完する

## 6. 佐賀新聞の扱い

佐賀新聞の人事異動特集ページは、年ごとのまとめページから級別記事をたどって CSV 化できることがあります。
このリポジトリでは `source_index.csv` に特集URLを登録したうえで、次のコマンドで年次CSVを自動生成できます。

```bash
python build_newspaper_transfer_csv.py 2025
```

うまく読めない年だけ、次のどちらかで補えば十分です。

- 人事異動特集を目視でCSVに転記する
- 読める範囲でHTMLやPDFを保存してから整形する

重要なのは `公開ソースに書かれていた異動行` を年単位で安定して残すことです。
リアルタイム性より、継続更新しやすさを優先したほうがこの用途には向いています。
