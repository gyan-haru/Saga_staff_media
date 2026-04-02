# 佐賀県 企画×担当者アーカイブ 仕様書

## 1. 位置づけ

本プロジェクトは、佐賀県の公開Webページに掲載される

- コンペ
- 公募
- プレスリリース
- 担当部署
- 担当者

を継続的に収集・整理し、`企画の履歴` と `企画に関わる人の軌跡` を読めるアーカイブをつくるための基盤である。

目指すのは単なる新着通知ではない。  
公開記録を手がかりに、

- どんな企画が生まれてきたか
- その企画に誰が関わっていたか
- 異動後もどんなテーマに関わり続けているか
- そこからどんな価値観や問題意識が見えてくるか

を読める `人物ネットワーク型メディア` の土台を整えることを目的とする。

## 2. プロダクトの考え方

このプロダクトの主役は、案件単体ではなく `面白い企画に継続して関わる人` である。

県庁の公開ページを追うと、企画書、公募資料、記者発表文から、その事業の背景や意図、県として何を実現したいのかが読み取れる。さらに担当者名をたどることで、

- 以前どの部署にいたのか
- どんなテーマを扱ってきたのか
- どの企画群に連続して関わっているのか

を横断的に見られるようになる。

その結果として、「この人は地域づくりを軸に動いている」「この人は文化と観光をまたぐ企画を多く担当している」といった `企画上の一貫性` を見いだせるようにする。

最終的には、そうした人物を取材し、

- 企画に込めた思い
- 佐賀県に対する視点
- 異動しても変わらず向き合っているテーマ

を本人の言葉として蓄積できるメディアへ発展させる。

## 3. 編集方針

本システムで扱う情報は、次の3層に分ける。

### 3-1. 事実レイヤー

公開資料から確認できる事実を保存する。

- タイトル
- URL
- 公開日
- 企画概要
- 事業の趣旨
- 担当部署
- 担当者
- 予算
- 締切
- PDF / ZIP
- 根拠となる本文スニペット

### 3-2. 仮説レイヤー

公開情報から読み取れる傾向やつながりを `編集部仮説` として扱う。

例:

- この人物は子ども政策と教育政策をまたいでいる
- この人物は地域交通と観光をつなぐ案件に継続して関わっている
- この部署移動はテーマの連続性を持っている

仮説は断定せず、必ず根拠となる案件群や本文に紐づける。

### 3-3. 取材レイヤー

人物の価値観や思いは、機械推定で断定しない。  
本人インタビューや寄稿、コメントなど、明示的な一次情報として扱う。

例:

- インタビュー記事
- 取材メモ
- 本人コメント
- 編集後記

## 4. 目的

### 4-1. 主目的

- 佐賀県の公開企画情報を継続的に蓄積する
- 企画ごとの担当部署・担当者を抽出する
- 担当者ごとの関与案件一覧を閲覧可能にする
- 異動をまたいだ担当履歴を追えるようにする
- テーマの近い企画や人物同士の関係性を見える化する
- 将来的な取材メディアのためのデータ基盤を整える

### 4-2. 非目的

- 私生活や非公開情報を追跡すること
- 公開情報だけで人物の思想や人格を断定すること
- 人物をランキング化して優劣をつけること

## 5. 想定ユーザー体験

### 5-1. 閲覧者の導線

1. トップページや企画一覧から面白い事業を見つける
2. 企画詳細ページで概要・趣旨・資料・担当情報を読む
3. 担当者名を開く
4. その人物が関わった他の案件一覧を見る
5. 関与部署やテーマの変遷を追う
6. 必要に応じて取材記事や編集メモへ進む

### 5-2. 編集側の導線

1. 新規案件を取り込む
2. 抽出結果を確認し、担当者や部署の誤りを修正する
3. 同一人物の統合を行う
4. テーマタグや注目案件を付与する
5. 取材候補人物を発見する
6. 取材記事を公開する

## 6. 対象データ

### 6-1. 収集対象

佐賀県公式サイト上の `kiji` 記事ページを主対象とする。

取得元一覧ページは、runtime データ置き場の `list_sources.csv` で管理する。  
既定では repo 内の [data/list_sources.csv](/Users/harrn/Desktop/saga_staff_media/data/list_sources.csv) を使うが、`SAGA_MEDIA_DATA_DIR` を設定すると外部ディレクトリ側の `list_sources.csv` が優先される。

CSVの列:

- `url`
- `department_name`
- `source_type`

`source_type` は `proposal` または `press_release` を想定する。

### 6-2. データ種別

- `proposal`
  コンペ、公募、プロポーザル、委託募集、入札関連
- `press_release`
  記者発表、報道発表、お知らせ、開催告知、結果公表

### 6-3. 抽出対象

企画ごとに以下を抽出・保存する。

- タイトル
- URL
- 種別
- 概要
- 事業の趣旨
- 予算
- 参加申込締切
- 提案提出締切
- 公開日
- 担当部署
- 担当者
- 連絡先メール
- 電話番号
- PDF URL
- ZIP URL
- 生テキスト
- 抽出根拠スニペット

将来的には以下も追加する。

- テーマタグ
- 人物の所属履歴
- インタビュー記事
- 編集部メモ
- 関連人物リンク

## 7. 中核データモデル

### 7-1. Project

1件の企画・発表・公募・結果公表を表す。

### 7-2. Person

公開資料上の担当者を表す。  
同一人物統合は、名前正規化だけでなく、部署・時期・連絡先・管理画面での統合作業を前提とする。

### 7-3. Department

担当部署や組織単位を表す。

### 7-4. Appearance

ある人物が、ある案件に、どの部署経由で現れたかを表す。  
人物と案件の中間テーブルとして機能し、人物履歴をつくる中心データとする。

### 7-5. Tag

企画のテーマを表す。  
例:

- 観光
- 文化
- 教育
- 子ども
- スタートアップ
- 防災
- 交通

### 7-6. Interview

将来的に、人物取材記事やコメントを保存するためのデータ。

## 8. 機能仕様

### 8-1. 企画情報収集

指定した一覧ページから対象記事URLを収集する。

処理:

- 一覧ページを取得
- `kiji` を含む記事URLを探索
- 種別ごとにリンクを絞り込む
- 重複URLを除外する

### 8-2. 記事詳細解析

各記事から企画情報を抽出する。

処理:

- HTML取得
- 本文抽出
- PDF / ZIP リンク抽出
- 添付資料本文抽出
- HTML本文と添付資料本文の結合
- 正規表現・ルールベースで各項目を抽出

### 8-3. テキスト正規化

抽出前に表記ゆれを吸収する。

例:

- 全角半角の統一
- 漢数字の数値変換
- 締切表現の統一
- 空白・改行の整理

### 8-4. 担当部署・担当者抽出

問い合わせ先、提出先、記者発表ヘッダなどから担当情報を抽出する。

抽出対象:

- 部署名
- 担当者名
- role
- 抽出元スニペット

将来的には `複数担当者対応` を行う。

### 8-5. 人物統合

同姓同名や表記ゆれの問題があるため、自動統合だけで完結させない。

必要機能:

- 名前正規化
- 候補人物の突合
- 管理画面での統合作業
- 根拠付きの統合履歴

### 8-6. テーマタグ付与

案件本文からテーマタグを付与する。

用途:

- 人物ごとの関心領域を見やすくする
- 関連企画推薦に使う
- ネットワーク可視化の軸にする

### 8-7. ネットワーク生成

人物詳細ページ上で、次の関係性を表示できるようにする。

- 同じ人物が関わった別案件
- 同じ部署に現れた人物
- 同じテーマタグを持つ案件
- 異動をまたいで近いテーマを持つ人物

### 8-8. 取材管理

将来的に以下を管理する。

- 取材候補フラグ
- 取材状況
- 取材記事
- コメント引用
- 編集メモ

### 8-9. 通知

新規案件が取り込まれた際にDiscord等へ通知できる。

通知内容:

- 種別
- タイトル
- 担当部署
- 担当者
- 締切
- 予算
- 概要
- URL

## 9. 画面仕様

### 9-1. トップページ `/`

- 最新企画一覧
- 注目担当者
- 種別絞り込み

### 9-2. 企画一覧 `/projects`

- 企画を一覧表示
- `source_type` で絞り込み
- 将来的にタグ絞り込みを追加

### 9-3. 企画詳細 `/projects/<id>`

- 企画タイトル
- 概要
- 趣旨
- 予算
- 締切
- 担当部署
- 担当者
- PDF / ZIP
- 関連企画
- 根拠スニペット

### 9-4. 担当者一覧 `/people`

- 担当者ごとの案件数
- 関与部署数
- 将来的に主テーマ数

### 9-5. 担当者詳細 `/people/<person_key>`

- 担当者名
- 関与案件一覧
- 関与部署履歴
- 最近の案件
- よく関わるテーマ
- 関連人物
- 編集部仮説
- 関連取材記事

### 9-6. 管理画面 `/admin/*`

- 抽出結果確認
- 誤抽出修正
- 人物統合
- 部署統合
- 将来的にタグ編集、取材管理

## 10. 表示ルール

媒体としての信頼性を保つため、画面上の情報を明確に区別する。

- `公開情報`
  公開資料から直接確認できた事実
- `編集部仮説`
  公開情報にもとづく解釈
- `取材`
  本人または関係者への取材で得た情報

「思い」「価値観」「問題意識」は、原則として `取材` として表示する。  
公開情報だけから自動的に断定しない。

## 11. システム構成

### 11-1. 取込バッチ

- [ingest.py](/Users/harrn/Desktop/saga_staff_media/ingest.py)
- [extractor.py](/Users/harrn/Desktop/saga_staff_media/extractor.py)
- [config.py](/Users/harrn/Desktop/saga_staff_media/config.py)

役割:

- 一覧ページ巡回
- 記事本文取得
- 添付資料解析
- 抽出処理
- DB保存

### 11-2. データベース

- [database.py](/Users/harrn/Desktop/saga_staff_media/database.py)
- runtime の `saga_media.db`

役割:

- 企画保存
- 人物保存
- 部署保存
- 関与履歴保存
- 将来的なタグ・取材記事保存

### 11-3. 閲覧アプリ

- [app.py](/Users/harrn/Desktop/saga_staff_media/app.py)
- [templates/](/Users/harrn/Desktop/saga_staff_media/templates)
- [static/style.css](/Users/harrn/Desktop/saga_staff_media/static/style.css)

役割:

- 一覧画面表示
- 詳細画面表示
- 人物ページ表示
- 管理画面表示

## 12. 現在の実装状況

実装済み:

- 一覧ページから `kiji` URL を収集
- HTML / PDF / ZIP 内 PDF の本文抽出
- 趣旨 / 予算 / 締切 / 担当部署 / 担当者 の抽出
- SQLite保存
- 企画一覧、企画詳細、担当者一覧、担当者詳細の表示
- 管理画面での簡易レビュー
- 取得元CSV管理

未実装または弱い部分:

- 複数担当者の保持
- 同姓同名の厳密な人物統合
- 記者会見や重点施策資料の継続投入
- 重点テーマと人物同定のレビューUI強化
- 取材記事管理
- 編集部仮説の管理画面

## 13. セットアップ

```bash
cd saga_staff_media
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

仮想環境を毎回 `activate` しなくても、以下のように `./.venv/bin/python` を直接使えば実行できる。

### 13-1. 外部データ置き場

repo とは別にデータを持ちたい場合は、`SAGA_MEDIA_DATA_DIR` を設定する。  
このディレクトリに次が作られる。

- `saga_media.db`
- `crawled_urls.txt`
- `exports/`
- `transfers/`
- `policy_sources/`
- `list_sources.csv`
- `department_hierarchy.csv`

初期化は次で行える。

```bash
./.venv/bin/python setup_runtime.py --runtime-dir /path/to/saga_staff_media_data --copy-transfers --copy-policy-sources
```

### 13-2. Windows での初回セットアップ

Windows では [windows/README.md](/Users/harrn/Desktop/saga_staff_media/windows/README.md) の `.bat` を使うと早い。

1. `windows/setup_windows.bat`
2. `windows/run_app.bat`
3. 必要に応じて `windows/ingest.bat`

既定の外部データ置き場は `%USERPROFILE%\saga_staff_media_data`。

## 14. 基本の実行フロー

1. 一覧ページから記事を取り込む
2. 保存済み `raw_text` から担当者・部署を再構築する
3. Web画面で確認する

初回取り込み:

```bash
./.venv/bin/python ingest.py
```

抽出ルールを更新したあとに、保存済みデータから `person_mentions` / `appearances` を作り直す:

```bash
./.venv/bin/python rebuild_person_mentions.py
```

Webを起動する:

```bash
./.venv/bin/python app.py
```

ブラウザでは少なくとも次を確認するとよい。

- `http://127.0.0.1:8000/`
- `http://127.0.0.1:8000/topics`
- `http://127.0.0.1:8000/people`
- `http://127.0.0.1:8000/directory`
- `http://127.0.0.1:8000/network`
- `http://127.0.0.1:8000/admin/identities`

## 15. 取り込みのバリエーション

全件再取得する場合:

```bash
./.venv/bin/python ingest.py --force
```

proposal だけ再取得する場合:

```bash
./.venv/bin/python ingest.py --force --source-type proposal
```

特定の一覧URLだけ再取得する場合:

```bash
./.venv/bin/python ingest.py --force --source-url https://www.pref.saga.lg.jp/list00156.html
```

Discord通知も送る場合:

```bash
DISCORD_WEBHOOK_URL=... ./.venv/bin/python ingest.py --notify
```

`ingest.py` で新規取得、`rebuild_person_mentions.py` で再抽出反映、という役割分担にしている。  
抽出ルールだけを直した場合は、毎回ネット再取得せず `rebuild_person_mentions.py` を先に回すほうが速い。

## 16. 診断と補助スクリプト

一覧URLごとの `拾えた記事数 / DB保存数 / 担当者抽出数` を確認するには:

```bash
./.venv/bin/python diagnose_source_coverage.py --max-pages 1
```

担当者欠落の原因別診断を出すには:

```bash
./.venv/bin/python diagnose_missing_people.py
```

人事異動CSVを1本だけ取り込むには:

```bash
./.venv/bin/python import_transfers.py path/to/transfers.csv
```

年ごとのCSVをまとめて取り込むには:

```bash
./.venv/bin/python import_transfer_directory.py
```

取り込み前に年次CSVを検証するには:

```bash
./.venv/bin/python validate_transfer_csv.py
```

佐賀新聞の人事異動特集から年次CSVを自動生成するには:

```bash
./.venv/bin/python build_newspaper_transfer_csv.py 2025
```

重点施策ソースCSVを取り込むには:

```bash
./.venv/bin/python import_policy_sources.py data/policy_sources/policy_source_template.csv
```

既存企画から `重点テーマ -> project_topic_links -> person_topic_rollups` を再構築するには:

```bash
./.venv/bin/python rebuild_policy_topics.py
```

## 17. 年次の人事異動更新フロー

年1回の更新は、次の順に実行すると分かりやすい。

1. 必要なら佐賀新聞や公式異動表から年次CSVを作る
2. CSVを検証する
3. `transfer_events` に取り込む
4. Web画面で人物ページと `/directory` を確認する

例:

```bash
./.venv/bin/python build_newspaper_transfer_csv.py 2025
./.venv/bin/python validate_transfer_csv.py
./.venv/bin/python import_transfer_directory.py
./.venv/bin/python app.py
```

## 18. 重点テーマの更新フロー

知事会見、重点事業資料、当初予算資料などを人物・企画・ネットワークに反映したいときは、次の順に実行する。

1. [data/policy_sources/policy_source_template.csv](/Users/harrn/Desktop/saga_staff_media/data/policy_sources/policy_source_template.csv) 形式でCSVを作る
2. `policy_sources` / `policy_topics` に取り込む
3. `project_topic_links` と `person_topic_rollups` を再構築する
4. `/topics` `/people/<person_key>` `/network` を確認する

例:

```bash
./.venv/bin/python import_policy_sources.py data/policy_sources/policy_source_template.csv
./.venv/bin/python rebuild_policy_topics.py
./.venv/bin/python app.py
```

## 19. 当面の優先開発

1. 人物統合の精度を上げる
2. 同姓候補の review UI を強くする
3. `topic -> person -> department` の導線を強くする
4. 編集部仮説を保存する仕組みを追加する
5. 取材記事テーブルと画面を追加する

## 20. 人物同定と異動履歴の設計

人物同定、複数担当者、異動履歴の設計方針は
[docs/person_identity_and_transfer_design.md](/Users/harrn/Desktop/saga_staff_media/docs/person_identity_and_transfer_design.md)
にまとめた。

人事異動DBの具体スキーマ、取り込み順、`person_identity_links` とつなぐ信頼度ルールは
[docs/transfer_history_ingestion_spec.md](/Users/harrn/Desktop/saga_staff_media/docs/transfer_history_ingestion_spec.md)
にまとめた。

年1回更新の運用メモは
[docs/annual_transfer_update_workflow.md](/Users/harrn/Desktop/saga_staff_media/docs/annual_transfer_update_workflow.md)
にまとめた。
