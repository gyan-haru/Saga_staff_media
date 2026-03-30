# 人物同定と異動履歴の設計メモ

## 1. 目的

このプロジェクトでは、単に `担当者名` を一覧化するだけではなく、

- 同じ人物がどの案件に連続して関わっているか
- 部署異動をまたいでどのテーマを持ち続けているか
- 公開資料だけでどこまで確からしく追えるか

を扱いたい。

そのためには、次の2つを分けて保存する必要がある。

1. `公開資料に書いてあった観測値`
2. `それがどの人物かという同定結果`

今の `people` と `appearances` はこの2つが混ざっているため、

- 複数担当者を1件しか持てない
- 姓だけの抽出でも `people` に入ってしまう
- 同姓同名かどうかの判断を後からやりにくい
- 新聞や公式の異動情報を、案件ページ由来の担当者情報と同列で扱いにくい

という制約がある。

## 2. 設計方針

### 2-1. 事実と推定を分ける

- 公開資料に書いてある `氏名・部署・連絡先・日付・本文スニペット` は、そのまま保存する
- その観測値を「同じ人物だ」と判断した結果は、別テーブルで持つ
- 自動判定と手動確定を区別する

### 2-2. 姓だけ表記は自動統合しない

- `中村` `古川` `廣田` のような姓のみは、自動で同一人物に統合しない
- フルネーム一致、またはメール・電話などの補助証拠がある場合だけ自動統合候補にする

### 2-3. 複数担当者を自然に持てる構造にする

- 1案件に対して複数の `person_mention` を保存できるようにする
- 1件の異動記事に複数人の `transfer_event` を保存できるようにする

### 2-4. 異動履歴は独立した一次情報として持つ

- 案件ページの担当者欄
- 新聞の人事異動記事
- 県の公式異動表

は性質が違うので、同じ `appearance` の延長ではなく、イベントとして持つ。

## 3. 提案データモデル

### 3-1. 既存テーブルの位置づけ

- `projects`
  案件・記事本体
- `departments`
  部署マスタ
- `people`
  以後は `正規化後の人物` として扱う

`people` はそのまま使えるが、意味を `抽出された名前` から `編集部が同一人物だと扱う単位` に変える。

### 3-2. 新規テーブル

#### `person_mentions`

1件の公開資料の中で観測された「人物の出現」を保存する。

主なカラム:

- `id`
- `project_id`
- `mention_index`
- `raw_person_name`
- `normalized_person_name`
- `name_quality`
  `full_name`, `surname_only`, `unknown`
- `raw_department_name`
- `department_id`
- `role`
  `contact`, `author`, `press_header`, `manual` など
- `contact_email`
- `contact_phone`
- `extracted_section`
- `source_confidence`
  0.0-1.0
- `review_status`
  `pending`, `approved`, `rejected`
- `created_at`

補足:

- 1プロジェクトに複数レコードを持てる
- 現在の `appearances` が持っている生データの受け皿になる

#### `person_identity_links`

`person_mentions` がどの `people` に結びつくかを表す。

主なカラム:

- `id`
- `person_mention_id`
- `person_id`
- `link_status`
  `auto_matched`, `reviewed_match`, `reviewed_distinct`, `review_pending`
- `confidence`
- `matched_by`
  `full_name`, `email`, `phone`, `department_continuity`, `manual`
- `notes`
- `created_at`
- `updated_at`

補足:

- 1 mention に対して、有効な link は1件だけにする
- ただしレビュー履歴を残したいので、論理的に最新1件が有効という扱いでもよい

#### `transfer_sources`

異動情報の元記事や元表を保存する。

主なカラム:

- `id`
- `source_type`
  `official_transfer_list`, `newspaper_transfer_list`, `manual_note`
- `title`
- `url`
- `published_at`
- `effective_date`
- `publisher`
- `raw_text`
- `created_at`

#### `transfer_events`

異動の1行を1レコードで保存する。

主なカラム:

- `id`
- `transfer_source_id`
- `event_index`
- `effective_date`
- `raw_person_name`
- `normalized_person_name`
- `name_quality`
- `from_department_raw`
- `from_department_id`
- `to_department_raw`
- `to_department_id`
- `from_title_raw`
- `to_title_raw`
- `person_id`
  未確定なら `NULL`
- `identity_status`
  `auto_matched`, `reviewed_match`, `review_pending`, `distinct_person`
- `confidence`
- `evidence_snippet`
- `created_at`

補足:

- 異動情報は人物にひも付く前でも保存できる
- `person_id` を後から埋められる

#### `person_merge_candidates`

自動で「同一人物かもしれない」と出した候補をレビューする。

主なカラム:

- `id`
- `left_person_id`
- `right_person_id`
- `candidate_type`
  `same_name`, `same_email`, `same_phone`, `transfer_continuity`
- `score`
- `reason_json`
- `decision`
  `pending`, `merged`, `kept_separate`
- `reviewed_at`

## 4. 既存テーブルとの関係

### 4-1. `appearances` の扱い

短期的には残してよい。ただし役割を変える。

- `appearances`
  表示用の互換テーブル
- `person_mentions`
  新しい事実テーブル

移行中は、

1. まず `person_mentions` を正とする
2. 互換維持のため、最も信頼度の高い1件だけ `appearances` に反映する
3. UIが `person_mentions` 対応できたら `appearances` は縮小または廃止する

### 4-2. `people` の扱い

`people` は引き続き正規化済みの人物マスタとして使う。

ただし登録条件を厳しくする。

- フルネーム、または
- 姓だけでも同一メール/同一電話/同一異動表の前後関係がある

ときだけ `people` 候補に昇格させる。

## 5. 同一人物判定ルール

### 5-1. 自動で結びやすいケース

- フルネーム完全一致 + 同じメール
- フルネーム完全一致 + 同じ電話
- フルネーム完全一致 + 同じ部署 + 近い時期
- フルネーム完全一致 + 異動表で `前部署 -> 新部署` の連続性がある

### 5-2. 自動で結ばないケース

- 姓のみ一致
- 名前が2文字以下で部署系語に近い
- 同じ姓で、部署も連絡先も一致しない
- 役職や肩書が主語に混ざっている

### 5-3. スコア例

- 同一メール: `+0.60`
- 同一電話: `+0.50`
- フルネーム完全一致: `+0.35`
- 同一部署: `+0.15`
- 異動表の連続性: `+0.25`
- 姓のみ一致: `+0.05`
- 部署不一致: `-0.20`
- 時期が大きく離れていて補助証拠なし: `-0.15`

運用ルール:

- `0.85` 以上: 自動リンク可
- `0.60-0.84`: レビュー候補
- `0.59` 以下: 自動リンクしない

## 6. 同姓と同一人物の区別

### 6-1. 基本原則

- `同姓` は `別人` を初期値にする
- `同一人物` は証拠が積み上がったときだけ採用する

### 6-2. 実際の判断材料

- フルネームがあるか
- 同じメールアドレスか
- 同じ電話番号か
- 同じ部署に継続して出ているか
- 異動表で前後がつながるか
- その時期の案件内容が連続しているか

### 6-3. 表示上の扱い

- 証拠十分: `異動履歴`
- まだ弱い: `同一人物候補`
- 証拠なし: 別人物のまま

## 7. 異動履歴の作り方

### 7-1. 公式・新聞の異動表から保存するもの

- 発令日
- 氏名
- 新所属
- 旧所属
- 新職名
- 旧職名
- ソースURL
- 証拠スニペット

### 7-2. プロジェクト担当者ページから推定するもの

案件ページから直接 `transfer_event` を作るのではなく、

- `person_mentions` に担当部署を保存する
- 同一 `people` に結びついた mention を時系列で並べる
- その後に `department_timeline` として導出する

つまり、

- 異動表は `一次情報の異動`
- 案件ページは `活動痕跡`

として分ける。

## 8. 推奨UI

### 8-1. 人物詳細ページ

- `確定した人物名`
- `同定の確からしさ`
- `確認済みの所属履歴`
- `異動イベント`
- `案件から見える活動履歴`
- `同一人物候補`

### 8-2. 管理画面

- `姓だけ抽出` 一覧
- `複数候補にぶつかる mention` 一覧
- `同一メール/電話` 候補
- `異動表未リンク` 一覧
- `merge / keep separate` 操作

## 9. 段階的な実装順

### Phase 1

- `person_mentions` を追加
- 1案件に複数担当者を保存できるようにする
- `people` への自動昇格条件を厳しくする

### Phase 2

- `person_identity_links` を追加
- 自動リンクと手動確定を分ける
- 管理画面に `同一人物候補` を出す

### Phase 3

- `transfer_sources` と `transfer_events` を追加
- 公式異動表と新聞異動表を取り込む
- 人物ページに `異動履歴` を表示する

### Phase 4

- `department_timeline` の導出ロジックを追加
- `person -> projects -> transfers` の統合ビューを出す

## 10. 直近の実装タスク

次に着手するなら優先順位はこの順がよい。

1. `ProjectRecord` を `person_mentions: list[...]` 対応に拡張する
2. `extractor.py` で複数担当者抽出を返せるようにする
3. `database.py` で `person_mentions` を保存する
4. `people` 自動登録を `フルネーム or 強い補助証拠あり` に制限する
5. `transfer_events` 取り込み用の別バッチを追加する

## 11. 今回の結論

### 11-1. 任意の人の異動追跡は可能か

可能。ただし条件付き。

- 公式異動表や新聞人事欄に載る人は追いやすい
- 案件ページに担当者として何度も出る人も追いやすい
- 姓だけしか出ない人、短期間に1回しか出ない人は弱い

### 11-2. 何を避けるべきか

- 姓だけで自動統合すること
- `appearances` 1件だけで人物マスタを確定すること
- 異動表と案件担当者欄を同じ粒度で混ぜること

### 11-3. 何をやるべきか

- まず観測値テーブルを分離する
- 次に人物同定を別管理にする
- 最後に異動イベントを重ねる

### 11-4. 実装仕様

実際のテーブル定義、取り込み順、`person_identity_links` とつなぐ信頼度ルールは
[docs/transfer_history_ingestion_spec.md](/Users/harrn/Desktop/saga_staff_media/docs/transfer_history_ingestion_spec.md)
に切り出した。
