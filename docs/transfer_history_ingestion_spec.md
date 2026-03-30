# 人事履歴DB 取り込み仕様

## 1. 目的

この仕様は、佐賀県庁職員の人事異動を

- 県公式の異動表
- 佐賀新聞などの新聞人事記事
- 必要に応じた手動入力

から同じ形で保存し、既存の `person_mentions` / `person_identity_links` と安全につなぐための実装メモである。

狙いは `担当者名を抽出する精度` を直接上げることではない。  
主に次の3点を強くする。

1. 同姓同名の別人判定
2. 同一人物の部署異動の追跡
3. 担当者不明案件への候補補完

## 2. 基本原則

### 2-1. 異動情報も観測値として保存する

異動表の1行は、まず `公開資料に書かれていた事実` として保存する。

- 氏名
- 発令日
- 新所属
- 旧所属
- 新職名
- 旧職名
- 証拠スニペット

この段階では `people` に結び付いていなくてもよい。

### 2-2. 人物同定は別リンクで持つ

案件担当者の出現を `person_mentions` と `person_identity_links` に分けたのと同じ考え方で、
異動情報も `transfer_events` と `transfer_identity_links` に分ける。

### 2-3. 公式を優先し、新聞は補強に使う

ソースの強さは次の順に扱う。

1. `official_transfer_list`
2. `newspaper_transfer_list`
3. `manual_note`

同じ人物・同じ発令日の情報が複数ソースに出たときは、公式を優先しつつ、新聞は補助証拠として残す。

## 3. スキーマ

この仕様に対応するテーブルは [database.py](/Users/harrn/Desktop/saga_staff_media/database.py) の `init_db()` に追加してある。

### 3-1. `transfer_sources`

異動情報の元になった記事やPDFや表全体を保存する。

主なカラム:

- `source_type`
  `official_transfer_list`, `newspaper_transfer_list`, `manual_note`
- `source_key`
  同じソースを二重取り込みしないための一意キー  
  例: `official_transfer_list:https://www.pref.saga.lg.jp/...`
- `title`
- `url`
- `publisher`
- `published_at`
- `effective_date`
- `raw_text`
- `source_hash`

### 3-2. `transfer_events`

異動表の1行を1レコードで持つ。

主なカラム:

- `transfer_source_id`
- `event_index`
- `effective_date`
- `raw_person_name`
- `normalized_person_name`
- `name_quality`
  `full_name`, `surname_only`, `unknown`
- `from_department_raw`
- `from_department_id`
- `to_department_raw`
- `to_department_id`
- `from_title_raw`
- `to_title_raw`
- `evidence_snippet`
- `review_status`
  `pending`, `approved`, `rejected`

### 3-3. `transfer_identity_links`

異動イベントがどの `people` に結び付くかを保存する。

主なカラム:

- `transfer_event_id`
- `person_id`
- `link_status`
  `auto_matched`, `reviewed_match`, `reviewed_distinct`, `review_pending`
- `confidence`
- `matched_by`
- `notes`

## 4. 取り込みフロー

### 4-1. Step 1: source を保存する

まず記事単位、表単位で `transfer_sources` を保存する。

この段階で最低限ほしい項目:

- `source_type`
- `source_key`
- `title`
- `url`
- `publisher`
- `published_at`
- `effective_date`
- `raw_text`

`source_key` は URL がある場合は URL ベースでよい。  
PDFや紙面入力など URL が弱い場合は、`source_type + effective_date + title` の正規化文字列を使う。

### 4-2. Step 2: 行単位に分解する

ソース本文を人事行ごとに `transfer_events` に落とす。

最低限の正規化ルール:

- 全角空白と連続空白を縮約する
- 氏名の両端空白を除く
- 部署名の `佐賀県` 有無は比較用に正規化する
- `新所属 / 旧所属 / 新職名 / 旧職名` を可能な範囲で分離する

### 4-3. Step 3: 部署を departments に寄せる

`from_department_raw` と `to_department_raw` は必ず残しつつ、
既存の `departments` に寄せられるものだけ `from_department_id` / `to_department_id` を入れる。

ここでは `clean_department_name()` と同等の正規化を流用する。

### 4-4. Step 4: 人物同定を行う

`transfer_events` を `people` に直結せず、`transfer_identity_links` を作る。

流れ:

1. `normalized_person_name` が一致する `people` を候補に出す
2. 候補ごとに信頼度スコアを計算する
3. 閾値以上は `auto_matched`
4. 中間帯は `review_pending`
5. 明らかに違うものはリンクしない

### 4-5. Step 5: mention 補完に使う

`transfer_identity_links` で確定した異動履歴は、案件側の `person_identity_links` スコアに再利用する。

使い方は2系統ある。

1. `同じ人物らしさ` を上げる
2. `その時期その部署にいたはず` という補完候補を出す

## 5. 公式優先の正規化仕様

### 5-1. 公式異動表

理想形:

- 発令日が明示されている
- 氏名が列として独立している
- 新所属と旧所属が分かれている

この形式は `transfer_events` にかなりそのまま入れやすい。  
もしPDF由来でレイアウト崩れがあっても、列構造を優先してパースする。

### 5-2. 新聞人事記事

新聞は次のぶれを許容する。

- 氏名の前後に肩書が混ざる
- 旧所属または旧職名が省略される
- 見出しと本文に同じ情報が重複する

新聞由来は `source_type = newspaper_transfer_list` にし、
情報が欠ける列は空欄で保存する。  
無理に列を埋めず、`evidence_snippet` を必ず残す。

### 5-3. 公式と新聞が両方ある場合

同じ `effective_date + normalized_person_name + to_department_raw` に近いイベントが両方ある場合は、

- 公式側を主レコードにする
- 新聞側は別 `transfer_source` / `transfer_event` として保持する
- UIでは `裏取りあり` として束ねて表示する

現段階では `merge` せず、表示層またはビュー層で束ねるほうが安全。

## 6. `person_identity_links` とつなぐ信頼度ルール

### 6-1. transfer -> people のスコア

`transfer_identity_links` を作るときの基準。

- フルネーム完全一致: `+0.45`
- 同じ人物に結び付いた mention と同じメールが過去にある: `+0.35`
- 同じ人物に結び付いた mention と同じ電話が過去にある: `+0.30`
- `to_department` が、その人物の直近 mention の部署と一致: `+0.20`
- `from_department` が、その人物の過去 mention の部署と一致: `+0.15`
- 発令日の前後180日で活動痕跡がある: `+0.10`
- 姓のみ一致: `+0.05`
- 同じ発令日に別部署で強い矛盾: `-0.40`
- フルネーム不一致: `-0.50`

判定:

- `0.85` 以上: `auto_matched`
- `0.60` 以上 `0.85` 未満: `review_pending`
- `0.60` 未満: リンクしない

### 6-2. people/mention 側に戻すときの加点

`person_identity_links` を計算するときに、確定した異動履歴を証拠として使う。

- mention の部署が、人物の直近 `to_department` と一致: `+0.20`
- mention の日付が異動発令日の90日以内で、その `to_department` と一致: `+0.15`
- mention の部署が、その人物の `from_department` にしか出てこない: `-0.15`
- 同じ時期に別人物の異動先と強く一致: `-0.20`

重要なのは、異動履歴を `自動確定の決め手` ではなく `補助証拠` として使うこと。
姓だけ mention は、異動履歴があっても単独では自動確定しない。

### 6-3. 補完ラベル

公開画面では断定表現を避けて、次のラベルに分ける。

- `確定: 異動履歴と一致`
- `推定: 異動履歴から候補`
- `参考: 同姓・同部署の候補`

## 7. レビュー対象

次のケースは自動リンクせず、管理画面レビューに送る。

- 姓だけ表記で候補が2人以上いる
- 同じフルネームが複数部署に存在する
- 公式と新聞で部署名が食い違う
- 新聞記事しかなく、旧所属が欠けている
- 同じ発令日に複数の有力候補がぶつかる

将来的には `/admin/transfers` のような画面で、

- 未リンク異動イベント
- 同姓同名衝突
- 公式/新聞の食い違い

をまとめて見られるようにする。

## 8. 実装順

### Phase 3-A

- `transfer_sources`
- `transfer_events`
- `transfer_identity_links`

を初期化時に作る。  
この段階では UI を作らなくてよい。

### Phase 3-B

`import_transfers.py` を追加し、まずは CSV または手動整形テキストから取り込めるようにする。

入力の最小列:

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

### Phase 3-C

`transfer_identity_links` の自動リンクを実装し、
`person_identity_links` のスコア計算に `department_continuity` を加点材料として組み込む。

### Phase 3-D

人物詳細ページで

- 確定異動履歴
- 活動痕跡
- 補完候補

を並べて出す。

## 9. この仕様で避けること

- 新聞記事だけで `people` を自動新規作成し続けること
- 姓だけの人事行を、そのまま案件担当者と自動で同一視すること
- 公式と新聞の食い違いを上書きで消すこと

## 10. 直近の次の一手

次に着手するなら順番はこの3つがよい。

1. `import_transfers.py` の CSV 取り込み版を作る
2. `transfer_identity_links` の自動スコアリングを実装する
3. `person_identity_links` 側に `department_continuity` 加点を足す
