# Windows 起動ファイル

このディレクトリには、Windows でこのプロジェクトを動かすための `.bat` をまとめています。  
考え方は `repo にはコード、runtime データは外部ディレクトリ` です。

## 1. ファイル一覧

- `setup_windows.bat`
  初回セットアップ。`.venv` 作成、依存インストール、runtime データ置き場初期化まで行う。
- `set_runtime_env.bat`
  実行時に必要な環境変数をまとめて設定する。
- `run_app.bat`
  Web アプリを起動する。
- `ingest.bat`
  一覧ページから記事を取り込む。
- `rebuild_person_mentions.bat`
  保存済み `raw_text` から担当者・部署抽出を再構築する。
- `rebuild_employee_slots.bat`
  人事異動ベースの勤務スロットを再構築する。

## 2. 初回セットアップ

PowerShell またはコマンドプロンプトで repo ルートへ移動して、次を実行する。

```bat
windows\setup_windows.bat
```

このスクリプトは次を行う。

- `.venv` を作成
- `pip` を更新
- `requirements.txt` をインストール
- 外部 runtime データ置き場を初期化
- `list_sources.csv`、`department_hierarchy.csv`、テンプレート CSV をコピー

既存DBやクロール済みログも一緒に持っていきたい場合は、追加オプションも使える。

```bat
windows\setup_windows.bat --copy-db --copy-transfers --copy-policy-sources
```

## 3. よく使う実行

Web を起動する。

```bat
windows\run_app.bat
```

記事を取り込む。

```bat
windows\ingest.bat
```

proposal だけ再取得する。

```bat
windows\ingest.bat --force --source-type proposal
```

特定の一覧URLだけ再取得する。

```bat
windows\ingest.bat --force --source-url https://www.pref.saga.lg.jp/list00156.html
```

抽出ルールを変えたあとに、保存済みデータから担当者・部署を再構築する。

```bat
windows\rebuild_person_mentions.bat
```

人事異動データや勤務スロット候補を再計算する。

```bat
windows\rebuild_employee_slots.bat
```

## 4. runtime データ置き場

既定の runtime データ置き場は次。

```text
%USERPROFILE%\saga_staff_media_data
```

この中に、主に次が置かれる。

- `saga_media.db`
- `crawled_urls.txt`
- `list_sources.csv`
- `department_hierarchy.csv`
- `exports\`
- `transfers\`
- `policy_sources\`

イメージは次のとおり。

```text
%USERPROFILE%\saga_staff_media_data\
├── saga_media.db
├── crawled_urls.txt
├── list_sources.csv
├── department_hierarchy.csv
├── exports\
├── transfers\
│   ├── source_index.csv
│   ├── transfer_template.csv
│   └── *.csv
└── policy_sources\
    ├── policy_source_template.csv
    └── *.csv
```

## 5. 環境変数

`.bat` は実行時に次の環境変数を自動設定する。

- `REPO_ROOT`
- `SAGA_MEDIA_DATA_DIR`
- `SAGA_MEDIA_EXPORT_DIR`
- `SAGA_MEDIA_TRANSFERS_DIR`
- `SAGA_MEDIA_POLICY_SOURCES_DIR`
- `SAGA_MEDIA_SOURCES_CSV_PATH`
- `SAGA_MEDIA_DEPARTMENT_HIERARCHY_CSV_PATH`
- `SAGA_MEDIA_TRANSFER_SOURCE_INDEX_CSV_PATH`
- `SAGA_MEDIA_TRANSFER_TEMPLATE_CSV_PATH`
- `SAGA_MEDIA_POLICY_SOURCE_TEMPLATE_CSV_PATH`

そのため、Windows 側では repo にコードだけを置き、DB や CSV を外部ディレクトリへ逃がしたまま運用できる。

## 6. データ置き場を変えたい場合

既定の `%USERPROFILE%\saga_staff_media_data` ではなく別ドライブへ置きたい場合は、実行前に `SAGA_MEDIA_DATA_DIR` を設定する。

PowerShell の例:

```powershell
$env:SAGA_MEDIA_DATA_DIR = "D:\saga_staff_media_data"
windows\setup_windows.bat
```

この場合も、`exports`、`transfers`、`policy_sources` などは自動でその配下に作られる。

## 7. 補足

- `.venv not found` と出たら、先に `windows\setup_windows.bat` を実行する。
- repo 内の `data\` を直接使うこともできるが、Windows では外部 runtime データ置き場を使う前提のほうが扱いやすい。
- 人事異動 CSV や政策ソース CSV を追加したい場合は、`transfers\` と `policy_sources\` のテンプレートを基準にする。
