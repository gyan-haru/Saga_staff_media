# Windows 起動ファイル

このディレクトリには、Windows で動かすための起動用 `.bat` をまとめています。

## 1. 初回セットアップ

`setup_windows.bat`

- `.venv` を作成
- `requirements.txt` をインストール
- 外部 runtime データ置き場を初期化

外部データ置き場は、既定では `%USERPROFILE%\saga_staff_media_data` です。変更したい場合は、実行前に `SAGA_MEDIA_DATA_DIR` を設定してください。

## 2. よく使う起動

- `run_app.bat`
- `ingest.bat`
- `rebuild_person_mentions.bat`
- `rebuild_employee_slots.bat`

## 3. 外部データ置き場

この `.bat` 群は次の環境変数を自動設定します。

- `SAGA_MEDIA_DATA_DIR`
- `SAGA_MEDIA_EXPORT_DIR`
- `SAGA_MEDIA_TRANSFERS_DIR`
- `SAGA_MEDIA_POLICY_SOURCES_DIR`
- `SAGA_MEDIA_SOURCES_CSV_PATH`
- `SAGA_MEDIA_DEPARTMENT_HIERARCHY_CSV_PATH`

そのため、Windows 側では「repo にはコードだけ、データは外部ディレクトリ」に寄せて運用できます。
