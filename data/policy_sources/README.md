# 重点施策ソースCSV

知事会見、重点事業資料、当初予算資料などを `policy_topics` に取り込むためのCSV置き場です。

## 必須列

- `source_type`
- `title`
- `url`
- `published_at`
- `summary`
- `raw_text`
- `topic_names`

## 使い方

```bash
./.venv/bin/python import_policy_sources.py data/policy_sources/policy_source_template.csv
./.venv/bin/python rebuild_policy_topics.py
```

`topic_names` は `|` 区切りで複数指定できます。空欄の場合は `title / summary / raw_text` から推定します。
