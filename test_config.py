from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from config import resolve_runtime_input_path


class ResolveRuntimeInputPathTests(unittest.TestCase):
    def test_explicit_path_copies_bundled_seed_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bundled = root / "bundled" / "list_sources.csv"
            bundled.parent.mkdir(parents=True, exist_ok=True)
            bundled.write_text("url,department_name,source_type\n", encoding="utf-8")
            explicit = root / "runtime" / "list_sources.csv"

            with mock.patch.dict(os.environ, {"TEST_SOURCES_CSV_PATH": str(explicit)}, clear=False):
                resolved = resolve_runtime_input_path(
                    "TEST_SOURCES_CSV_PATH",
                    root / "unused" / "list_sources.csv",
                    bundled,
                )

            self.assertEqual(resolved, explicit)
            self.assertTrue(explicit.exists())
            self.assertEqual(explicit.read_text(encoding="utf-8"), bundled.read_text(encoding="utf-8"))

    def test_runtime_default_copies_bundled_seed_when_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            bundled = root / "bundled" / "department_hierarchy.csv"
            bundled.parent.mkdir(parents=True, exist_ok=True)
            bundled.write_text("department_name,top_unit\n", encoding="utf-8")
            runtime_default = root / "runtime" / "department_hierarchy.csv"

            with mock.patch.dict(os.environ, {}, clear=False):
                resolved = resolve_runtime_input_path(
                    "TEST_DEPARTMENT_HIERARCHY_CSV_PATH",
                    runtime_default,
                    bundled,
                )

            self.assertEqual(resolved, runtime_default)
            self.assertTrue(runtime_default.exists())
            self.assertEqual(runtime_default.read_text(encoding="utf-8"), bundled.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
