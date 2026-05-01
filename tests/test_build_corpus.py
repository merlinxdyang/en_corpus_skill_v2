from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "skills/public/english-corpus-prep/scripts/build_corpus.py"


class BuildCorpusTests(unittest.TestCase):
    def run_script(self, *args: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(
            [sys.executable, str(SCRIPT), *args],
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_mirrors_nested_tree_and_copies_sidecars(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw = base / "raw_corpus"
            nested = raw / "level1" / "level2"
            nested.mkdir(parents=True)
            (nested / "essay.txt").write_text("America’s cafés are open.\nPage 1\n\nThis is corpus text.", encoding="utf-8")
            (raw / "metadata.csv").write_text("id,title\n1,Essay\n", encoding="utf-8")
            (nested / "readme.txt").write_text("This describes the folder.", encoding="utf-8")

            out = base / "out"
            result = self.run_script(
                str(raw),
                "--output-dir",
                str(out),
                "--recursive",
                "--tagger",
                "none",
                "--relative-paths",
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            cleaned = out / "raw_corpus_cleaned"
            self.assertTrue((cleaned / "level1/level2/essay_cleaned.txt").exists())
            self.assertTrue((cleaned / "metadata.csv").exists())
            self.assertTrue((cleaned / "level1/level2/readme.txt").exists())
            self.assertFalse((out / "raw_corpus_tagged").exists())

            text = (cleaned / "level1/level2/essay_cleaned.txt").read_text(encoding="utf-8")
            self.assertIn("America's cafés are open.", text)
            self.assertNotIn("Page 1", text)

            manifest = (out / "logs/processing_manifest.jsonl").read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(manifest), 1)
            self.assertEqual(json.loads(manifest[0])["relative_path"], "level1/level2/essay.txt")

            metadata_candidates = (out / "logs/metadata_candidates.jsonl").read_text(encoding="utf-8").strip().splitlines()
            self.assertEqual(len(metadata_candidates), 2)

            self.assertTrue((out / "corpus_report_en.md").exists())
            self.assertTrue((out / "corpus_report_zh.md").exists())

    def test_explicit_heuristic_tagger_emits_ptb_shaped_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw = base / "raw"
            raw.mkdir()
            (raw / "sample.txt").write_text("This is a sample.", encoding="utf-8")

            out = base / "out"
            result = self.run_script(
                str(raw),
                "--output-dir",
                str(out),
                "--recursive",
                "--tagger",
                "heuristic",
                "--relative-paths",
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            tagged = (out / "raw_tagged/sample_tagged.txt").read_text(encoding="utf-8")
            self.assertIn("This_DT", tagged)
            self.assertIn("is_VBZ", tagged)
            self.assertIn("._.", tagged)


if __name__ == "__main__":
    unittest.main()
