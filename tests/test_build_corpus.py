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

    def test_policy_profile_removes_common_web_boilerplate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw = base / "raw"
            raw.mkdir()
            sample = "\n".join(
                [
                    "Search Results | NIST",
                    "Skip to main content",
                    "Official websites use .gov",
                    "A locked padlock",
                    "Share sensitive information only on official, secure websites.",
                    "Home • The Administration • Office of Science and Technology Policy",
                    "About",
                    "Pressroom",
                    "This is the actual policy paragraph with substance.",
                ]
            )
            (raw / "webpage.txt").write_text(sample, encoding="utf-8")

            out = base / "out"
            result = self.run_script(
                str(raw),
                "--output-dir",
                str(out),
                "--recursive",
                "--tagger",
                "none",
                "--relative-paths",
                "--cleaning-profile",
                "policy",
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            text = (out / "raw_cleaned/webpage_cleaned.txt").read_text(encoding="utf-8")
            self.assertNotIn("Search Results | NIST", text)
            self.assertNotIn("Skip to main content", text)
            self.assertNotIn("Official websites use .gov", text)
            self.assertNotIn("Pressroom", text)
            self.assertIn("actual policy paragraph with substance", text)

    def test_policy_profile_keeps_regular_semantic_lines(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw = base / "raw"
            raw.mkdir()
            (raw / "meaningful.txt").write_text(
                "Searchlight to the California border.\nThis line should remain.",
                encoding="utf-8",
            )
            out = base / "out"
            result = self.run_script(
                str(raw),
                "--output-dir",
                str(out),
                "--recursive",
                "--tagger",
                "none",
                "--relative-paths",
                "--cleaning-profile",
                "policy",
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            text = (out / "raw_cleaned/meaningful_cleaned.txt").read_text(encoding="utf-8")
            self.assertIn("Searchlight to the California border.", text)

    def test_policy_profile_strips_inline_boilerplate_fragments(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw = base / "raw"
            raw.mkdir()
            (raw / "inline.txt").write_text(
                "AI Risk Management Framework Workshop Attracts 800+ | NIST Updated February 3, 2025 Was this page helpful?",
                encoding="utf-8",
            )
            out = base / "out"
            result = self.run_script(
                str(raw),
                "--output-dir",
                str(out),
                "--recursive",
                "--tagger",
                "none",
                "--relative-paths",
                "--cleaning-profile",
                "policy",
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            text = (out / "raw_cleaned/inline_cleaned.txt").read_text(encoding="utf-8")
            self.assertNotIn("Was this page helpful", text)
            self.assertIn("AI Risk Management Framework Workshop Attracts 800+", text)

    def test_policy_profile_strips_inline_federal_register_codes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw = base / "raw"
            raw.mkdir()
            (raw / "fr-inline.txt").write_text(
                "Federal Register\nACTION: Proposedrule. SUMMARY: A real paragraph. BILLING CODE 4120-01-C The agency continues.",
                encoding="utf-8",
            )
            out = base / "out"
            result = self.run_script(
                str(raw),
                "--output-dir",
                str(out),
                "--recursive",
                "--tagger",
                "none",
                "--relative-paths",
                "--cleaning-profile",
                "policy",
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            text = (out / "raw_cleaned/fr-inline_cleaned.txt").read_text(encoding="utf-8")
            self.assertIn("Proposed rule", text)
            self.assertIn("The agency continues", text)
            self.assertNotIn("Proposedrule", text)
            self.assertNotIn("BILLING CODE", text)

    def test_policy_profile_drops_punctuation_and_xml_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw = base / "raw"
            raw.mkdir()
            (raw / "artifact.txt").write_text(
                ".\n?\ntrump.eps</gph>\nMeaningful line remains.",
                encoding="utf-8",
            )
            out = base / "out"
            result = self.run_script(
                str(raw),
                "--output-dir",
                str(out),
                "--recursive",
                "--tagger",
                "none",
                "--relative-paths",
                "--cleaning-profile",
                "policy_strict",
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            text = (out / "raw_cleaned/artifact_cleaned.txt").read_text(encoding="utf-8")
            self.assertNotIn("trump.eps</gph>", text)
            self.assertIn("Meaningful line remains.", text)

    def test_policy_profile_drops_last_modified_timestamp_lines(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw = base / "raw"
            raw.mkdir()
            (raw / "stamp.txt").write_text(
                "Last modified\nApril 2, 2022\nMain body starts here.",
                encoding="utf-8",
            )
            out = base / "out"
            result = self.run_script(
                str(raw),
                "--output-dir",
                str(out),
                "--recursive",
                "--tagger",
                "none",
                "--relative-paths",
                "--cleaning-profile",
                "policy",
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            text = (out / "raw_cleaned/stamp_cleaned.txt").read_text(encoding="utf-8")
            self.assertNotIn("Last modified", text)
            self.assertNotIn("April 2, 2022", text)
            self.assertIn("Main body starts here.", text)

    def test_generic_profile_drops_repeated_layout_templates(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw = base / "raw"
            raw.mkdir()
            noisy = "\n".join(
                [
                    "Federal Register / Vol. 85, No. 197 / Friday, October 9, 2020 / Proposed Rules",
                    "Meaningful legal paragraph starts here and explains the rule.",
                    "VerDate Sep<11>2014 20:46 Oct 08, 2020 Jkt 253001 PO 00000 Frm 00002 Fmt 4701 Sfmt 4702 E:\\FR\\FM\\09OCP2.SGM 09OCP2",
                    "Another meaningful sentence remains for analysis.",
                    "Federal Register / Vol. 85, No. 197 / Friday, October 9, 2020 / Proposed Rules",
                    "VerDate Sep<11>2014 20:46 Oct 08, 2020 Jkt 253001 PO 00000 Frm 00003 Fmt 4701 Sfmt 4702 E:\\FR\\FM\\09OCP2.SGM 09OCP2",
                ]
            )
            (raw / "layout.txt").write_text(noisy, encoding="utf-8")
            out = base / "out"
            result = self.run_script(str(raw), "--output-dir", str(out), "--recursive", "--tagger", "none", "--relative-paths")
            self.assertEqual(result.returncode, 0, result.stderr)
            text = (out / "raw_cleaned/layout_cleaned.txt").read_text(encoding="utf-8")
            self.assertNotIn("VerDate Sep<11>2014", text)
            self.assertNotIn("Federal Register / Vol. 85", text)
            self.assertIn("Meaningful legal paragraph starts here", text)

    def test_generic_profile_drops_front_matter_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw = base / "raw"
            raw.mkdir()
            (raw / "lead.txt").write_text(
                "text/xml\nEN\nPursuant to Title 17 this file is in the public domain.\nThe substantive clause begins here and defines procedural rights for agency review.",
                encoding="utf-8",
            )
            out = base / "out"
            result = self.run_script(str(raw), "--output-dir", str(out), "--recursive", "--tagger", "none", "--relative-paths")
            self.assertEqual(result.returncode, 0, result.stderr)
            text = (out / "raw_cleaned/lead_cleaned.txt").read_text(encoding="utf-8")
            self.assertNotIn("text/xml", text)
            self.assertNotIn("public domain", text.lower())
            self.assertIn("substantive clause begins here", text)

    def test_generic_profile_repairs_split_words_across_lines(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw = base / "raw"
            raw.mkdir()
            (raw / "split.txt").write_text(
                "The statute grants additional Powe\nrs to the agency and preserves due process.",
                encoding="utf-8",
            )
            out = base / "out"
            result = self.run_script(str(raw), "--output-dir", str(out), "--recursive", "--tagger", "none", "--relative-paths")
            self.assertEqual(result.returncode, 0, result.stderr)
            text = (out / "raw_cleaned/split_cleaned.txt").read_text(encoding="utf-8")
            self.assertIn("Powers to the agency", text)

    def test_generic_profile_does_not_merge_regular_wrapped_words(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw = base / "raw"
            raw.mkdir()
            (raw / "wrapped.txt").write_text(
                "The agency may use the fund for other\npurposes authorized by Congress.",
                encoding="utf-8",
            )
            out = base / "out"
            result = self.run_script(str(raw), "--output-dir", str(out), "--recursive", "--tagger", "none", "--relative-paths")
            self.assertEqual(result.returncode, 0, result.stderr)
            text = (out / "raw_cleaned/wrapped_cleaned.txt").read_text(encoding="utf-8")
            self.assertIn("other purposes", text)
            self.assertNotIn("otherpurposes", text)

    def test_generic_profile_does_not_merge_attested_common_bigrams(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw = base / "raw"
            raw.mkdir()
            (raw / "bigrams.txt").write_text(
                "\n".join(
                    [
                        "This source has a malformed nationalsecurity token elsewhere.",
                        "This paragraph discusses national security and a significant new rule.",
                    ]
                ),
                encoding="utf-8",
            )
            out = base / "out"
            result = self.run_script(str(raw), "--output-dir", str(out), "--recursive", "--tagger", "none", "--relative-paths")
            self.assertEqual(result.returncode, 0, result.stderr)
            text = (out / "raw_cleaned/bigrams_cleaned.txt").read_text(encoding="utf-8")
            self.assertIn("national security", text)
            self.assertIn("significant new rule", text)
            self.assertNotIn("nationalsecurity and", text)
            self.assertNotIn("significantnew", text)

    def test_generic_profile_repairs_common_split_prefix_words(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw = base / "raw"
            raw.mkdir()
            (raw / "constituted.txt").write_text(
                "The emergency was cons\ntituted by an unusual and extraordinary threat.",
                encoding="utf-8",
            )
            out = base / "out"
            result = self.run_script(str(raw), "--output-dir", str(out), "--recursive", "--tagger", "none", "--relative-paths")
            self.assertEqual(result.returncode, 0, result.stderr)
            text = (out / "raw_cleaned/constituted_cleaned.txt").read_text(encoding="utf-8")
            self.assertIn("constituted by", text)
            self.assertNotIn("cons tituted", text)

    def test_policy_profile_preserves_congress_numbered_substance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            base = Path(tmp)
            raw = base / "raw"
            raw.mkdir()
            (raw / "bill.txt").write_text(
                "\n".join(
                    [
                        "text/xml",
                        "U.S. Senate",
                        "117th CONGRESS",
                        "The table of contents for this Act is as follows:",
                        "Sec. 1001. Table of contents.",
                        "Sec. 1002. Appropriations.",
                        "There is appropriated to the Fund, out of amounts in the Treasury not otherwise appropriated-",
                        "for fiscal year 2022, $400,000,000, to remain available until September 30, 2022;",
                        "for fiscal year 2023, $400,000,000, to remain available until September 30, 2023;",
                        "The amounts provided under this section are designated as an emergency requirement.",
                    ]
                ),
                encoding="utf-8",
            )
            out = base / "out"
            result = self.run_script(
                str(raw),
                "--output-dir",
                str(out),
                "--recursive",
                "--tagger",
                "none",
                "--relative-paths",
                "--cleaning-profile",
                "policy",
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            text = (out / "raw_cleaned/bill_cleaned.txt").read_text(encoding="utf-8")
            self.assertNotIn("Sec. 1002. Appropriations.", text)
            self.assertIn("for fiscal year 2022, $400,000,000", text)
            self.assertIn("emergency requirement", text)


if __name__ == "__main__":
    unittest.main()
