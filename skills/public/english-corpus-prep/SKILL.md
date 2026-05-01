---
name: english-corpus-prep
description: Mirror complex English corpus trees and clean PDF/TXT corpus files while preserving sidecar metadata relationships. Use when Codex needs to scan deeply nested corpus folders, distinguish PDF/TXT corpus files from centralized or adjacent metadata files, produce *_cleaned TXT output, optionally produce Penn Treebank-style *_tagged output, and write bilingual corpus reports.
---

# English Corpus Prep

Prepare analysis-ready English corpus output from complex directory trees. The bundled script is the source of truth; patch it only when a project needs a new cleaning profile or a new file classification rule.

## Workflow

1. Gather one or more input files/directories.
2. Recursively inventory the full tree when `--recursive` is used.
3. Classify files before processing:
   - corpus: `.pdf`, ordinary `.txt`
   - sidecar/metainformation: metadata/index/manifest/readme files and non-PDF/TXT formats
4. Mirror the original relative directory structure into a new output folder.
5. Clean corpus files only:
   - PDF -> extracted text -> `*_cleaned.txt`
   - TXT -> UTF-8 normalized text -> `*_cleaned.txt`
6. Preserve legal UTF-8 text. Do not strip non-ASCII characters wholesale.
7. Copy or register sidecar/metainformation files according to `--sidecar-policy`.
8. If tagging is enabled, generate Penn Treebank-style `token_TAG` output as `*_tagged.txt`.
9. Write all logs under `logs/`.
10. Write reports at output root:
   - `corpus_report_en.md`
   - `corpus_report_zh.md`
   - `corpus_report.json`
   - `config_used.json`

## Run The Pipeline

Recommended command:

```bash
python3 scripts/build_corpus.py <input-path> \
  --output-dir <output-dir> \
  --recursive \
  --tagger auto \
  --copy-sidecars \
  --relative-paths \
  --max-workers 4 \
  --cleaning-profile generic
```

For dependency-free cleaned output:

```bash
python3 scripts/build_corpus.py <input-path> --output-dir <output-dir> --recursive --tagger none
```

## Parameters

- `--recursive`: scan nested directories.
- `--tagger auto|nltk|spacy|none|heuristic`: choose POS tagging mode.
- `--on-tagger-missing fail|skip`: fail by default when a requested real tagger is unavailable.
- `--copy-sidecars`: compatibility alias for `--sidecar-policy copy`.
- `--sidecar-policy copy|manifest-only|skip`: copy, only log, or ignore non-corpus files.
- `--relative-paths`: avoid absolute paths in reports/manifests.
- `--max-workers N`: process corpus files concurrently.
- `--cleaning-profile generic|academic|policy|light`: choose cleaning strength.
- `--collection-name NAME`: override output tree prefix.

## POS Tagging Contract

Tagged output must use Penn Treebank-style `token_TAG` formatting:

```text
This_DT is_VBZ a_DT sample_NN ._.
```

Use a real tagger whenever tagged output is meant for analysis:

- `nltk`: NLTK `pos_tag`, PTB-style tags.
- `spacy`: spaCy English fine-grained tags.
- `auto`: NLTK first, spaCy second.

The `heuristic` tagger is explicit opt-in only. It emits PTB-shaped output for smoke tests or no-dependency demos, but it is not reliable enough for linguistic analysis.

## Output Contract

Given `--output-dir corpus_output` and input `raw_corpus`, write:

```text
corpus_output/
├── raw_corpus_cleaned/
│   └── ...mirrored input tree...
├── raw_corpus_tagged/
│   └── ...mirrored input tree...
├── logs/
│   ├── errors.jsonl
│   ├── file_inventory.jsonl
│   ├── metadata_candidates.jsonl
│   ├── processing_manifest.jsonl
│   ├── sidecar_manifest.jsonl
│   └── skipped_files.jsonl
├── corpus_report_en.md
├── corpus_report_zh.md
├── corpus_report.json
└── config_used.json
```

Do not create a merged clean corpus by default. File-level and directory-level provenance matter more than a single concatenated text file for this skill.

## Quality Checks

After each run:

1. Inspect `logs/file_inventory.jsonl` to confirm corpus/sidecar classification.
2. Inspect `logs/metadata_candidates.jsonl` for centralized metadata files.
3. Spot-check 3-5 cleaned files from different subdirectories.
4. If tagging was enabled, verify `*_tagged.txt` uses `token_TAG` format and `corpus_report_*.md` states which tagger was used.
5. Check `logs/errors.jsonl` for scanned PDFs, parser failures, and encoding failures.
