# English Corpus Prep Skill v2

English Corpus Prep Skill v2 is a Codex skill for preparing English corpora from messy, deeply nested folders. It focuses on two corpus source formats: `PDF` and `TXT`. Everything else is treated as sidecar metadata and preserved or logged according to the selected policy.

The design goal is provenance preservation: the output mirrors the original directory tree, cleaned files keep their original relative locations, metadata files remain near or alongside the corpus structure, and no merged mega-corpus is created by default.

## 中文简介

English Corpus Prep Skill v2 是一个用于处理英文语料库的 Codex skill，适合处理结构复杂、层级很深、metadata 分散或集中的语料目录。

新版只把 `PDF` 和普通 `TXT` 视为语料文件。其他文件，例如 `metadata.csv`、`index.xlsx`、`manifest.json`、`readme.txt`、`*.json`、`*.xml` 等，默认作为 sidecar/metainformation 原样保留或登记。脚本不会修改原始目录，而是在新的输出目录中复制原目录结构，并生成清洗后的 `*_cleaned.txt` 和可选的 Penn Treebank 风格 `*_tagged.txt`。

核心目标是保留来源关系：不打乱原始目录，不破坏语料与 metadata 的相对关系，不默认合并总语料。

---

## What It Does

- Recursively scans complex corpus folders.
- Detects PDF corpus files, ordinary TXT corpus files, and sidecar metadata files.
- Extracts text from PDFs with `pypdf` or `pdfplumber`.
- Decodes TXT files to UTF-8 where possible.
- Cleans text conservatively while preserving legal UTF-8 characters.
- Mirrors the original relative directory structure into a new output folder.
- Writes cleaned corpus files with `_cleaned.txt` suffix.
- Optionally writes Penn Treebank-style POS tagged files with `_tagged.txt` suffix.
- Copies or logs sidecar metadata files.
- Writes machine-readable logs and reports.
- Generates both English and Chinese corpus reports.

## 功能说明

- 递归扫描复杂树形目录。
- 自动识别 PDF 语料、普通 TXT 语料和 sidecar/metainformation 文件。
- 使用 `pypdf` 或 `pdfplumber` 抽取 PDF 文本。
- 尽可能将 TXT 解码为 UTF-8。
- 使用保守清洗规则，保留合法 UTF-8 字符。
- 在新输出目录中镜像原始相对目录结构。
- 清洗后语料文件统一加 `_cleaned.txt` 后缀。
- 可选生成 Penn Treebank 风格词性标注文件，统一加 `_tagged.txt` 后缀。
- 原样复制或登记 sidecar/metainformation 文件。
- 生成机器可读日志和报告。
- 同时生成英文与中文语料库报告。

---

## Repository Layout

```text
.
├── README.md
├── examples/
│   └── README.md
├── skills/
│   └── public/
│       └── english-corpus-prep/
│           ├── SKILL.md
│           ├── agents/
│           │   └── openai.yaml
│           ├── references/
│           │   └── cleaning-profile.md
│           └── scripts/
│               └── build_corpus.py
└── tests/
    └── test_build_corpus.py
```

## 仓库结构

```text
.
├── README.md
├── examples/
│   └── README.md
├── skills/
│   └── public/
│       └── english-corpus-prep/
│           ├── SKILL.md
│           ├── agents/
│           │   └── openai.yaml
│           ├── references/
│           │   └── cleaning-profile.md
│           └── scripts/
│               └── build_corpus.py
└── tests/
    └── test_build_corpus.py
```

---

## Supported Inputs

Corpus files:

- `*.pdf`
- ordinary `*.txt`

Sidecar/metainformation files:

- `*.csv`, `*.tsv`
- `*.json`, `*.jsonl`, `*.ndjson`
- `*.xml`, `*.yaml`, `*.yml`
- `*.xlsx`, `*.xls`
- `*.md`, `*.html`, `*.htm`, `*.log`
- other unknown files

TXT files are treated as metadata/sidecars instead of corpus when their filename contains one of these markers:

- `metadata`
- `meta`
- `manifest`
- `index`
- `catalog`
- `catalogue`
- `readme`

## 支持的输入

语料文件：

- `*.pdf`
- 普通 `*.txt`

Sidecar/metainformation 文件：

- `*.csv`, `*.tsv`
- `*.json`, `*.jsonl`, `*.ndjson`
- `*.xml`, `*.yaml`, `*.yml`
- `*.xlsx`, `*.xls`
- `*.md`, `*.html`, `*.htm`, `*.log`
- 其他无法识别的文件

如果 TXT 文件名包含以下关键词，则默认视为 metadata/sidecar，而不是语料：

- `metadata`
- `meta`
- `manifest`
- `index`
- `catalog`
- `catalogue`
- `readme`

---

## Quick Start

Dependency-free cleaned output:

```bash
python3 skills/public/english-corpus-prep/scripts/build_corpus.py ./raw_corpus \
  --output-dir ./corpus_output \
  --recursive \
  --tagger none \
  --relative-paths
```

Cleaned output plus POS tagging:

```bash
python3 skills/public/english-corpus-prep/scripts/build_corpus.py ./raw_corpus \
  --output-dir ./corpus_output \
  --recursive \
  --tagger auto \
  --copy-sidecars \
  --relative-paths \
  --max-workers 4 \
  --cleaning-profile generic
```

## 快速开始

只生成清洗语料，不生成词性标注：

```bash
python3 skills/public/english-corpus-prep/scripts/build_corpus.py ./raw_corpus \
  --output-dir ./corpus_output \
  --recursive \
  --tagger none \
  --relative-paths
```

生成清洗语料并进行词性标注：

```bash
python3 skills/public/english-corpus-prep/scripts/build_corpus.py ./raw_corpus \
  --output-dir ./corpus_output \
  --recursive \
  --tagger auto \
  --copy-sidecars \
  --relative-paths \
  --max-workers 4 \
  --cleaning-profile generic
```

---

## Command Reference

```bash
python3 skills/public/english-corpus-prep/scripts/build_corpus.py <input-path...> \
  --output-dir <output-dir> \
  [--recursive] \
  [--tagger auto|nltk|spacy|none|heuristic] \
  [--on-tagger-missing fail|skip] \
  [--sidecar-policy copy|manifest-only|skip] \
  [--copy-sidecars] \
  [--relative-paths] \
  [--max-workers N] \
  [--cleaning-profile generic|academic|policy|light] \
  [--collection-name NAME]
```

### `<input-path...>`

One or more files or directories. Directory inputs can contain arbitrary subdirectory depth.

### `--output-dir <output-dir>`

New output root. The script creates cleaned/tagged trees, logs, reports, and config files here. The original input files are not modified.

### `--recursive`

Scan subdirectories recursively. Recommended for real corpus folders.

### `--tagger auto|nltk|spacy|none|heuristic`

Controls POS tagging.

- `auto`: try NLTK first, then spaCy.
- `nltk`: use NLTK `pos_tag`, which emits PTB-style tags.
- `spacy`: use spaCy English fine-grained tags.
- `none`: skip tagged output.
- `heuristic`: use the built-in no-dependency tagger. This emits PTB-shaped output for smoke tests, but it is not reliable enough for serious linguistic analysis.

### `--on-tagger-missing fail|skip`

Controls behavior when the requested real tagger is unavailable.

- `fail`: stop the run and print installation guidance.
- `skip`: generate cleaned output only.

### `--sidecar-policy copy|manifest-only|skip`

Controls non-corpus files.

- `copy`: copy sidecars into the mirrored cleaned/tagged trees.
- `manifest-only`: do not copy sidecars, only record them in logs.
- `skip`: ignore sidecars.

### `--copy-sidecars`

Compatibility alias for `--sidecar-policy copy`.

### `--relative-paths`

Write relative paths in manifests and reports. Recommended when output may be shared or published.

### `--max-workers N`

Number of corpus files processed concurrently. Use `1` for maximum simplicity, or `4` for moderate parallelism.

### `--cleaning-profile generic|academic|policy|light`

Cleaning strength.

- `generic`: default, general English corpus cleaning.
- `academic`: for papers and academic reports; removes trailing references/bibliography sections.
- `policy`: for institutional and policy reports; conservative, no hard-coded agency names.
- `light`: minimal normalization only.

### `--collection-name NAME`

Override the output tree prefix. If omitted, a single input directory uses its directory name; multiple inputs use `corpus`.

## 命令参数说明

```bash
python3 skills/public/english-corpus-prep/scripts/build_corpus.py <输入路径...> \
  --output-dir <输出目录> \
  [--recursive] \
  [--tagger auto|nltk|spacy|none|heuristic] \
  [--on-tagger-missing fail|skip] \
  [--sidecar-policy copy|manifest-only|skip] \
  [--copy-sidecars] \
  [--relative-paths] \
  [--max-workers N] \
  [--cleaning-profile generic|academic|policy|light] \
  [--collection-name NAME]
```

### `<输入路径...>`

一个或多个文件或目录。目录可以包含任意层级的子目录。

### `--output-dir <输出目录>`

新的输出根目录。脚本会在这里创建 cleaned/tagged 目录树、日志、报告和配置文件。原始输入文件不会被修改。

### `--recursive`

递归扫描子目录。真实语料库目录建议开启。

### `--tagger auto|nltk|spacy|none|heuristic`

控制词性标注。

- `auto`: 优先尝试 NLTK，然后尝试 spaCy。
- `nltk`: 使用 NLTK `pos_tag`，输出 PTB 风格 tag。
- `spacy`: 使用 spaCy 英文模型的细粒度 tag。
- `none`: 不生成 tagged 输出。
- `heuristic`: 使用内置无依赖启发式标注器。只适合冒烟测试，不适合严肃语言学分析。

### `--on-tagger-missing fail|skip`

当请求的真实 tagger 不可用时如何处理。

- `fail`: 停止运行并打印安装提示。
- `skip`: 只生成 cleaned 输出。

### `--sidecar-policy copy|manifest-only|skip`

控制非语料文件。

- `copy`: 将 sidecar 文件复制到镜像后的 cleaned/tagged 目录树。
- `manifest-only`: 不复制 sidecar，只在日志中记录。
- `skip`: 忽略 sidecar。

### `--copy-sidecars`

兼容参数，等价于 `--sidecar-policy copy`。

### `--relative-paths`

在 manifest 和报告中写入相对路径。推荐在需要分享或发布输出时使用。

### `--max-workers N`

并行处理语料文件数量。`1` 最简单稳定；`4` 适合中等并行。

### `--cleaning-profile generic|academic|policy|light`

清洗强度。

- `generic`: 默认，通用英文语料清洗。
- `academic`: 适合论文和学术报告，会删除尾部 references/bibliography 区段。
- `policy`: 适合机构或政策报告，保守处理，不写死机构名。
- `light`: 只做最小规范化。

### `--collection-name NAME`

手动指定输出目录树前缀。未指定时，单个输入目录使用目录名；多个输入使用 `corpus`。

---

## POS Tagging Format

Tagged output uses Penn Treebank-style `token_TAG` formatting. Punctuation is tagged too.

Example:

```text
This_DT is_VBZ a_DT sample_NN ._.
```

Recommended taggers:

- NLTK `pos_tag`
- spaCy English model with `token.tag_`

The built-in `heuristic` mode is intentionally explicit because it is not a real statistical or neural tagger.

## 词性标注格式

词性标注输出使用 Penn Treebank 风格 `token_TAG` 格式，标点也会标注。

示例：

```text
This_DT is_VBZ a_DT sample_NN ._.
```

推荐 tagger：

- NLTK `pos_tag`
- spaCy 英文模型的 `token.tag_`

内置 `heuristic` 模式必须显式选择，因为它不是真正的统计或神经网络 tagger。

---

## Output Contract

Assume input:

```text
raw_corpus/
├── collection_A/
│   ├── article_001.pdf
│   ├── article_001_metadata.json
│   └── deep/
│       └── article_002.txt
├── metadata.csv
└── readme.txt
```

Output:

```text
corpus_output/
├── raw_corpus_cleaned/
│   ├── collection_A/
│   │   ├── article_001_cleaned.txt
│   │   ├── article_001_metadata.json
│   │   └── deep/
│   │       └── article_002_cleaned.txt
│   ├── metadata.csv
│   └── readme.txt
│
├── raw_corpus_tagged/
│   ├── collection_A/
│   │   ├── article_001_tagged.txt
│   │   ├── article_001_metadata.json
│   │   └── deep/
│   │       └── article_002_tagged.txt
│   ├── metadata.csv
│   └── readme.txt
│
├── logs/
│   ├── errors.jsonl
│   ├── file_inventory.jsonl
│   ├── metadata_candidates.jsonl
│   ├── processing_manifest.jsonl
│   ├── sidecar_manifest.jsonl
│   └── skipped_files.jsonl
│
├── corpus_report_en.md
├── corpus_report_zh.md
├── corpus_report.json
└── config_used.json
```

If `--tagger none` is used, no `*_tagged/` tree is created.

## 输出契约

假设输入：

```text
raw_corpus/
├── collection_A/
│   ├── article_001.pdf
│   ├── article_001_metadata.json
│   └── deep/
│       └── article_002.txt
├── metadata.csv
└── readme.txt
```

输出：

```text
corpus_output/
├── raw_corpus_cleaned/
│   ├── collection_A/
│   │   ├── article_001_cleaned.txt
│   │   ├── article_001_metadata.json
│   │   └── deep/
│   │       └── article_002_cleaned.txt
│   ├── metadata.csv
│   └── readme.txt
│
├── raw_corpus_tagged/
│   ├── collection_A/
│   │   ├── article_001_tagged.txt
│   │   ├── article_001_metadata.json
│   │   └── deep/
│   │       └── article_002_tagged.txt
│   ├── metadata.csv
│   └── readme.txt
│
├── logs/
│   ├── errors.jsonl
│   ├── file_inventory.jsonl
│   ├── metadata_candidates.jsonl
│   ├── processing_manifest.jsonl
│   ├── sidecar_manifest.jsonl
│   └── skipped_files.jsonl
│
├── corpus_report_en.md
├── corpus_report_zh.md
├── corpus_report.json
└── config_used.json
```

如果使用 `--tagger none`，不会创建 `*_tagged/` 目录树。

---

## Logs and Reports

The script writes all logs under `logs/`:

- `file_inventory.jsonl`: every discovered file and its classification.
- `processing_manifest.jsonl`: successfully cleaned corpus files.
- `sidecar_manifest.jsonl`: sidecar files copied or registered.
- `metadata_candidates.jsonl`: likely centralized or adjacent metadata files.
- `errors.jsonl`: parser, encoding, unreadable text, and copy errors.
- `skipped_files.jsonl`: currently mirrors errors for easier downstream inspection.

Reports:

- `corpus_report_en.md`: English human-readable report.
- `corpus_report_zh.md`: Chinese human-readable report.
- `corpus_report.json`: machine-readable summary.
- `config_used.json`: run configuration.

## 日志与报告

脚本会把所有日志写入 `logs/`：

- `file_inventory.jsonl`: 全量文件清单和分类结果。
- `processing_manifest.jsonl`: 成功清洗的语料文件。
- `sidecar_manifest.jsonl`: 被复制或登记的 sidecar 文件。
- `metadata_candidates.jsonl`: 疑似集中式或相邻 metadata 文件。
- `errors.jsonl`: 解析失败、编码失败、无法抽取文本、复制失败等错误。
- `skipped_files.jsonl`: 当前与 errors 一致，便于后续流程检查跳过文件。

报告文件：

- `corpus_report_en.md`: 英文可读报告。
- `corpus_report_zh.md`: 中文可读报告。
- `corpus_report.json`: 机器可读摘要。
- `config_used.json`: 本次运行配置。

---

## Dependencies

Required:

- Python 3.10+

Optional for PDF extraction:

```bash
pip install pypdf
pip install pdfplumber
```

Optional for POS tagging with NLTK:

```bash
pip install nltk
python3 - <<'PY'
import nltk
nltk.download("averaged_perceptron_tagger")
nltk.download("averaged_perceptron_tagger_eng")
PY
```

Optional for POS tagging with spaCy:

```bash
pip install spacy
python3 -m spacy download en_core_web_sm
```

## 依赖

必需：

- Python 3.10+

PDF 抽取可选依赖：

```bash
pip install pypdf
pip install pdfplumber
```

NLTK 词性标注可选依赖：

```bash
pip install nltk
python3 - <<'PY'
import nltk
nltk.download("averaged_perceptron_tagger")
nltk.download("averaged_perceptron_tagger_eng")
PY
```

spaCy 词性标注可选依赖：

```bash
pip install spacy
python3 -m spacy download en_core_web_sm
```

---

## Testing

Run:

```bash
python3 -B -m unittest discover -s tests -v
```

Current tests cover:

- nested directory mirroring
- TXT corpus cleaning
- UTF-8 preservation
- sidecar copying
- metadata candidate detection
- `--tagger none`
- explicit `--tagger heuristic` output shape

## 测试

运行：

```bash
python3 -B -m unittest discover -s tests -v
```

当前测试覆盖：

- 嵌套目录镜像
- TXT 语料清洗
- UTF-8 字符保留
- sidecar 文件复制
- metadata candidate 识别
- `--tagger none`
- 显式 `--tagger heuristic` 输出格式

---

## Design Notes

- This tool does not merge all cleaned text into one corpus file by default. Preserving file-level provenance is more important for messy real-world corpora.
- This tool does not silently downgrade from a real tagger to heuristic mode. If a real tagger is requested and unavailable, the default behavior is to fail.
- This tool avoids project-specific cleaning rules in the default profile. Corpus-specific deletion patterns should be added as explicit profiles or configuration rules.

## 设计说明

- 默认不合并总语料。对于真实复杂语料库，文件级来源关系比一个大文本更重要。
- 不会在真实 tagger 不可用时静默降级到 heuristic。默认行为是失败并提示安装依赖。
- 默认清洗规则不写入项目专用删除模式。特定语料库的删除规则应作为显式 profile 或配置规则加入。
