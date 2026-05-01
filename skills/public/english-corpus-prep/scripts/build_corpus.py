#!/usr/bin/env python3
"""Mirror a PDF/TXT corpus tree, clean corpus files, and optionally emit PTB POS tags."""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import re
import shutil
import sys
import time
import unicodedata
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Iterator, Sequence


PDF_EXTENSIONS = {".pdf"}
TXT_EXTENSIONS = {".txt"}
METADATA_EXTENSIONS = {".csv", ".tsv", ".json", ".jsonl", ".ndjson", ".xml", ".yaml", ".yml", ".xlsx", ".xls"}
TEXT_LIKE_SIDECAR_EXTENSIONS = {".md", ".rst", ".html", ".htm", ".log"}
PTB_PUNCT_TAGS = {
    ",": ",",
    ".": ".",
    ":": ":",
    ";": ":",
    "?": ".",
    "!": ".",
    "(": "-LRB-",
    ")": "-RRB-",
    "[": "-LSB-",
    "]": "-RSB-",
    "{": "-LCB-",
    "}": "-RCB-",
    "``": "``",
    "''": "''",
    '"': '"',
    "'": "'",
    "-": ":",
    "--": ":",
    "...": ":",
    "#": "#",
    "$": "$",
}

UTF8_ENCODINGS = ("utf-8", "utf-8-sig")
UTF16_ENCODINGS = ("utf-16", "utf-16-le", "utf-16-be")
LEGACY_ENCODINGS = ("gb18030", "big5", "shift_jis", "cp1252", "latin-1")
TOKEN_PATTERN = re.compile(r"\w+(?:['’]\w+)?|\d+(?:\.\d+)?|\.{3}|--|[^\w\s]", re.UNICODE)
URL_PATTERN = re.compile(r"https?://\S+|www\.\S+", re.IGNORECASE)


@dataclass(frozen=True)
class FileEntry:
    source: str
    relative_path: str
    detected_format: str
    role: str
    file_size_bytes: int
    metadata_candidate: bool


@dataclass(frozen=True)
class ProcessRecord:
    source: str
    relative_path: str
    detected_format: str
    source_encoding: str
    converted_to_utf8: bool
    cleaned_output: str
    tagged_output: str | None
    chars_raw: int
    chars_clean: int
    tokens_clean: int
    pdf_pages: int | None
    pdf_empty_pages: int | None


@dataclass(frozen=True)
class SidecarRecord:
    source: str
    relative_path: str
    detected_format: str
    cleaned_copy: str | None
    tagged_copy: str | None
    metadata_candidate: bool


@dataclass(frozen=True)
class ErrorRecord:
    timestamp_epoch: float
    source: str
    relative_path: str
    detected_format: str
    error_code: str
    message: str
    hint: str


@dataclass(frozen=True)
class ExtractedText:
    text: str
    source_encoding: str
    converted_to_utf8: bool
    pdf_pages: int | None = None
    pdf_empty_pages: int | None = None


@dataclass(frozen=True)
class ProcessOutcome:
    record: ProcessRecord | None
    error: ErrorRecord | None


class CorpusError(Exception):
    def __init__(self, code: str, message: str, hint: str = "") -> None:
        super().__init__(message)
        self.code = code
        self.hint = hint


class TaggerUnavailable(CorpusError):
    pass


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean PDF/TXT corpus files while preserving the original directory tree."
    )
    parser.add_argument("inputs", nargs="+", help="Files or directories to ingest.")
    parser.add_argument("--output-dir", required=True, help="Directory where all outputs are written.")
    parser.add_argument("--recursive", action="store_true", help="Recurse into input directories.")
    parser.add_argument(
        "--tagger",
        choices=("auto", "nltk", "spacy", "none", "heuristic"),
        default="auto",
        help="POS tagger for *_tagged output. Default: auto.",
    )
    parser.add_argument(
        "--on-tagger-missing",
        choices=("fail", "skip"),
        default="fail",
        help="What to do when the requested tagger is unavailable. Default: fail.",
    )
    parser.add_argument(
        "--sidecar-policy",
        choices=("copy", "manifest-only", "skip"),
        default="copy",
        help="How to handle non-PDF/TXT files. Default: copy.",
    )
    parser.add_argument(
        "--copy-sidecars",
        action="store_true",
        help="Compatibility flag equivalent to --sidecar-policy copy.",
    )
    parser.add_argument(
        "--relative-paths",
        action="store_true",
        help="Store relative paths in manifests and reports instead of absolute paths.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=1,
        help="Number of files to process concurrently. Default: 1.",
    )
    parser.add_argument(
        "--cleaning-profile",
        choices=("generic", "academic", "policy", "light"),
        default="generic",
        help="Cleaning strength. Default: generic.",
    )
    parser.add_argument(
        "--collection-name",
        help="Override the output tree prefix. Defaults to the single input name or 'corpus'.",
    )
    parser.add_argument(
        "--assume-yes",
        action="store_true",
        help="Reserved for compatibility with older runs; this version does not prompt interactively.",
    )
    return parser.parse_args()


def utc_now() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


def sanitize_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", value).strip("._-")
    return cleaned or "corpus"


def source_value(path: Path, relative_paths: bool) -> str:
    if not relative_paths:
        return str(path.resolve())
    try:
        return str(path.resolve().relative_to(Path.cwd().resolve()))
    except ValueError:
        return path.name


def source_record_value(path: Path, rel: Path, relative_paths: bool) -> str:
    return str(rel) if relative_paths else str(path.resolve())


def human_size(num_bytes: int) -> str:
    if num_bytes < 1024:
        return f"{num_bytes} B"
    if num_bytes < 1024**2:
        return f"{num_bytes / 1024:.2f} KB"
    if num_bytes < 1024**3:
        return f"{num_bytes / (1024**2):.2f} MB"
    return f"{num_bytes / (1024**3):.2f} GB"


def collection_name(inputs: Sequence[str], explicit: str | None) -> str:
    if explicit:
        return sanitize_name(explicit)
    existing = [Path(item) for item in inputs if Path(item).exists()]
    if len(existing) == 1:
        path = existing[0]
        return sanitize_name(path.name if path.is_dir() else path.stem)
    return "corpus"


def is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def iter_input_files(inputs: Sequence[str], recursive: bool, output_dir: Path) -> Iterator[tuple[Path, Path]]:
    existing_inputs = [Path(item) for item in inputs if Path(item).exists()]
    multiple_inputs = len(existing_inputs) > 1

    for item in inputs:
        input_path = Path(item)
        if not input_path.exists():
            print(f"[WARN] Missing path: {input_path}", file=sys.stderr)
            continue

        if input_path.is_file():
            rel = Path(sanitize_name(input_path.parent.name)) / input_path.name if multiple_inputs else Path(input_path.name)
            if not is_relative_to(input_path, output_dir):
                yield input_path, rel
            continue

        iterator: Iterable[Path] = input_path.rglob("*") if recursive else input_path.glob("*")
        prefix = Path(sanitize_name(input_path.name)) if multiple_inputs else Path()
        for subpath in iterator:
            if not subpath.is_file() or is_relative_to(subpath, output_dir):
                continue
            try:
                rel = prefix / subpath.relative_to(input_path)
            except ValueError:
                rel = prefix / subpath.name
            yield subpath, rel


def detect_format(path: Path, sample: bytes) -> tuple[str, str, bool]:
    ext = path.suffix.lower()
    lower_name = path.name.lower()
    metadata_candidate = ext in METADATA_EXTENSIONS or any(
        marker in lower_name for marker in ("metadata", "meta", "manifest", "index", "catalogue", "catalog", "readme")
    )

    if sample.startswith(b"%PDF-") or ext in PDF_EXTENSIONS:
        return "pdf", "corpus", metadata_candidate
    if ext in TXT_EXTENSIONS and metadata_candidate:
        return "txt", "sidecar", True
    if ext in TXT_EXTENSIONS:
        return "txt", "corpus", metadata_candidate
    if ext in METADATA_EXTENSIONS:
        return ext.lstrip("."), "sidecar", True
    if ext in TEXT_LIKE_SIDECAR_EXTENSIONS:
        return ext.lstrip("."), "sidecar", metadata_candidate
    return ext.lstrip(".") or "unknown", "sidecar", metadata_candidate


def build_inventory(inputs: Sequence[str], recursive: bool, output_dir: Path, relative_paths: bool) -> list[FileEntry]:
    entries: list[FileEntry] = []
    for path, rel in iter_input_files(inputs, recursive, output_dir):
        try:
            size = path.stat().st_size
            sample = path.read_bytes()[:4096]
        except OSError as exc:
            entries.append(
                FileEntry(
                    source=source_record_value(path, rel, relative_paths),
                    relative_path=str(rel),
                    detected_format="unknown",
                    role="unreadable",
                    file_size_bytes=0,
                    metadata_candidate=False,
                )
            )
            print(f"[WARN] Cannot inspect {path}: {exc}", file=sys.stderr)
            continue
        fmt, role, metadata_candidate = detect_format(path, sample)
        entries.append(
            FileEntry(
                source=source_record_value(path, rel, relative_paths),
                relative_path=str(rel),
                detected_format=fmt,
                role=role,
                file_size_bytes=size,
                metadata_candidate=metadata_candidate,
            )
        )
    return entries


def decode_to_text(raw: bytes, path: Path) -> tuple[str, str, bool]:
    for encoding in UTF8_ENCODINGS:
        try:
            text = raw.decode(encoding, errors="strict")
            return text, "utf-8", encoding != "utf-8"
        except UnicodeDecodeError:
            pass

    sample = raw[:4096]
    looks_utf16 = raw.startswith((b"\xff\xfe", b"\xfe\xff")) or (bool(sample) and sample.count(0) / len(sample) > 0.2)
    if looks_utf16:
        for encoding in UTF16_ENCODINGS:
            try:
                return raw.decode(encoding, errors="strict"), encoding, True
            except UnicodeDecodeError:
                pass

    for encoding in LEGACY_ENCODINGS:
        try:
            return raw.decode(encoding, errors="strict"), encoding, True
        except UnicodeDecodeError:
            pass

    raise CorpusError("ENCODING_ERROR", f"Cannot decode {path}", "Convert the file to UTF-8 and retry.")


def extract_txt(path: Path) -> ExtractedText:
    text, encoding, converted = decode_to_text(path.read_bytes(), path)
    return ExtractedText(text=text, source_encoding=encoding, converted_to_utf8=converted)


def extract_pdf(path: Path) -> ExtractedText:
    errors: list[str] = []

    try:
        from pypdf import PdfReader  # type: ignore

        page_texts: list[str] = []
        with contextlib.redirect_stderr(io.StringIO()):
            reader = PdfReader(str(path))
            for page in reader.pages:
                page_texts.append(page.extract_text() or "")
        return ExtractedText(
            text="\n\n".join(page_texts),
            source_encoding="utf-8",
            converted_to_utf8=False,
            pdf_pages=len(page_texts),
            pdf_empty_pages=sum(1 for text in page_texts if not text.strip()),
        )
    except Exception as exc:
        errors.append(f"pypdf: {exc}")

    try:
        import pdfplumber  # type: ignore

        page_texts = []
        with contextlib.redirect_stderr(io.StringIO()):
            with pdfplumber.open(str(path)) as pdf:
                for page in pdf.pages:
                    page_texts.append(page.extract_text() or "")
        return ExtractedText(
            text="\n\n".join(page_texts),
            source_encoding="utf-8",
            converted_to_utf8=False,
            pdf_pages=len(page_texts),
            pdf_empty_pages=sum(1 for text in page_texts if not text.strip()),
        )
    except Exception as exc:
        errors.append(f"pdfplumber: {exc}")

    raise CorpusError(
        "PARSE_ERROR",
        f"Cannot extract PDF text from {path}",
        "Install pypdf/pdfplumber, check whether the PDF is corrupted, or OCR scanned PDFs first.",
    ) from Exception(" | ".join(errors))


def normalize_unicode_text(text: str) -> str:
    replacements = {
        "\u00ad": "",
        "\u00a0": " ",
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2013": "-",
        "\u2014": "-",
        "\u2212": "-",
        "\ufeff": "",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return unicodedata.normalize("NFKC", text)


def clean_text(text: str, profile: str) -> str:
    text = normalize_unicode_text(text)
    text = re.sub(r"([^\W\d_])-\n([^\W\d_])", r"\1\2", text, flags=re.UNICODE)
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{4,}", "\n\n\n", text)

    lines: list[str] = []
    in_references_tail = False
    for line in text.split("\n"):
        cleaned = line.strip()
        if not cleaned:
            if lines and lines[-1] != "":
                lines.append("")
            continue

        lowered = cleaned.lower()
        if re.fullmatch(r"page\s+\d+(\s+of\s+\d+)?", lowered):
            continue
        if re.fullmatch(r"\d+", cleaned):
            continue
        if lowered == "this page is intentionally left blank.":
            continue

        if profile in {"academic", "policy"} and lowered in {"references", "bibliography", "works cited"}:
            in_references_tail = True
            continue
        if in_references_tail:
            continue

        if profile != "light":
            cleaned = URL_PATTERN.sub("", cleaned).strip()
            if not cleaned:
                continue

        lines.append(cleaned)

    cleaned_text = "\n".join(lines)
    cleaned_text = re.sub(r"\n{3,}", "\n\n", cleaned_text)
    return cleaned_text.strip()


def tokenize(text: str) -> list[str]:
    return TOKEN_PATTERN.findall(text)


def count_tokens_and_types(text: str) -> tuple[int, int]:
    tokens = tokenize(text)
    return len(tokens), len({token.lower() for token in tokens})


def tag_for_punctuation(token: str) -> str | None:
    if token in PTB_PUNCT_TAGS:
        return PTB_PUNCT_TAGS[token]
    if re.fullmatch(r"[^\w\s]", token, flags=re.UNICODE):
        return token
    return None


Tagger = Callable[[list[str]], list[tuple[str, str]]]


def heuristic_tagger(tokens: list[str]) -> list[tuple[str, str]]:
    tagged: list[tuple[str, str]] = []
    determiners = {"a", "an", "the", "this", "that", "these", "those"}
    be_verbs = {"am": "VBP", "is": "VBZ", "are": "VBP", "was": "VBD", "were": "VBD", "be": "VB", "been": "VBN"}
    for token in tokens:
        punct = tag_for_punctuation(token)
        lower = token.lower()
        if punct:
            tag = punct
        elif re.fullmatch(r"\d+(?:\.\d+)?", token):
            tag = "CD"
        elif lower in determiners:
            tag = "DT"
        elif lower in be_verbs:
            tag = be_verbs[lower]
        elif lower in {"and", "or", "but"}:
            tag = "CC"
        elif lower == "to":
            tag = "TO"
        elif lower.endswith("ing"):
            tag = "VBG"
        elif lower.endswith("ed"):
            tag = "VBD"
        elif token[:1].isupper():
            tag = "NNP"
        elif lower.endswith("s") and len(lower) > 3:
            tag = "NNS"
        else:
            tag = "NN"
        tagged.append((token, tag))
    return tagged


def nltk_tagger() -> Tagger:
    import nltk  # type: ignore

    def tag(tokens: list[str]) -> list[tuple[str, str]]:
        output: list[tuple[str, str]] = []
        word_tokens: list[str] = []
        word_positions: list[int] = []
        for idx, token in enumerate(tokens):
            punct = tag_for_punctuation(token)
            if punct:
                output.append((token, punct))
            else:
                output.append((token, ""))
                word_tokens.append(token)
                word_positions.append(idx)
        if word_tokens:
            tagged_words = nltk.pos_tag(word_tokens)
            for pos, (_, tag_value) in zip(word_positions, tagged_words):
                output[pos] = (tokens[pos], tag_value)
        return output

    tag(["This", "is", "a", "test", "."])
    return tag


def spacy_tagger() -> Tagger:
    import spacy  # type: ignore

    nlp = spacy.load("en_core_web_sm")

    def tag(tokens: list[str]) -> list[tuple[str, str]]:
        doc = nlp(" ".join(tokens))
        return [(token.text, tag_for_punctuation(token.text) or token.tag_) for token in doc]

    tag(["This", "is", "a", "test", "."])
    return tag


def resolve_tagger(name: str, on_missing: str) -> tuple[Tagger | None, str, str | None]:
    if name == "none":
        return None, "none", None
    if name == "heuristic":
        return heuristic_tagger, "heuristic", "Heuristic tagger selected explicitly; output is PTB-shaped but not linguistically reliable."

    attempts = ("nltk", "spacy") if name == "auto" else (name,)
    errors: list[str] = []
    for attempt in attempts:
        try:
            if attempt == "nltk":
                return nltk_tagger(), "nltk", None
            if attempt == "spacy":
                return spacy_tagger(), "spacy", None
        except Exception as exc:
            errors.append(f"{attempt}: {exc}")

    message = "No requested POS tagger is available. " + " | ".join(errors)
    hint = "Install NLTK tagger data or spaCy en_core_web_sm, or rerun with --tagger none."
    if on_missing == "skip":
        print(f"[WARN] {message} Tagged output will be skipped. {hint}", file=sys.stderr)
        return None, "none", message
    raise TaggerUnavailable("TAGGER_UNAVAILABLE", message, hint)


def build_pos_template(text: str, tagger: Tagger) -> str:
    lines: list[str] = []
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            lines.append("")
            continue
        tagged = tagger(tokenize(stripped))
        lines.append(" ".join(f"{token}_{tag}" for token, tag in tagged))
    return "\n".join(lines).strip()


def cleaned_output_path(cleaned_root: Path, rel: Path) -> Path:
    return cleaned_root / rel.parent / f"{sanitize_name(rel.stem)}_cleaned.txt"


def tagged_output_path(tagged_root: Path, rel: Path) -> Path:
    return tagged_root / rel.parent / f"{sanitize_name(rel.stem)}_tagged.txt"


def error_record(path: Path, rel: Path, fmt: str, code: str, message: str, hint: str, relative_paths: bool) -> ErrorRecord:
    return ErrorRecord(
        timestamp_epoch=time.time(),
        source=source_record_value(path, rel, relative_paths),
        relative_path=str(rel),
        detected_format=fmt,
        error_code=code,
        message=message,
        hint=hint,
    )


def process_corpus_file(
    path: Path,
    rel: Path,
    fmt: str,
    cleaned_root: Path,
    tagged_root: Path | None,
    profile: str,
    tagger: Tagger | None,
    relative_paths: bool,
) -> ProcessOutcome:
    try:
        extraction = extract_pdf(path) if fmt == "pdf" else extract_txt(path)
        if not extraction.text.strip():
            return ProcessOutcome(
                None,
                error_record(
                    path,
                    rel,
                    fmt,
                    "TEXT_UNREADABLE",
                    "No readable text content extracted.",
                    "For scanned PDFs or images, run OCR first.",
                    relative_paths,
                ),
            )

        cleaned = clean_text(extraction.text, profile)
        if not cleaned:
            return ProcessOutcome(
                None,
                error_record(
                    path,
                    rel,
                    fmt,
                    "EMPTY_AFTER_CLEAN",
                    "Text became empty after cleaning.",
                    "Use --cleaning-profile light if this file should contain corpus text.",
                    relative_paths,
                ),
            )

        clean_path = cleaned_output_path(cleaned_root, rel)
        clean_path.parent.mkdir(parents=True, exist_ok=True)
        clean_path.write_text(cleaned + "\n", encoding="utf-8")

        tagged_path: Path | None = None
        if tagger and tagged_root:
            tagged_text = build_pos_template(cleaned, tagger)
            tagged_path = tagged_output_path(tagged_root, rel)
            tagged_path.parent.mkdir(parents=True, exist_ok=True)
            tagged_path.write_text(tagged_text + ("\n" if tagged_text else ""), encoding="utf-8")

        token_count, _ = count_tokens_and_types(cleaned)
        return ProcessOutcome(
            ProcessRecord(
                source=source_record_value(path, rel, relative_paths),
                relative_path=str(rel),
                detected_format=fmt,
                source_encoding=extraction.source_encoding,
                converted_to_utf8=extraction.converted_to_utf8,
                cleaned_output=str(clean_path.relative_to(cleaned_root.parent)) if relative_paths else str(clean_path.resolve()),
                tagged_output=str(tagged_path.relative_to(tagged_root.parent)) if tagged_path and tagged_root and relative_paths else (str(tagged_path.resolve()) if tagged_path else None),
                chars_raw=len(extraction.text),
                chars_clean=len(cleaned),
                tokens_clean=token_count,
                pdf_pages=extraction.pdf_pages,
                pdf_empty_pages=extraction.pdf_empty_pages,
            ),
            None,
        )
    except CorpusError as exc:
        return ProcessOutcome(None, error_record(path, rel, fmt, exc.code, str(exc), exc.hint, relative_paths))
    except Exception as exc:
        return ProcessOutcome(
            None,
            error_record(
                path,
                rel,
                fmt,
                "UNEXPECTED_ERROR",
                f"Unexpected processing failure: {exc}",
                "Inspect file integrity and parser dependencies.",
                relative_paths,
            ),
        )


def copy_sidecar(
    path: Path,
    rel: Path,
    fmt: str,
    metadata_candidate: bool,
    cleaned_root: Path,
    tagged_root: Path | None,
    policy: str,
    relative_paths: bool,
) -> SidecarRecord | None:
    if policy == "skip":
        return None

    cleaned_copy: Path | None = None
    tagged_copy: Path | None = None
    if policy == "copy":
        cleaned_copy = cleaned_root / rel
        cleaned_copy.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, cleaned_copy)
        if tagged_root:
            tagged_copy = tagged_root / rel
            tagged_copy.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, tagged_copy)

    return SidecarRecord(
        source=source_record_value(path, rel, relative_paths),
        relative_path=str(rel),
        detected_format=fmt,
        cleaned_copy=str(cleaned_copy.relative_to(cleaned_root.parent)) if cleaned_copy and relative_paths else (str(cleaned_copy.resolve()) if cleaned_copy else None),
        tagged_copy=str(tagged_copy.relative_to(tagged_root.parent)) if tagged_copy and tagged_root and relative_paths else (str(tagged_copy.resolve()) if tagged_copy else None),
        metadata_candidate=metadata_candidate,
    )


def write_jsonl(path: Path, rows: Iterable[object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            if hasattr(row, "__dataclass_fields__"):
                payload = asdict(row)
            else:
                payload = row
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def write_config(path: Path, args: argparse.Namespace, tagger_name: str, collection: str) -> None:
    payload = {
        "created_at_utc": utc_now(),
        "inputs": args.inputs,
        "output_dir": args.output_dir,
        "recursive": args.recursive,
        "tagger_requested": args.tagger,
        "tagger_used": tagger_name,
        "on_tagger_missing": args.on_tagger_missing,
        "sidecar_policy": args.sidecar_policy,
        "relative_paths": args.relative_paths,
        "max_workers": args.max_workers,
        "cleaning_profile": args.cleaning_profile,
        "collection_name": collection,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def report_markdown(language: str, report: dict) -> str:
    zh = language == "zh"
    title = "语料库处理报告" if zh else "Corpus Processing Report"
    lines = [f"# {title}", ""]
    labels = {
        "created": "生成时间" if zh else "Generated at",
        "input": "输入文件数" if zh else "Input files",
        "processed": "已处理语料文件" if zh else "Processed corpus files",
        "sidecars": "Sidecar/元信息文件" if zh else "Sidecar/metadata files",
        "skipped": "跳过/失败文件" if zh else "Skipped/failed files",
        "size": "输入总大小" if zh else "Total input size",
        "cleaned": "清洗输出目录" if zh else "Cleaned output tree",
        "tagged": "词性标注输出目录" if zh else "Tagged output tree",
        "tagger": "词性标注器" if zh else "POS tagger",
        "profile": "清洗配置" if zh else "Cleaning profile",
    }
    lines.extend(
        [
            f"- **{labels['created']}**: {report['created_at_utc']}",
            f"- **{labels['input']}**: {report['input_files_total']}",
            f"- **{labels['processed']}**: {report['processed_corpus_files']}",
            f"- **{labels['sidecars']}**: {report['sidecar_files']}",
            f"- **{labels['skipped']}**: {report['skipped_or_failed_files']}",
            f"- **{labels['size']}**: {report['total_input_size_human']}",
            f"- **{labels['cleaned']}**: `{report['outputs']['cleaned_tree']}`",
            f"- **{labels['tagged']}**: `{report['outputs']['tagged_tree'] or 'N/A'}`",
            f"- **{labels['tagger']}**: {report['tagger']['used']}",
            f"- **{labels['profile']}**: {report['cleaning_profile']}",
            "",
        ]
    )

    section = "格式分布" if zh else "Format Distribution"
    lines.extend([f"## {section}", ""])
    for key, value in report["format_distribution"].items():
        lines.append(f"- `{key}`: {value}")
    lines.append("")

    section = "错误汇总" if zh else "Error Summary"
    lines.extend([f"## {section}", ""])
    if report["error_distribution"]:
        for key, value in report["error_distribution"].items():
            lines.append(f"- `{key}`: {value}")
    else:
        lines.append("无错误。" if zh else "No errors.")
    lines.append("")

    section = "说明" if zh else "Notes"
    lines.extend([f"## {section}", ""])
    if zh:
        lines.append("输出目录保留原始相对目录结构。PDF/TXT 会被清洗为 `*_cleaned.txt`；启用词性标注时会生成 `*_tagged.txt`。")
        lines.append("非 PDF/TXT 文件按 sidecar 策略处理，集中式 metadata 会记录在日志中的 `metadata_candidates.jsonl`。")
    else:
        lines.append("The output trees preserve the original relative directory structure. PDF/TXT files become `*_cleaned.txt`; tagged output uses `*_tagged.txt` when enabled.")
        lines.append("Non-PDF/TXT files are handled according to the sidecar policy. Central metadata candidates are recorded in `logs/metadata_candidates.jsonl`.")
    if report["tagger"]["warning"]:
        lines.extend(["", f"> {report['tagger']['warning']}"])
    lines.append("")
    return "\n".join(lines)


def main() -> int:
    args = parse_args()
    if args.copy_sidecars:
        args.sidecar_policy = "copy"
    args.max_workers = max(1, args.max_workers)

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    collection = collection_name(args.inputs, args.collection_name)
    cleaned_root = output_dir / f"{collection}_cleaned"
    tagged_root = output_dir / f"{collection}_tagged" if args.tagger != "none" else None
    logs_dir = output_dir / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    cleaned_root.mkdir(parents=True, exist_ok=True)

    try:
        tagger, tagger_name, tagger_warning = resolve_tagger(args.tagger, args.on_tagger_missing)
    except TaggerUnavailable as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        if exc.hint:
            print(f"[HINT] {exc.hint}", file=sys.stderr)
        return 2

    if tagger and tagged_root:
        tagged_root.mkdir(parents=True, exist_ok=True)
    elif args.tagger != "none":
        tagged_root = None

    inventory = build_inventory(args.inputs, args.recursive, output_dir, args.relative_paths)
    write_jsonl(logs_dir / "file_inventory.jsonl", inventory)

    corpus_jobs: list[tuple[Path, Path, str]] = []
    sidecar_records: list[SidecarRecord] = []
    errors: list[ErrorRecord] = []
    path_lookup: dict[str, tuple[Path, Path]] = {}

    for path, rel in iter_input_files(args.inputs, args.recursive, output_dir):
        path_lookup[str(rel)] = (path, rel)

    for entry in inventory:
        lookup = path_lookup.get(entry.relative_path)
        if not lookup:
            errors.append(
                ErrorRecord(time.time(), entry.source, entry.relative_path, entry.detected_format, "FILE_IO_ERROR", "File disappeared before processing.", "Re-run after checking the input tree.")
            )
            continue
        path, rel = lookup
        if entry.role == "corpus":
            corpus_jobs.append((path, rel, entry.detected_format))
        elif entry.role == "sidecar":
            try:
                record = copy_sidecar(
                    path,
                    rel,
                    entry.detected_format,
                    entry.metadata_candidate,
                    cleaned_root,
                    tagged_root,
                    args.sidecar_policy,
                    args.relative_paths,
                )
                if record:
                    sidecar_records.append(record)
            except OSError as exc:
                errors.append(
                    error_record(path, rel, entry.detected_format, "SIDECAR_COPY_ERROR", f"Cannot copy sidecar: {exc}", "Check file permissions and free disk space.", args.relative_paths)
                )
        else:
            errors.append(
                ErrorRecord(time.time(), entry.source, entry.relative_path, entry.detected_format, "FILE_IO_ERROR", "Cannot inspect file.", "Check file permissions.")
            )

    process_records: list[ProcessRecord] = []
    with ThreadPoolExecutor(max_workers=args.max_workers) as executor:
        future_map = {
            executor.submit(
                process_corpus_file,
                path,
                rel,
                fmt,
                cleaned_root,
                tagged_root,
                args.cleaning_profile,
                tagger,
                args.relative_paths,
            ): (path, rel)
            for path, rel, fmt in corpus_jobs
        }
        for future in as_completed(future_map):
            outcome = future.result()
            if outcome.record:
                process_records.append(outcome.record)
                print(f"[OK] {outcome.record.relative_path}")
            if outcome.error:
                errors.append(outcome.error)
                print(f"[WARN] {outcome.error.relative_path}: {outcome.error.message}", file=sys.stderr)

    process_records.sort(key=lambda item: item.relative_path)
    sidecar_records.sort(key=lambda item: item.relative_path)
    errors.sort(key=lambda item: item.relative_path)

    write_jsonl(logs_dir / "processing_manifest.jsonl", process_records)
    write_jsonl(logs_dir / "sidecar_manifest.jsonl", sidecar_records)
    write_jsonl(logs_dir / "metadata_candidates.jsonl", [record for record in sidecar_records if record.metadata_candidate])
    write_jsonl(logs_dir / "errors.jsonl", errors)
    write_jsonl(logs_dir / "skipped_files.jsonl", errors)
    write_config(output_dir / "config_used.json", args, tagger_name, collection)

    total_size = sum(entry.file_size_bytes for entry in inventory)
    format_distribution = Counter(entry.detected_format for entry in inventory)
    error_distribution = Counter(error.error_code for error in errors)
    clean_tokens = sum(record.tokens_clean for record in process_records)
    clean_chars = sum(record.chars_clean for record in process_records)
    converted_count = sum(1 for record in process_records if record.converted_to_utf8)

    report = {
        "created_at_utc": utc_now(),
        "input_files_total": len(inventory),
        "processed_corpus_files": len(process_records),
        "sidecar_files": len(sidecar_records),
        "skipped_or_failed_files": len(errors),
        "total_input_size_bytes": total_size,
        "total_input_size_human": human_size(total_size),
        "format_distribution": dict(sorted(format_distribution.items())),
        "error_distribution": dict(sorted(error_distribution.items())),
        "converted_to_utf8_files": converted_count,
        "cleaning_profile": args.cleaning_profile,
        "cleaned_corpus": {
            "file_count": len(process_records),
            "token_count": clean_tokens,
            "char_count": clean_chars,
        },
        "tagger": {
            "requested": args.tagger,
            "used": tagger_name,
            "warning": tagger_warning,
        },
        "sidecar_policy": args.sidecar_policy,
        "outputs": {
            "cleaned_tree": source_value(cleaned_root, args.relative_paths),
            "tagged_tree": source_value(tagged_root, args.relative_paths) if tagged_root else None,
            "logs_dir": source_value(logs_dir, args.relative_paths),
        },
    }
    (output_dir / "corpus_report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output_dir / "corpus_report_en.md").write_text(report_markdown("en", report), encoding="utf-8")
    (output_dir / "corpus_report_zh.md").write_text(report_markdown("zh", report), encoding="utf-8")

    print(f"[DONE] Processed corpus files: {len(process_records)}")
    print(f"[DONE] Cleaned tree: {cleaned_root}")
    if tagged_root:
        print(f"[DONE] Tagged tree: {tagged_root}")
    print(f"[DONE] Logs: {logs_dir}")
    return 0 if not errors else 1


if __name__ == "__main__":
    raise SystemExit(main())
