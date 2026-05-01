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
MIME_LINE_PATTERN = re.compile(r"^[a-z]+/[a-z0-9.+-]+$", re.IGNORECASE)
PATH_LIKE_PATTERN = re.compile(r"[A-Za-z]:\\|[/\\][A-Za-z0-9_.-]+[/\\][A-Za-z0-9_.-]+")
FRAGMENT_LEAD_PATTERN = re.compile(r"^[a-z]{1,4}\b")
FRAGMENT_TAIL_PATTERN = re.compile(r"([A-Za-z]{3,12})$")
FRAGMENT_LEAD_LONG_PATTERN = re.compile(r"^[a-z]{4,8}\b")
MONTH_DATE_LINE_PATTERN = re.compile(
    r"^(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2},\s+\d{4}$",
    re.IGNORECASE,
)
SOCIAL_SHARE_PATTERN = re.compile(
    r"(share to facebook|share to x|share to linkedin|share this page|share via email|share ia email)",
    re.IGNORECASE,
)
FEDERAL_REGISTER_CONTROL_PATTERNS = (
    re.compile(r"VerDate\s+.*?(?:RULES\d*|PRESDOC\d*|EXECORD\d*|MISCELLANEOUS)?\s*$", re.IGNORECASE),
    re.compile(r"^\d{4,6}\s+Federal Register\s*/\s*Vol\..*$", re.IGNORECASE),
    re.compile(r"^\s*\[FR Doc\..*$", re.IGNORECASE),
    re.compile(r"^\s*Filed\s+\d+[-/]\d+[-/]\d+;\s+.*$", re.IGNORECASE),
    re.compile(r"^\s*Billing code\s+.*$", re.IGNORECASE),
    re.compile(r"^.*\.EPS</GPH>.*$", re.IGNORECASE),
    re.compile(r"^[A-Za-z0-9_-]+\s+on\s+DSK[A-Z0-9]+.*$", re.IGNORECASE),
    re.compile(r"^\s*PO\s+\d+\s+Frm\s+\d+.*$", re.IGNORECASE),
    re.compile(r"^\s*Jkt\s+\d+.*$", re.IGNORECASE),
)
INLINE_BOILERPLATE_PATTERNS = (
    re.compile(r"\bBILLING CODE\s+[0-9A-Z]+(?:[-–—][0-9A-Z]+)*(?:\s*[-–—]\s*[A-Z])?\b", re.IGNORECASE),
    re.compile(r"\bwas this page helpful\??\b", re.IGNORECASE),
    re.compile(r"\bback to top\b", re.IGNORECASE),
    re.compile(r"\blast modified:\s*(?:[A-Za-z]+\s+\d{1,2},\s+\d{4})?\b", re.IGNORECASE),
    re.compile(r"\b\d+\s+of\s+\d+\s+pages?\s+of\s+\d+\s+results,\s*ordered by relevance\b\.?", re.IGNORECASE),
    re.compile(r"\)\s*or https:// means you've safely connected to the \.gov website\.", re.IGNORECASE),
    re.compile(r"share sensitive information only on official,\s*secure websites\.?", re.IGNORECASE),
    re.compile(r"\bnext\s*[»>]+\b", re.IGNORECASE),
)

BOILERPLATE_PREFIXES = (
    "skip to main content",
    "official websites use .gov",
    "a .gov website belongs to an official government organization",
    "secure .gov websites use https",
    "share sensitive information only on official, secure websites",
    "search results |",
    "search nist",
    "include archived content",
    "use \"\" to search for an exact phrase",
    "home • the administration • office of science and technology policy",
)

NAV_SINGLE_LINES = {
    "about",
    "pressroom",
    "ostp blog",
    "divisions",
    "initiatives",
    "r&d budgets",
    "resource library",
    "nstc",
    "pcast",
    "contact",
    "members",
    "executive order",
    "committees",
    "documents & reports",
    "archives",
    "search",
    "relevance",
    "date",
    "next »",
    "meetings",
    "quick links",
    "webcasts",
    "news archive",
    "connect with pcast",
}
NAV_SCRAPE_HINTS = (
    "home",
    "about",
    "contact",
    "reports",
    "documents",
    "resource library",
    "view all reports",
    "archives",
    "search",
    "website",
    "office of science and technology policy",
    "the administration",
)
COMMON_FUNCTION_WORDS = {
    "a",
    "an",
    "and",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "if",
    "in",
    "into",
    "is",
    "it",
    "may",
    "might",
    "must",
    "new",
    "of",
    "on",
    "or",
    "should",
    "that",
    "the",
    "their",
    "this",
    "to",
    "was",
    "were",
    "will",
    "would",
    "was",
    "with",
}
LINK_LABEL_PATTERN = re.compile(
    r"^(full report|blog post|fact sheet|press release|webcast|backgrounder|executive summary|executive report|references|infographic|agenda|presentation|annex reports|expert contributors?|working group members|white house report|request for information responses|forensics addendum)(?:\s*\(pdf\))?$",
    re.IGNORECASE,
)
STANDALONE_ENUMERATOR_PATTERN = re.compile(
    r"^\(?[a-zA-Z0-9ivxlcdmIVXLCDM]{1,6}\)?\.?$|^[A-Z]\)$|^\([A-Z]\)$|^\([ivxlcdm]+\)$|^\([0-9]+\)$"
)
SPLIT_TOKEN_REPAIRS = {
    "Governm ent": "Government",
    "governm ent": "government",
    "cont racting": "contracting",
    "Cont racting": "Contracting",
    "servi ces": "services",
    "Servi ces": "Services",
    "recove ry": "recovery",
    "Recove ry": "Recovery",
    "Powe rs": "Powers",
    "powe rs": "powers",
    "consti tuted": "constituted",
    "Consti tuted": "Constituted",
    "cons tituted": "constituted",
    "Cons tituted": "Constituted",
    "intellect ual": "intellectual",
    "Intellect ual": "Intellectual",
    "opport unity": "opportunity",
    "Opport unity": "Opportunity",
    "cust omer": "customer",
    "Cust omer": "Customer",
    "memor andum": "memorandum",
    "Memor andum": "Memorandum",
    "establ ished": "established",
    "Establ ished": "Established",
    "docum ent": "document",
    "Docum ent": "Document",
    "submis sion": "submission",
    "Submis sion": "Submission",
    "ac counts": "accounts",
    "Ac counts": "Accounts",
    "rev iew": "review",
    "Rev iew": "Review",
    "pate nt": "patent",
    "Pate nt": "Patent",
    "exc ess": "excess",
    "Exc ess": "Excess",
    "Proposedrule": "Proposed rule",
    "proposedrule": "proposed rule",
    "Finalrule": "Final rule",
    "finalrule": "final rule",
    "Thisproposed": "This proposed",
    "thisproposed": "this proposed",
}
TOKEN_SPLIT_PREFIXES = {
    "ac",
    "ad",
    "ag",
    "al",
    "ap",
    "as",
    "at",
    "co",
    "com",
    "con",
    "cons",
    "de",
    "dis",
    "ex",
    "im",
    "in",
    "inter",
    "micro",
    "mis",
    "non",
    "over",
    "pre",
    "pro",
    "re",
    "sub",
    "super",
    "trans",
    "un",
}


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
    doc_type: str
    recommended_for_primary_policy_corpus: bool
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
class CleanedText:
    text: str
    doc_type: str
    recommended_for_primary_policy_corpus: bool


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
        choices=("generic", "academic", "policy", "policy_strict", "light"),
        default="generic",
        help="Cleaning strength. Default: generic.",
    )
    parser.add_argument(
        "--keep-tables",
        action="store_true",
        help="Keep table-like blocks in cleaned text. By default, likely tables are removed from Federal Register running-text output.",
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


def classify_doc(name: str, text: str) -> str:
    head = text[:4000]
    lower_head = head.lower()
    if "search results | nist" in lower_head or "1 of 1000 pages" in lower_head:
        return "nist_search_results"
    if "pcast documents & reports | the white house" in lower_head:
        return "whitehouse_index"
    if "text/xml" in lower_head and ("u.s. senate" in lower_head or "congress" in lower_head):
        return "congress_xml_text"
    if "federal register" in lower_head or "fr doc." in text.lower() or "verdate" in lower_head:
        return "federal_register_pdf_text"
    lower_name = name.lower()
    if "nist" in lower_name and ("search" in lower_head or "last modified" in lower_head):
        return "nist_search_results"
    return "generic_official_text"


def recommended_for_primary_policy_corpus(doc_type: str) -> bool:
    return doc_type not in {"nist_search_results", "whitehouse_index"}


def build_raw_vocab(paths: Sequence[Path]) -> Counter[str]:
    vocab: Counter[str] = Counter()
    for path in paths:
        if path.suffix.lower() not in TXT_EXTENSIONS:
            continue
        try:
            text, _, _ = decode_to_text(path.read_bytes(), path)
        except Exception:
            continue
        text = normalize_unicode_text(text)
        for word in re.findall(r"\b[A-Za-z][A-Za-z'-]{1,}\b", text):
            vocab[word.lower()] += 1
    return vocab


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


def clean_text(
    text: str,
    profile: str,
    source_name: str = "",
    vocab: Counter[str] | None = None,
    keep_tables: bool = False,
) -> CleanedText:
    raw = normalize_unicode_text(text)
    doc_type = classify_doc(source_name, raw)
    primary_recommendation = recommended_for_primary_policy_corpus(doc_type)

    if profile == "light":
        lightly_cleaned = re.sub(r"([^\W\d_])-\n([^\W\d_])", r"\1\2", raw, flags=re.UNICODE)
        lightly_cleaned = re.sub(r"[ \t]+", " ", lightly_cleaned)
        lightly_cleaned = re.sub(r"\n{3,}", "\n\n", lightly_cleaned).strip()
        return CleanedText(lightly_cleaned, doc_type, primary_recommendation)

    lines = normalize_extracted_lines(raw.splitlines(), profile, doc_type)
    lines = remove_contextual_noise_lines(lines, profile, doc_type)
    lines = remove_signature_blocks(lines, doc_type)

    if doc_type == "congress_xml_text":
        lines = remove_toc_blocks_congress(lines)
        lines = remove_standalone_enumerators(lines)

    if not keep_tables and doc_type == "federal_register_pdf_text":
        lines = remove_table_blocks(lines)

    if doc_type != "congress_xml_text":
        lines = drop_document_noise(lines, strict=profile == "policy_strict")
    lines = repair_broken_word_wraps(lines)
    lines = drop_repeated_noisy_lines(lines)

    cleaned_text = join_wrapped_lines(lines, doc_type)
    cleaned_text = repair_spacing_punct(cleaned_text)
    cleaned_text = repair_split_tokens(cleaned_text, vocab or Counter())
    cleaned_text = repair_spacing_punct(cleaned_text)
    cleaned_text = paragraph_filter(cleaned_text, doc_type)
    cleaned_text = "\n\n".join(
        re.sub(r"[ \t]+", " ", paragraph).strip()
        for paragraph in cleaned_text.split("\n\n")
        if paragraph.strip()
    )
    return CleanedText(cleaned_text.strip(), doc_type, primary_recommendation)


def normalize_extracted_lines(lines: Sequence[str], profile: str, doc_type: str) -> list[str]:
    policy_like = profile in {"policy", "policy_strict"}
    policy_strict = profile == "policy_strict"
    normalized: list[str] = []
    in_references_tail = False
    drop_following_last_modified_date = False
    for line in lines:
        cleaned = re.sub(r"[ \t]+", " ", line).strip()
        if not cleaned:
            if normalized and normalized[-1] != "":
                normalized.append("")
            continue

        if policy_like:
            cleaned = strip_inline_boilerplate(cleaned)
            cleaned = re.sub(r"\s{2,}", " ", cleaned).strip(" |:-")
            if not cleaned:
                continue

        lowered = cleaned.lower()
        if policy_like and lowered in {"last modified", "last modified:"}:
            drop_following_last_modified_date = True
            continue
        if policy_like and drop_following_last_modified_date and MONTH_DATE_LINE_PATTERN.fullmatch(cleaned):
            drop_following_last_modified_date = False
            continue
        drop_following_last_modified_date = False

        if re.fullmatch(r"page\s+\d+(\s+of\s+\d+)?", lowered):
            continue
        if re.fullmatch(r"\d+", cleaned):
            continue
        if policy_strict and re.fullmatch(r"[^\w\s]{1,3}", cleaned):
            continue
        if lowered == "this page is intentionally left blank.":
            continue
        if should_drop_boilerplate_line(cleaned, lowered):
            continue
        if policy_strict and looks_like_navigation_scrape(cleaned):
            continue
        if profile in {"academic", "policy", "policy_strict"} and lowered in {"references", "bibliography", "works cited"}:
            in_references_tail = True
            continue
        if in_references_tail:
            continue
        if doc_type in {"nist_search_results", "whitehouse_index"}:
            cleaned = URL_PATTERN.sub("", cleaned).strip()
        elif re.fullmatch(r"https?://\S+|www\.\S+", cleaned, flags=re.IGNORECASE):
            continue
        if cleaned:
            normalized.append(cleaned)
    return normalized


def remove_contextual_noise_lines(lines: list[str], profile: str, doc_type: str) -> list[str]:
    out: list[str] = []
    skip_next_date = False
    for line in lines:
        s = line.strip()
        if not s:
            continue

        if doc_type in {"nist_search_results", "whitehouse_index"}:
            s = re.sub(r"\s*Was this page helpful\??.*$", "", s, flags=re.IGNORECASE).strip()
            s = re.sub(r"\s+Registration Contact\b.*$", "", s, flags=re.IGNORECASE).strip()
            s = re.sub(r"\b[\w.+-]+@[\w.-]+\.\w+\b", "", s)
            s = re.sub(r"\(\d{3}\)\s*\d{3}[- ]\d{4}", "", s)
            if doc_type == "whitehouse_index":
                s = s.replace("PCAST Documents & Reports | The White House", "PCAST Documents & Reports")
                s = re.sub(r"\s*Skip to main content.*$", "", s, flags=re.IGNORECASE).strip()
                s = re.sub(r"^Home\s+•.*$", "", s).strip()
            if doc_type == "nist_search_results":
                s = re.sub(r"\s+Quick Links\s+Download.*?Overview of the AI RMF\s+", " ", s)

        if doc_type == "nist_search_results" and re.match(r"^Last modified:$", s, re.IGNORECASE):
            skip_next_date = True
            continue
        if skip_next_date:
            if MONTH_DATE_LINE_PATTERN.fullmatch(s):
                skip_next_date = False
                continue
            skip_next_date = False

        if is_source_noise_line(s, doc_type):
            continue
        if doc_type == "whitehouse_index":
            if LINK_LABEL_PATTERN.match(s):
                continue
            if re.match(r"^[A-Z][a-z]+ \d{1,2}, \d{4} (?:b\s*log post|blog post|press release|fact sheet)(?: .*)?$", s, re.IGNORECASE):
                continue
        out.append(s)
    return out


def is_source_noise_line(line: str, doc_type: str) -> bool:
    s = line.strip()
    lowered = s.lower()
    if not s:
        return True
    if any(pattern.search(s) for pattern in FEDERAL_REGISTER_CONTROL_PATTERNS):
        return True
    if re.match(r"^https?://\S+$", s):
        return True
    if doc_type in {"nist_search_results", "whitehouse_index"}:
        if lowered in NAV_SINGLE_LINES or lowered in {"search results", "search nist", "official websites use .gov"}:
            return True
        if "use \"\" to search for an exact phrase" in lowered:
            return True
        if "pages of" in lowered and "ordered by" in lowered:
            return True
        if "means you've safely connected" in lowered:
            return True
        if "share sensitive information only" in lowered:
            return True
        if LINK_LABEL_PATTERN.match(s):
            return True
    if doc_type == "congress_xml_text":
        if s in {"U.S. Senate", "text/xml", "EN"}:
            return True
        if s.startswith("Pursuant to Title 17 Section 105"):
            return True
    return False


def remove_signature_blocks(lines: list[str], doc_type: str) -> list[str]:
    if doc_type != "federal_register_pdf_text":
        return lines
    out = list(lines)
    for marker in ("THE WHITE HOUSE,", "The White House,", "Andrei Iancu,"):
        indexes = [idx for idx, line in enumerate(out) if line.strip().startswith(marker)]
        if indexes and len(out) - indexes[-1] <= 25:
            return out[: indexes[-1]]
    return out


def remove_toc_blocks_congress(lines: list[str]) -> list[str]:
    out: list[str] = []
    toc_mode = False
    for line in lines:
        s = line.strip()
        if not s:
            continue
        if toc_mode:
            if is_congress_toc_entry(s):
                continue
            toc_mode = False
        if re.search(r"\btable of contents\b", s, re.IGNORECASE) and re.search(r"\bfollows\b", s, re.IGNORECASE):
            before = re.split(r"(?i)(?:\([a-z]\)\s*)?The table of contents\b.*?\bfollows:?", s)[0].strip()
            if before and len(re.findall(r"[A-Za-z]+", before)) >= 4 and "table of contents" not in before.lower():
                out.append(before)
            toc_mode = True
            continue
        if re.match(r"^Sec\.\s+\d+[A-Za-z]?\.", s):
            continue
        out.append(s)
    return out


def is_congress_toc_entry(line: str) -> bool:
    if re.match(r"^Sec\.\s+\d+[A-Za-z]?\.", line):
        return True
    if re.match(r"^(DIVISION|TITLE|Subtitle|PART|Subpart|CHAPTER)\s+[A-Z0-9IVXLC-]+", line):
        return True
    if re.match(r"^\d+[A-Z]?\.$", line):
        return False
    return False


def remove_standalone_enumerators(lines: list[str]) -> list[str]:
    return [line for line in lines if not STANDALONE_ENUMERATOR_PATTERN.match(line.strip())]


def remove_table_blocks(lines: list[str]) -> list[str]:
    out: list[str] = []
    in_table = False
    for line in lines:
        s = line.strip()
        if re.match(r"^TABLE\s+\d+[A-Z]?\s*[-—]", s, re.IGNORECASE):
            in_table = True
            continue
        if in_table:
            if re.match(r"^\([a-z]\)\s+[A-Z]", s) or re.match(
                r"^(In|The|This|Applicants|Overall|Consistent|During|For)\b", s
            ):
                in_table = False
                out.append(s)
            continue

        numeric_chars = sum(ch.isdigit() or ch in "$%+[]" for ch in s)
        alpha_words = re.findall(r"[A-Za-z]{2,}", s)
        if numeric_chars > 8 and len(alpha_words) < 8:
            continue
        if re.match(
            r"^(Fee description|Current fees|Final rule|Dollar change|Percentage change|FY \d{4}|entity|large|small|micro)",
            s,
            re.IGNORECASE,
        ):
            continue
        out.append(s)
    return out


def is_heading_like(line: str) -> bool:
    s = line.strip()
    if not s:
        return False
    if re.match(r"^(Section|Sec\.)\s+\d+", s):
        return True
    if re.match(r"^[IVXLC]+\.\s+[A-Z]", s):
        return True
    if re.match(r"^[A-Z][A-Za-z0-9 ,;:&'()/-]{2,90}$", s) and not re.search(r"[.!?;:]$", s):
        words = s.split()
        if words and sum(1 for word in words if word[:1].isupper() or word.isupper()) / len(words) > 0.45:
            return len(words) <= 12
    return False


def join_wrapped_lines(lines: list[str], doc_type: str) -> str:
    if doc_type in {"nist_search_results", "whitehouse_index"}:
        return "\n\n".join(line for line in lines if line.strip())

    paragraphs: list[str] = []
    buffer: list[str] = []

    def flush() -> None:
        nonlocal buffer
        if buffer:
            paragraphs.append(" ".join(buffer))
            buffer = []

    for line in lines:
        s = line.strip()
        if not s:
            flush()
            continue
        if is_heading_like(s):
            flush()
            paragraphs.append(s)
            continue
        if re.match(r"^(Section|Sec\.)\s+\d+", s):
            flush()
            buffer = [s]
            continue
        buffer.append(s)
        if re.search(r"[.!?;:]$", s) and len(" ".join(buffer)) > 120:
            flush()

    flush()
    return "\n\n".join(paragraphs)


def repair_spacing_punct(text: str) -> str:
    text = re.sub(r"\s+([,.;:!?])", r"\1", text)
    text = re.sub(r"\(\s+", "(", text)
    text = re.sub(r"\s+\)", ")", text)
    text = re.sub(r"\[\s+", "[", text)
    text = re.sub(r"\s+\]", "]", text)
    text = re.sub(r"\bU\.\s*S\.\s*C\.", "U.S.C.", text)
    text = re.sub(r"\bC\.\s*F\.\s*R\.", "C.F.R.", text)
    text = re.sub(r"\bU\.\s*S\.", "U.S.", text)
    text = re.sub(r"https?://\s*\S*", "", text)
    text = re.sub(r"www\.\s*\S*", "", text)
    text = re.sub(r"([A-Za-z]+)-\s+(\d+)", r"\1-\2", text)
    text = re.sub(r"\b([A-Za-z]{2,})-\s+([A-Za-z]{2,})\b", r"\1-\2", text)
    text = re.sub(r"\s{2,}", " ", text)
    text = re.sub(r"\n\s+", "\n", text)
    text = re.sub(r"([.;:!?])([A-Z])", r"\1 \2", text)
    return text.strip()


def repair_split_tokens(text: str, vocab: Counter[str]) -> str:
    for bad, good in SPLIT_TOKEN_REPAIRS.items():
        text = re.sub(r"\b" + re.escape(bad) + r"\b", good, text)

    def merge_if_attested(match: re.Match[str]) -> str:
        left, right = match.group(1), match.group(2)
        combo = (left + right).lower()
        if len(right) > 3 and left.lower() not in TOKEN_SPLIT_PREFIXES:
            return match.group(0)
        if left.lower() in COMMON_FUNCTION_WORDS or right.lower() in COMMON_FUNCTION_WORDS:
            return match.group(0)
        if vocab.get(combo, 0) >= 2 and len(combo) >= 7:
            return left + right
        return match.group(0)

    text = re.sub(r"\b([A-Za-z]{4,12})\s+([a-z]{2,8})\b", merge_if_attested, text)

    def merge_prefix(match: re.Match[str]) -> str:
        left, right = match.group(1), match.group(2)
        combo = (left + right).lower()
        if left.lower() in TOKEN_SPLIT_PREFIXES and vocab.get(combo, 0) >= 2 and len(combo) >= 6:
            return (left + right).capitalize() if left[:1].isupper() else left + right
        return match.group(0)

    return re.sub(r"\b([A-Za-z]{2,8})\s+([a-z]{2,12})\b", merge_prefix, text)


def paragraph_filter(text: str, doc_type: str) -> str:
    paragraphs = [paragraph.strip() for paragraph in re.split(r"\n{2,}", text) if paragraph.strip()]
    kept: list[str] = []
    seen: set[str] = set()
    for paragraph in paragraphs:
        lowered = paragraph.lower()
        if any(
            phrase in lowered
            for phrase in (
                "official websites use .gov",
                "secure .gov websites use https",
                "pursuant to title 17 section 105",
                "this file is not subject to copyright protection",
                "skip to main content",
                "skip to footer",
            )
        ):
            continue
        if doc_type == "whitehouse_index" and LINK_LABEL_PATTERN.match(paragraph):
            continue
        if paragraph.lower() in NAV_SINGLE_LINES:
            continue
        words = re.findall(r"[A-Za-z]+", paragraph)
        if len(words) < 3 and not is_heading_like(paragraph):
            continue
        key = re.sub(r"\s+", " ", lowered)
        if key in seen:
            continue
        seen.add(key)
        kept.append(paragraph)
    return "\n\n".join(kept)


def should_drop_boilerplate_line(cleaned: str, lowered: str) -> bool:
    if any(lowered.startswith(prefix) for prefix in BOILERPLATE_PREFIXES):
        return True

    if lowered in NAV_SINGLE_LINES:
        return True

    if lowered in {"a lock (", "lock", "a locked padlock", "search results"}:
        return True

    if SOCIAL_SHARE_PATTERN.search(lowered):
        return True

    if lowered.startswith("search results") and "nist" in lowered:
        return True

    if "skip to footer" in lowered:
        return True

    # Typical scraped-site utility line.
    if "all visitors to the nist campus must" in lowered:
        return True

    if "</gph>" in lowered:
        return True

    if re.fullmatch(r"\d+\s+of\s+\d+\s+pages?\s+of\s+\d+\s+results,\s*ordered by relevance\s*\.?", lowered):
        return True

    if re.fullmatch(r"last modified:\s*", lowered):
        return True
    if lowered == "last modified":
        return True

    return False


def drop_repeated_noisy_lines(lines: list[str]) -> list[str]:
    counts = Counter(line.lower() for line in lines if line)
    filtered: list[str] = []
    for line in lines:
        if not line:
            filtered.append(line)
            continue
        lowered = line.lower()
        if counts.get(lowered, 0) >= 3 and (
            "share to " in lowered
            or "project links" in lowered
            or "search results |" in lowered
            or "official websites use .gov" in lowered
        ):
            continue
        filtered.append(line)
    return filtered


def drop_document_noise(lines: list[str], strict: bool) -> list[str]:
    if not lines:
        return lines

    counts = Counter(line.lower() for line in lines if line)
    signature_counts = Counter(line_signature_for_repetition(line) for line in lines if line)
    kept: list[str] = []
    content_started = False
    for idx, line in enumerate(lines):
        if not line:
            kept.append(line)
            continue
        lowered = line.lower()
        if should_drop_front_matter_line(line, lowered, idx, content_started):
            continue
        if is_repeated_template_line(line, lowered, counts, signature_counts, strict):
            continue
        if is_layout_artifact_line(line, lowered, strict):
            continue
        kept.append(line)
        if is_content_like_line(line):
            content_started = True
    return kept


def should_drop_front_matter_line(line: str, lowered: str, idx: int, content_started: bool) -> bool:
    if idx < 80:
        if MIME_LINE_PATTERN.fullmatch(line):
            return True
        if re.fullmatch(r"[A-Z]{2,3}", line):
            return True
        if "copyright" in lowered or "public domain" in lowered:
            return True
    if not content_started and idx < 40:
        if re.fullmatch(r"[A-Z0-9 .:-]{8,}", line):
            return True
    return False


def is_content_like_line(line: str) -> bool:
    alpha = sum(1 for ch in line if ch.isalpha())
    words = re.findall(r"[A-Za-z]{3,}", line)
    return alpha >= 25 and len(words) >= 5


def line_stats(line: str) -> tuple[int, int, int, int]:
    alpha = sum(1 for ch in line if ch.isalpha())
    digits = sum(1 for ch in line if ch.isdigit())
    punct = sum(1 for ch in line if (not ch.isalnum() and not ch.isspace()))
    uppers = sum(1 for ch in line if ch.isupper())
    return alpha, digits, punct, uppers


def line_signature_for_repetition(line: str) -> str:
    normalized = re.sub(r"\d+", "#", line.lower())
    normalized = re.sub(r"\s+", " ", normalized).strip()
    return normalized


def is_repeated_template_line(
    line: str, lowered: str, counts: Counter[str], signature_counts: Counter[str], strict: bool
) -> bool:
    repeat = counts.get(lowered, 0)
    signature_repeat = signature_counts.get(line_signature_for_repetition(line), 0)
    if repeat < 2:
        if signature_repeat < 2:
            return False
    alpha, digits, punct, uppers = line_stats(line)
    length = len(line)
    if PATH_LIKE_PATTERN.search(line):
        return True
    if digits >= 4 and punct >= 6 and (repeat >= 2 or signature_repeat >= 2):
        return True
    if (repeat >= 3 or signature_repeat >= 3) and alpha < 45 and digits >= 2:
        return True
    if strict and (repeat >= 2 or signature_repeat >= 2) and length < 180 and uppers >= 10 and digits >= 2:
        return True
    return False


def is_layout_artifact_line(line: str, lowered: str, strict: bool) -> bool:
    alpha, digits, punct, uppers = line_stats(line)
    length = max(1, len(line))
    punct_ratio = punct / length
    upper_ratio = uppers / max(1, alpha)
    if PATH_LIKE_PATTERN.search(line):
        return True
    if "</" in line and ">" in line:
        return True
    if re.search(r"\b(verdate|jkt|frm|fmt|sfmt|fr doc|billing code)\b", lowered):
        return True
    if re.match(r"^\d{4,}\s+.+\bvol\.\b.+/.+", lowered):
        return True
    if re.search(r"\bon\s+[A-Z0-9]{8,}\b", line) and " with " in lowered:
        return True
    if digits >= 6 and punct_ratio > 0.15 and alpha < 70:
        return True
    if strict and upper_ratio > 0.45 and digits >= 3 and punct >= 5:
        return True
    return False


def repair_broken_word_wraps(lines: list[str]) -> list[str]:
    repaired = list(lines)
    idx = 0
    while idx < len(repaired) - 1:
        current = repaired[idx]
        nxt = repaired[idx + 1]
        if not current or not nxt:
            idx += 1
            continue
        if current.endswith((".", "?", "!", ":", ";", ")", "]")):
            idx += 1
            continue
        tail_match = FRAGMENT_TAIL_PATTERN.search(current)
        lead_match = FRAGMENT_LEAD_PATTERN.match(nxt)
        lead_long_match = FRAGMENT_LEAD_LONG_PATTERN.match(nxt)
        if not tail_match or not lead_match:
            if not tail_match or not lead_long_match:
                idx += 1
                continue
        tail = tail_match.group(1)
        if tail.lower() in COMMON_FUNCTION_WORDS:
            idx += 1
            continue
        lead = lead_match.group(0) if lead_match else lead_long_match.group(0)
        if lead.lower() in COMMON_FUNCTION_WORDS:
            idx += 1
            continue
        if len(tail) >= 3 and len(lead) <= 3 and (len(tail) + len(lead)) >= 6:
            repaired[idx] = current[: -len(tail)] + tail + lead + nxt[len(lead) :]
            repaired[idx + 1] = ""
        idx += 1
    return repaired


def strip_inline_boilerplate(text: str) -> str:
    updated = text
    for pattern in INLINE_BOILERPLATE_PATTERNS:
        updated = pattern.sub(" ", updated)
    return updated.strip()


def looks_like_navigation_scrape(text: str) -> bool:
    lowered = text.lower()
    separators = text.count("|") + text.count("•") + text.count(">")
    hint_hits = sum(1 for token in NAV_SCRAPE_HINTS if token in lowered)
    if separators >= 3 and hint_hits >= 3:
        return True
    if hint_hits >= 6 and "." not in text and ":" not in text:
        return True
    return False


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
    vocab: Counter[str],
    keep_tables: bool,
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

        cleaned_result = clean_text(extraction.text, profile, source_name=path.name, vocab=vocab, keep_tables=keep_tables)
        cleaned = cleaned_result.text
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
                doc_type=cleaned_result.doc_type,
                recommended_for_primary_policy_corpus=cleaned_result.recommended_for_primary_policy_corpus,
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
        "keep_tables": args.keep_tables,
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

    if "doc_type_distribution" in report:
        section = "文档类型分布" if zh else "Document Type Distribution"
        lines.extend([f"## {section}", ""])
        for key, value in report["doc_type_distribution"].items():
            lines.append(f"- `{key}`: {value}")
        primary_label = "建议纳入主政策语料的文件数" if zh else "Recommended primary policy files"
        lines.extend(["", f"- **{primary_label}**: {report.get('primary_policy_recommended_files', 0)}", ""])

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

    raw_vocab = build_raw_vocab([path for path, _, fmt in corpus_jobs if fmt == "txt"])

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
                raw_vocab,
                args.keep_tables,
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
    doc_type_distribution = Counter(record.doc_type for record in process_records)
    primary_recommended_count = sum(1 for record in process_records if record.recommended_for_primary_policy_corpus)

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
        "keep_tables": args.keep_tables,
        "doc_type_distribution": dict(sorted(doc_type_distribution.items())),
        "primary_policy_recommended_files": primary_recommended_count,
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
