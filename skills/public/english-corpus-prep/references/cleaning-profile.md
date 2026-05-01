# Cleaning Profiles

The default cleaning rules are conservative and corpus-safe. They should preserve legal UTF-8 English text, including smart quotes after normalization, accented names, scientific symbols, and non-ASCII loanwords. Do not remove all non-ASCII characters.

## Shared Rules

All profiles:

1. Normalize Unicode with `NFKC`.
2. Convert CRLF/CR newlines to `\n`.
3. Remove soft hyphens.
4. Normalize common smart quotes and dashes.
5. Join layout-split hyphenated words across line breaks.
6. Collapse repeated spaces and tabs.
7. Remove obvious page-number lines:
   - `Page 4`
   - `Page 4 of 12`
   - lines containing only digits
8. Remove `This page is intentionally left blank.`
9. Preserve paragraph-level blank lines.

## Profiles

### `light`

Minimal normalization only. Use when line structure, punctuation, or source formatting matters.

### `generic`

Default profile. Applies shared rules and removes URLs. Suitable for most English corpus preparation.

### `academic`

For papers, dissertations, and academic reports. Applies generic rules and removes a trailing `References`, `Bibliography`, or `Works Cited` section when the heading appears on its own line.

### `policy`

For government, institutional, or policy corpora. This profile first classifies broad document types and then applies reusable type-specific cleanup. It does not add one-off deletion rules for a single sample document.

Document types currently used by the profile:

- `federal_register_pdf_text`: removes Federal Register control lines, FR notices, billing codes, PDF extraction paths, and likely table blocks unless `--keep-tables` is enabled.
- `congress_xml_text`: removes XML/front-matter boilerplate and table-of-contents entries while preserving numbered substantive clauses, appropriations, amendments, and fiscal-year lines.
- `whitehouse_index` and `nist_search_results`: removes common navigation/search UI, link labels, contact fragments, and URLs; these are marked as not recommended for the primary policy corpus.
- `generic_official_text`: applies shared normalization and conservative layout cleanup.

### `policy_strict`

For heavily scraped policy corpora with substantial navigation/sidebar residue. Applies the `policy` model plus stronger web/navigation boilerplate filtering.

### `--keep-tables`

This flag is not a profile, but it changes the `policy` profile's Federal Register handling. Use it when table content is analytically important; otherwise the default favors running prose suitable for sentence/token analysis.

## Project-Specific Rules

Do not add project-specific deletion patterns directly into the default cleaning code. If a corpus needs special handling, add a named profile or a config-driven rule and document:

- the exact pattern
- why it is safe for that corpus
- a before/after test case
