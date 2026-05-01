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

For government, institutional, or policy reports. Currently follows the same conservative behavior as `academic`, without hard-coded agency names or project-specific phrases.

## Project-Specific Rules

Do not add project-specific deletion patterns directly into the default cleaning code. If a corpus needs special handling, add a named profile or a config-driven rule and document:

- the exact pattern
- why it is safe for that corpus
- a before/after test case
