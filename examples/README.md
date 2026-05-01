# Examples

The old generated sample output was removed because it used the previous merged-corpus contract and contained machine-specific absolute paths.

Use the test suite for a small reproducible example:

```bash
python3 -m unittest discover -s tests -v
```

To generate a local example manually:

```bash
python3 skills/public/english-corpus-prep/scripts/build_corpus.py ./raw_corpus \
  --output-dir ./corpus_output \
  --recursive \
  --tagger none \
  --relative-paths
```
