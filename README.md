# Python package for the SYNERGY+ and SYNERGY datasets

![PyPI](https://img.shields.io/pypi/v/synergy-dataset)

Python package for the [SYNERGY](https://github.com/asreview/synergy-dataset) and [SYNERGY+](https://github.com/asreview/synergy-dataset) datasets — collections of systematically labelled records for systematic review research.

## Installation

Requires Python 3.10 or later.

```sh
pip install synergy-dataset
```

## Quick Start

```sh
synergy get
```

This downloads and exports the SYNERGY+ dataset as CSV files.

For the full set of CLI options and Python API, see the technical reference below. For an introduction to the SYNERGY project and example notebooks (including a Python getting-started notebook), see the main repository: [asreview/synergy-dataset](https://github.com/asreview/synergy-dataset). Jupyter notebooks can be found in its [`examples`](https://github.com/asreview/synergy-dataset/tree/master/examples) folder.

Prefer working with the dataset in Python? Jump to the [Python API](#python-api).

## Dataset variants

| Variable | Value | Dataset |
|---|---|---|
| `SYNERGY_SET` | `synergy_plus` (default) | SYNERGY+ |
| `SYNERGY_SET` | `synergy` | Original SYNERGY |

Set `SYNERGY_SET=synergy` in your environment to use the original SYNERGY dataset.

---

## Command-line interface

### `synergy list` — list all datasets

```sh
synergy list
```

| Flag | Default | Description |
|---|---|---|
| `--tablefmt FORMAT` | `simple` | Table format (any `tabulate` format) |
| `--n-topics N` | `3` | Number of topics to show per dataset (`-1` for all) |

### `synergy show DATASET` — show dataset details

```sh
synergy show Appenzeller-Herzog_2019
```

### `synergy get` — export datasets to CSV

Exports one CSV per dataset to the output folder, plus a `review_metadata.csv` that combines key fields from each dataset's OpenAlex work object the studies' eligiblity criteria.

```sh
synergy get
```

For SYNERGY+, only open-access works with a valid abstract (≥ 20 words or ≥ 100 characters) are exported. Datasets with fewer than 5 included records are skipped.

| Flag | Default | Description |
|---|---|---|
| `-o, --output PATH` | `synergy_dataset` | Output folder |
| `-v, --vars VARS` | title + abstract | Comma-separated list of extra fields, or `extended` for all OpenAlex fields |
| `-d, --dataset NAME [NAME ...]` | all datasets | One or more dataset names to export |
| `-l, --ignore-legal` | prompt | Skip the abstract plaintext legal prompt |

**Examples**

Export all datasets with default fields:

```sh
synergy get -o ./output
```

Export with extended OpenAlex fields:

```sh
synergy get -v extended
```

Export a single dataset with specific fields:

```sh
synergy get -d Appenzeller-Herzog_2019 -v cited_by_count,publication_year
```

Skip the abstract plaintext legal prompt (e.g. for non-interactive/CI use):

```sh
synergy get --ignore-legal
```

Available `--vars` fields (on top of the always-included `openalex_id`, `doi`, `lens_id`, `title`, `abstract`, `label_included`):

```
publication_year  publication_date  type              language
language_fasttext cited_by_count    referenced_works_count  fwci
is_retracted      is_paratext       is_oa             oa_status
journal_name      author_names      authorships       primary_topic_name
primary_topic_field  primary_topic_domain  topics     keywords
mesh              sustainable_development_goals        indexed_in
referenced_works  related_works     counts_by_year
```

#### Output files

Each run of `synergy get` produces:

- **`{dataset_name}.csv`** — one file per dataset, with one row per work (filtered by the active settings).
- **`review_metadata.csv`** — one row per dataset (≥ 5 inclusions), combining:
  - `key` — dataset identifier (e.g. `Abgaz_2023`)
  - `split` — `train` or `test` (SYNERGY+ only)
  - `data_doi` — DOI of the dataset deposit
  - `n_records` — number of works in the export
  - `n_records_included` — number of included works
  - `eligibility_criteria` — the screening criteria text from `metadata.json` (overwritten by reviews.csv's `Eligibility Criteria`, if available)
  - All fields selected via `--vars` applied to the review publication itself (the OpenAlex work for the systematic review paper)
  - If `reviews.csv` (review-level metadata: screening process, search sizes, review type, etc.) is available at the top level of the dataset repository, all of its columns are merged in, converted to snake_case (e.g. `Paper link` → `paper_link`, `Ti-ab screeners` → `ti_ab_screeners`, `Paper inclusion %` → `paper_inclusion_pct`). If `reviews.csv` can't be found or downloaded, `synergy get` prints a warning and continues without these extra columns.

### `synergy attribute` — attribution for datasets

```sh
synergy attribute
synergy attribute --format markdown
```

---

## Python API

### Iterating over datasets

```python
from synergy_dataset import iter_datasets

for dataset in iter_datasets():
    print(dataset.name)
```

Filter by train/test split (SYNERGY+ only):

```python
for dataset in iter_datasets(split="train"):
    ...

for dataset in iter_datasets(split="test"):
    ...
```

### Working with a single dataset

```python
from synergy_dataset import Dataset

d = Dataset("Appenzeller-Herzog_2019")
```

#### Export to pandas DataFrame
This functionality requires pandas to be installed. Install with pandas with `pip install pandas`.
```python
df = d.to_frame()                         # title + abstract
df = d.to_frame(vars="extended")          # all OpenAlex fields
df = d.to_frame(vars=["cited_by_count"])  # specific fields
```

For SYNERGY+, only open-access works with a valid abstract (≥ 20 words or ≥ 100 characters) are included.

#### Export to dict

```python
records = d.to_dict()              # openalex_id → record dict
records = d.to_dict(vars="extended")
```

#### Iterate over works

```python
for work, label in d.iter():
    print(work["title"], label)
```

#### Dataset metadata and labels

```python
print(d.metadata)     # dict with dataset/publication/collection info
print(d.labels)       # openalex_id → {doi, pmid, lens_id, label_included, ...}
print(d.cite)         # citation string for this dataset
print(d.summary())    # quick statistics
```

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `SYNERGY_SET` | `synergy_plus` | Dataset variant: `synergy_plus` or `synergy` |
| `SYNERGY_VERSION` | `1.0` | Dataset version to download |
| `SYNERGY_PATH` | *(auto)* | Custom path to dataset; `development` for local dev |

---

## License

[MIT](/LICENSE)

## Contact

See [https://github.com/asreview/synergy-dataset](https://github.com/asreview/synergy-dataset) for contact details.
