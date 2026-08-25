"""Tests for the synergy+ dataset.
"""

import pytest

from synergy_dataset import WORK_EXTRACTORS
from synergy_dataset import Dataset
from synergy_dataset import iter_datasets
from synergy_dataset.base import download_raw_subset

DATASETS = ["Chou_2003", "Oud_2018"]

# Used for single-dataset tests
SINGLE_TEST_DATASET = "Oud_2018"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def enable_synergy_plus():
    """Switch the active dataset to synergy+ for the entire module."""
    import synergy_dataset.base as _base

    original = _base.SYNERGY_SET
    _base.SYNERGY_SET = _base.SYNERGY_PLUS
    yield
    _base.SYNERGY_SET = original


@pytest.fixture(scope="module")
def real_root(tmp_path_factory, enable_synergy_plus):
    """Download DATASETS once for the whole module and return the root dir
    iter_datasets(path=...) expects (i.e. the parent of synergy-dataset-plus).
    """
    root = tmp_path_factory.mktemp("synergy_plus_real")
    for name in DATASETS:
        download_raw_subset(name, path=root)
    return root


@pytest.fixture(scope="module", params=DATASETS)
def dataset(request, real_root):
    path = real_root / "synergy-dataset-plus" / request.param
    return Dataset(request.param, path=path)


@pytest.fixture(scope="module")
def single_test_dataset(real_root):
    path = real_root / "synergy-dataset-plus" / SINGLE_TEST_DATASET
    return Dataset(SINGLE_TEST_DATASET, path=path)


# ---------------------------------------------------------------------------
# iter_datasets
# ---------------------------------------------------------------------------


def test_iter_datasets_returns_multiple(real_root):
    # min_inclusions=None: Chou_2003 only has 1 qualifying inclusion, below
    # the default MIN_INCLUSIONS of 3, so it would otherwise be filtered out
    assert len(list(iter_datasets(path=real_root, min_inclusions=None))) > 1


def test_iter_datasets_yields_dataset_instances(real_root):
    for d in iter_datasets(path=real_root, min_inclusions=None):
        assert isinstance(d, Dataset)
        break


def test_iter_datasets_split_invalid_raises():
    with pytest.raises(ValueError, match="split must be"):
        next(iter_datasets(split="val"))


def test_iter_datasets_train_test_are_disjoint(real_root):
    from synergy_dataset.splits import TEST_SPLIT

    if not TEST_SPLIT:
        pytest.skip("TEST_SPLIT not yet populated")
    train = {d.name for d in iter_datasets(path=real_root, split="train")}
    test = {d.name for d in iter_datasets(path=real_root, split="test")}
    assert train.isdisjoint(test)


def test_iter_datasets_train_test_cover_all(real_root):
    from synergy_dataset.splits import TEST_SPLIT

    if not TEST_SPLIT:
        pytest.skip("TEST_SPLIT not yet populated")
    all_names = {d.name for d in iter_datasets(path=real_root)}
    train = {d.name for d in iter_datasets(path=real_root, split="train")}
    test = {d.name for d in iter_datasets(path=real_root, split="test")}
    assert train | test == all_names


# ---------------------------------------------------------------------------
# labels
# ---------------------------------------------------------------------------


def test_labels_is_dict(dataset):
    assert isinstance(dataset.labels, dict)


def test_labels_has_entries(dataset):
    assert len(dataset.labels) > 0


def test_labels_entry_structure(dataset):
    entry = next(iter(dataset.labels.values()))
    assert "label_included" in entry
    assert "label_abstract_included" in entry
    assert "doi" in entry
    assert "pmid" in entry
    assert "lens_id" in entry


def test_labels_label_included_is_binary(dataset):
    for entry in dataset.labels.values():
        assert entry["label_included"] in (0, 1)


def test_labels_has_inclusions(dataset):
    assert any(v["label_included"] == 1 for v in dataset.labels.values())


def test_labels_has_exclusions(dataset):
    assert any(v["label_included"] == 0 for v in dataset.labels.values())


def test_labels_abstract_label_is_int_or_none(dataset):
    for entry in dataset.labels.values():
        val = entry["label_abstract_included"]
        assert val is None or val in (0, 1)


# ---------------------------------------------------------------------------
# iter()
# ---------------------------------------------------------------------------


def test_iter_yields_pyalex_work(single_test_dataset):
    from pyalex import Work

    work, _ = next(single_test_dataset.iter())
    assert isinstance(work, Work)


def test_iter_label_is_int(single_test_dataset):
    _, label = next(single_test_dataset.iter())
    assert isinstance(label, int)
    assert label in (0, 1)


def test_iter_count_matches_counts(dataset):
    """For synergy+, iter() only yields the open-access/valid-abstract
    subset."""
    n_iter = sum(1 for _ in dataset.iter())
    assert n_iter == dataset.counts[0]


def test_iter_only_yields_open_access_valid_abstract(dataset):
    from synergy_dataset.base import _is_valid_abstract

    for work, _ in dataset.iter():
        open_access = work.get("open_access") or {}
        assert open_access.get("is_oa")
        assert _is_valid_abstract(work.get("abstract_inverted_index_cleaned"))


# ---------------------------------------------------------------------------
# to_dict()
# ---------------------------------------------------------------------------


def test_to_dict_returns_dict(single_test_dataset):
    assert isinstance(single_test_dataset.to_dict(), dict)


def test_to_dict_keys_match_iter_ids(dataset):
    """For synergy+, to_dict()'s keys are the iter()-filtered subset of
    labels, not every label."""
    result = dataset.to_dict()
    iter_ids = {w["id"].lower() for w, _ in dataset.iter()}
    assert set(result.keys()) == iter_ids


def test_to_dict_default_columns(single_test_dataset):
    record = next(iter(single_test_dataset.to_dict().values()))
    for col in ("doi", "lens_id", "title", "abstract", "label_included"):
        assert col in record


def test_to_dict_no_extra_columns_by_default(single_test_dataset):
    record = next(iter(single_test_dataset.to_dict().values()))
    expected = {"doi", "lens_id", "title", "abstract", "label_included"}
    assert set(record.keys()) - {"label_abstract_included"} == expected


def test_to_dict_custom_vars(single_test_dataset):
    result = single_test_dataset.to_dict(vars=["cited_by_count", "type"])
    record = next(iter(result.values()))
    assert "cited_by_count" in record
    assert "type" in record
    assert "title" not in record
    assert "abstract" not in record


def test_to_dict_custom_vars_still_has_base_fields(single_test_dataset):
    result = single_test_dataset.to_dict(vars=["cited_by_count"])
    record = next(iter(result.values()))
    assert "doi" in record
    assert "label_included" in record


def test_to_dict_extended_has_all_extractors(single_test_dataset):
    result = single_test_dataset.to_dict(vars="extended")
    record = next(iter(result.values()))
    for field in WORK_EXTRACTORS:
        assert field in record


def test_to_dict_unknown_var_raises(single_test_dataset):
    with pytest.raises(ValueError, match="Unknown vars"):
        single_test_dataset.to_dict(vars=["nonexistent_field"])


def test_to_dict_title_no_newlines(single_test_dataset):
    for record in single_test_dataset.to_dict().values():
        if record and record.get("title"):
            assert "\n" not in record["title"]
            assert "\r" not in record["title"]


def test_to_dict_abstract_no_newlines(single_test_dataset):
    for record in single_test_dataset.to_dict().values():
        if record and record.get("abstract"):
            assert "\n" not in record["abstract"]
            assert "\r" not in record["abstract"]


def test_to_dict_label_included_is_binary(dataset):
    for record in dataset.to_dict().values():
        if record:
            assert record["label_included"] in (0, 1)


def test_to_dict_author_names_is_list(single_test_dataset):
    result = single_test_dataset.to_dict(vars=["author_names"])
    for record in result.values():
        if record and record["author_names"] is not None:
            assert isinstance(record["author_names"], list)
            break


def test_to_dict_topics_is_list(single_test_dataset):
    result = single_test_dataset.to_dict(vars=["topics"])
    for record in result.values():
        if record and record["topics"] is not None:
            assert isinstance(record["topics"], list)
            break


# ---------------------------------------------------------------------------
# to_frame()
# ---------------------------------------------------------------------------


def test_to_frame_returns_dataframe(single_test_dataset):
    pd = pytest.importorskip("pandas")
    df = single_test_dataset.to_frame()
    assert isinstance(df, pd.DataFrame)


def test_to_frame_has_openalex_id_column(single_test_dataset):
    pytest.importorskip("pandas")
    df = single_test_dataset.to_frame()
    assert "openalex_id" in df.columns


def test_to_frame_default_columns(single_test_dataset):
    pytest.importorskip("pandas")
    df = single_test_dataset.to_frame()
    for col in ("doi", "lens_id", "title", "abstract", "label_included"):
        assert col in df.columns


def test_to_frame_shape_matches_counts(dataset):
    pytest.importorskip("pandas")
    df = dataset.to_frame()
    assert len(df) == dataset.counts[0]


def test_to_frame_extended_has_all_extractor_columns(single_test_dataset):
    pytest.importorskip("pandas")
    df = single_test_dataset.to_frame(vars="extended")
    for field in WORK_EXTRACTORS:
        assert field in df.columns


def test_to_frame_custom_vars(single_test_dataset):
    pytest.importorskip("pandas")
    df = single_test_dataset.to_frame(vars=["cited_by_count", "journal_name"])
    assert "cited_by_count" in df.columns
    assert "journal_name" in df.columns
    assert "title" not in df.columns


# ---------------------------------------------------------------------------
# summary()
# ---------------------------------------------------------------------------


def test_summary_returns_dict(dataset):
    assert isinstance(dataset.summary(), dict)


def test_summary_keys(dataset):
    s = dataset.summary()
    for key in (
        "name",
        "n_total",
        "n_included",
        "n_excluded",
        "inclusion_rate",
        "year_range",
        "languages",
        "primary_topics",
    ):
        assert key in s


def test_summary_name(dataset):
    assert dataset.summary()["name"] == dataset.name


def test_summary_counts(dataset):
    """summary()'s n_total/n_included/n_excluded come straight from
    labels.csv -- unlike iter()/to_dict()/to_frame(), they are not affected
    by the synergy+ open-access/abstract filter."""
    s = dataset.summary()
    assert s["n_total"] == len(dataset.labels)
    assert s["n_included"] + s["n_excluded"] == s["n_total"]


def test_summary_inclusion_rate(dataset):
    s = dataset.summary()
    assert 0.0 <= s["inclusion_rate"] <= 1.0
    if s["n_total"] > 0:
        assert abs(s["inclusion_rate"] - s["n_included"] / s["n_total"]) < 1e-9


def test_summary_year_range(dataset):
    s = dataset.summary()
    if s["year_range"] is not None:
        lo, hi = s["year_range"]
        assert lo <= hi


def test_summary_primary_topics_is_list(dataset):
    assert isinstance(dataset.summary()["primary_topics"], list)


# ---------------------------------------------------------------------------
# WORK_EXTRACTORS
# ---------------------------------------------------------------------------


def test_all_extractors_run_without_error(single_test_dataset):
    work, _ = next(single_test_dataset.iter())
    for name, extractor in WORK_EXTRACTORS.items():
        try:
            extractor(work)
        except Exception as exc:
            pytest.fail(f"Extractor {name!r} raised {exc!r}")


def test_all_extractors_usable_as_vars(single_test_dataset):
    for name in WORK_EXTRACTORS:
        single_test_dataset.to_dict(vars=[name])


# ---------------------------------------------------------------------------
# Download single dataset
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dataset_name", DATASETS)
def test_download_single_dataset(dataset_name, tmpdir):
    download_raw_subset(dataset_name, path=tmpdir)

    # min_inclusions=None: Chou_2003 doesn't clear the default
    # MIN_INCLUSIONS threshold on its own (see module docstring), which
    # isn't what this test is about -- it's checking that a single
    # downloaded dataset is discoverable and loads correctly.
    datasets = iter_datasets(path=tmpdir, min_inclusions=None)
    d = next(datasets)

    assert isinstance(d, Dataset)
    assert d.name == dataset_name
    assert isinstance(d.labels, dict)

    with pytest.raises(StopIteration):
        next(datasets)
