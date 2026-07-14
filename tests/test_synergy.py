import pytest

from synergy_dataset import WORK_EXTRACTORS
from synergy_dataset import Dataset
from synergy_dataset import iter_datasets
from synergy_dataset.base import _is_valid_abstract
from synergy_dataset.base import download_raw_subset
from synergy_dataset.extractors import _reconstruct_abstract

DATASETS = ["Walker_2018", "van_de_Schoot_2018"]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module", autouse=True)
def enable_synergy():
    """Ensure the active dataset is 'synergy' for the entire module."""
    import synergy_dataset.base as _base

    original = _base.SYNERGY_SET
    _base.SYNERGY_SET = "synergy"
    yield
    _base.SYNERGY_SET = original


@pytest.fixture(scope="module", params=DATASETS)
def dataset(request):
    return Dataset(request.param)


@pytest.fixture(scope="module")
def vds():
    """van_de_Schoot_2018 — used for single-dataset tests."""
    return Dataset("van_de_Schoot_2018")


# ---------------------------------------------------------------------------
# iter_datasets
# ---------------------------------------------------------------------------


def test_iter_datasets_returns_multiple():
    assert len(list(iter_datasets())) > 1


def test_iter_datasets_yields_dataset_instances():
    for d in iter_datasets():
        assert isinstance(d, Dataset)
        break


def test_iter_datasets_split_raises_for_synergy():
    with pytest.raises(ValueError, match="SYNERGY\\+"):
        next(iter_datasets(split="test"))


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


def test_iter_validate_true_yields_workmodel(vds):
    from synergy_dataset.models import WorkModel

    work, _ = next(vds.iter(validate=True))
    assert isinstance(work, WorkModel)


def test_iter_validate_false_yields_pyalex_work(vds):
    from pyalex import Work

    work, _ = next(vds.iter(validate=False))
    assert isinstance(work, Work)


def test_iter_label_is_int(vds):
    _, label = next(vds.iter())
    assert isinstance(label, int)
    assert label in (0, 1)


def test_iter_count_matches_labels(dataset):
    n_iter = sum(1 for _ in dataset.iter())
    assert n_iter == len(dataset.labels)



# ---------------------------------------------------------------------------
# to_dict()
# ---------------------------------------------------------------------------


def test_to_dict_returns_dict(vds):
    assert isinstance(vds.to_dict(), dict)


def test_to_dict_keys_match_labels(dataset):
    result = dataset.to_dict()
    assert list(result.keys()) == list(dataset.labels.keys())


def test_to_dict_default_columns(vds):
    record = next(iter(vds.to_dict().values()))
    for col in ("doi", "pmid", "lens_id", "title", "abstract", "label_included"):
        assert col in record


def test_to_dict_no_extra_columns_by_default(vds):
    record = next(iter(vds.to_dict().values()))
    expected = {"doi", "pmid", "lens_id", "title", "abstract", "label_included"}
    # label_abstract_included is allowed as an optional addition
    assert set(record.keys()) - {"label_abstract_included"} == expected


def test_to_dict_custom_vars(vds):
    result = vds.to_dict(vars=["cited_by_count", "type"])
    record = next(iter(result.values()))
    assert "cited_by_count" in record
    assert "type" in record
    assert "title" not in record
    assert "abstract" not in record


def test_to_dict_custom_vars_still_has_base_fields(vds):
    result = vds.to_dict(vars=["cited_by_count"])
    record = next(iter(result.values()))
    assert "doi" in record
    assert "label_included" in record


def test_to_dict_extended_has_all_extractors(vds):
    result = vds.to_dict(vars="extended")
    record = next(iter(result.values()))
    for field in WORK_EXTRACTORS:
        assert field in record


def test_to_dict_unknown_var_raises(vds):
    with pytest.raises(ValueError, match="Unknown vars"):
        vds.to_dict(vars=["nonexistent_field"])


def test_to_dict_title_no_newlines(vds):
    for record in vds.to_dict().values():
        if record and record.get("title"):
            assert "\n" not in record["title"]
            assert "\r" not in record["title"]


def test_to_dict_abstract_no_newlines(vds):
    for record in vds.to_dict().values():
        if record and record.get("abstract"):
            assert "\n" not in record["abstract"]
            assert "\r" not in record["abstract"]


def test_to_dict_label_included_is_binary(dataset):
    for record in dataset.to_dict().values():
        if record:
            assert record["label_included"] in (0, 1)


def test_to_dict_author_names_is_list(vds):
    result = vds.to_dict(vars=["author_names"])
    for record in result.values():
        if record and record["author_names"] is not None:
            assert isinstance(record["author_names"], list)
            break


def test_to_dict_topics_is_list(vds):
    result = vds.to_dict(vars=["topics"])
    for record in result.values():
        if record and record["topics"] is not None:
            assert isinstance(record["topics"], list)
            break


# ---------------------------------------------------------------------------
# to_frame()
# ---------------------------------------------------------------------------


def test_to_frame_returns_dataframe(vds):
    pd = pytest.importorskip("pandas")
    df = vds.to_frame()
    assert isinstance(df, pd.DataFrame)


def test_to_frame_has_openalex_id_column(vds):
    pytest.importorskip("pandas")
    df = vds.to_frame()
    assert "openalex_id" in df.columns


def test_to_frame_default_columns(vds):
    pytest.importorskip("pandas")
    df = vds.to_frame()
    for col in ("doi", "pmid", "lens_id", "title", "abstract", "label_included"):
        assert col in df.columns


def test_to_frame_shape_matches_labels(dataset):
    pytest.importorskip("pandas")
    df = dataset.to_frame()
    assert len(df) == len(dataset.labels)


def test_to_frame_extended_has_all_extractor_columns(vds):
    pytest.importorskip("pandas")
    df = vds.to_frame(vars="extended")
    for field in WORK_EXTRACTORS:
        assert field in df.columns


def test_to_frame_custom_vars(vds):
    pytest.importorskip("pandas")
    df = vds.to_frame(vars=["cited_by_count", "journal_name"])
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
# WorkModel (Pydantic validation)
# ---------------------------------------------------------------------------


def test_workmodel_from_real_work(vds):
    from synergy_dataset.models import WorkModel

    work, _ = next(vds.iter(validate=True))
    assert isinstance(work, WorkModel)


def test_workmodel_extra_fields_allowed():
    from synergy_dataset.models import WorkModel

    w = WorkModel(id="https://openalex.org/W123", unknown_future_field="value")
    assert w.id == "https://openalex.org/W123"


def test_workmodel_synergy_fields():
    from synergy_dataset.models import WorkModel

    w = WorkModel(abstract_original="Plain text.", language_fasttext="en")
    assert w.abstract_original == "Plain text."
    assert w.language_fasttext == "en"


def test_workmodel_all_optional(vds):
    """An empty dict must not raise — all fields are Optional."""
    from synergy_dataset.models import WorkModel

    w = WorkModel()
    assert w.id is None


# ---------------------------------------------------------------------------
# _reconstruct_abstract
# ---------------------------------------------------------------------------


def test_reconstruct_abstract_basic():
    index = {"Hello": [0], "world": [1]}
    assert _reconstruct_abstract(index) == "Hello world"


def test_reconstruct_abstract_ordering():
    index = {"second": [1], "first": [0], "third": [2]}
    assert _reconstruct_abstract(index) == "first second third"


def test_reconstruct_abstract_none():
    assert _reconstruct_abstract(None) is None


def test_reconstruct_abstract_empty():
    assert _reconstruct_abstract({}) is None


# ---------------------------------------------------------------------------
# _is_valid_abstract
# ---------------------------------------------------------------------------


def test_is_valid_abstract_enough_words():
    index = {f"word{i}": [i] for i in range(20)}
    assert _is_valid_abstract(index) is True


def test_is_valid_abstract_enough_chars():
    # 8 long words — under 20 words but over 100 chars
    index = {f"averylongword{i}": [i] for i in range(8)}
    abstract = " ".join([f"averylongword{i}" for i in range(8)])
    assert len(abstract) >= 100
    assert _is_valid_abstract(index) is True


def test_is_valid_abstract_too_short():
    index = {"hi": [0], "there": [1]}
    assert _is_valid_abstract(index) is False


def test_is_valid_abstract_none():
    assert _is_valid_abstract(None) is False


def test_is_valid_abstract_empty():
    assert _is_valid_abstract({}) is False


# ---------------------------------------------------------------------------
# WORK_EXTRACTORS
# ---------------------------------------------------------------------------


def test_all_extractors_run_without_error(vds):
    """Every registered extractor must not raise on a real work."""
    from synergy_dataset.models import WorkModel

    work, _ = next(vds.iter(validate=True))
    assert isinstance(work, WorkModel)
    for name, extractor in WORK_EXTRACTORS.items():
        try:
            extractor(work)
        except Exception as exc:
            pytest.fail(f"Extractor {name!r} raised {exc!r}")


def test_all_extractors_usable_as_vars(vds):
    """Each extractor name must be accepted by to_dict without error."""
    for name in WORK_EXTRACTORS:
        vds.to_dict(vars=[name])


# ---------------------------------------------------------------------------
# Download single dataset
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("dataset_name", DATASETS)
def test_download_single_dataset(dataset_name, tmpdir):
    download_raw_subset(dataset_name, path=tmpdir)

    datasets = iter_datasets(path=tmpdir)
    d = next(datasets)

    assert isinstance(d, Dataset)
    assert d.name == dataset_name
    assert isinstance(d.labels, dict)

    with pytest.raises(StopIteration):
        next(datasets)
