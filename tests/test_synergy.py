import pytest

from synergy_dataset import Dataset
from synergy_dataset import iter_datasets
from synergy_dataset.base import download_raw_subset


def test_iter():
    assert len(list(iter_datasets())) > 1


def test_build_dataset():
    d = Dataset("van_de_Schoot_2018")

    d.to_dict()


@pytest.mark.parametrize("dataset_name", ["Walker_2018", "van_de_Schoot_2018"])
def test_order_iter_dataset(dataset_name):
    d = Dataset(dataset_name)

    keys = [k for k, v in d.to_dict().items()]

    assert keys == list(d.labels.keys())


@pytest.mark.parametrize("dataset_name", ["Walker_2018", "van_de_Schoot_2018"])
def test_download_single_dataset(dataset_name, tmpdir):
    download_raw_subset(dataset_name, path=tmpdir)

    datasets = iter_datasets(path=tmpdir)
    d = next(datasets)

    assert isinstance(d, Dataset)
    assert d.name == dataset_name
    assert isinstance(d.labels, dict)

    with pytest.raises(StopIteration):
        next(datasets)
