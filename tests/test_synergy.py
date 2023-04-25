import pytest

from synergy_dataset import Dataset
from synergy_dataset import iter_datasets


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
