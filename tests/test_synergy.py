from synergy_dataset import Dataset
from synergy_dataset import iter_datasets


def test_iter():

    assert len(list(iter_datasets())) > 1


def test_build_dataset():

    d = Dataset("van_de_Schoot_2017")

    d.to_dict()
