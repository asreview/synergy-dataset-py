from pyodss import iter_datasets, Dataset


def test_iter():

    assert len(list(iter_datasets())) > 1


def build_dataset():

    d = Dataset("van_de_Schoot_2017")

    d.to_dict()
