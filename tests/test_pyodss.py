from pyodss import iter_datasets


def test_iter():

    assert len(list(iter_datasets())) > 1
