from synergy_dataset._version import __version__  # noqa
from synergy_dataset._version import __version_tuple__  # noqa
from synergy_dataset.base import Dataset
from synergy_dataset.base import download_raw_dataset
from synergy_dataset.extractors import WORK_EXTRACTORS
from synergy_dataset.base import download_raw_subset
from synergy_dataset.base import iter_datasets

__all__ = [
    "Dataset",
    "WORK_EXTRACTORS",
    "download_raw_dataset",
    "download_raw_subset",
    "iter_datasets",
    "WorkModel",
]


def __getattr__(name):
    if name == "WorkModel":
        from synergy_dataset.models import WorkModel

        return WorkModel
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
