import csv
import glob
import json
import os
import zipfile
from io import BytesIO
from pathlib import Path
from urllib.request import urlopen

import requests

try:
    import pandas as pd
except ImportError:
    pass

from pyalex import Work

WORK_MAPPING = ["doi", "title", "abstract"]

SYNERGY_VERSION = (
    os.getenv("SYNERGY_VERSION") if os.getenv("SYNERGY_VERSION") else "1.0"
)
SYNERGY_PATH = os.getenv("SYNERGY_PATH")
SYNERGY_ROOT = Path("~", ".synergy_dataset_source").expanduser()


def _get_path_raw_dataset(version=None):
    if SYNERGY_PATH and SYNERGY_PATH == "development":
        return Path(__file__).parent.parent.parent / "synergy-release"
    elif SYNERGY_PATH:
        return Path(SYNERGY_PATH).expanduser()
    else:
        version = SYNERGY_VERSION if version is None else version
        return Path(SYNERGY_ROOT, f"synergy-dataset-{version}")


def _get_download_url(version=None, source="dataverse"):
    if version is None:
        version = SYNERGY_VERSION

    if source == "dataverse":
        return f"https://dataverse.nl/api/access/dataset/:persistentId/versions/{version}?persistentId=doi:10.34894/HE6NAQ"  # noqa
    elif source == "github":
        return f"https://github.com/asreview/synergy-dataset/archive/refs/tags/v{version}.zip"  # noqa
    else:
        raise ValueError("Unknown source")


def _dataset_available(version=SYNERGY_VERSION):
    """Check if the dataset is available.

    Args:
        version (str, optional): The version of the dataset to download.

    Returns:
        bool: True if the dataset is available
    """
    return _get_path_raw_dataset(version=version).exists()


def download_raw_dataset(url=None, path=SYNERGY_ROOT, version=None, source="dataverse"):
    """Download the raw dataset from the SYNERGY repository.

    Args:
        url (str, optional): URL to the SYNERGY dataset.
        Defaults to latest github release.
        path (str, optional): Path to download the dataset to.
        Defaults to ~/.synergy_dataset_source.
        version (str, optional): The version of the dataset to download.
        source (str, optional): The source to download (github, dataverse).
        Default dataverse.
    """
    if url is None:
        url = _get_download_url(version=version, source=source)

    print(f"Downloading version {SYNERGY_VERSION} of the SYNERGY dataset...")

    release_zip = zipfile.ZipFile(BytesIO(urlopen(url).read()))
    release_zip.extractall(path=path)

    # hack because the version on dataverse has a v prefix
    for f in Path(path).iterdir():
        if f.is_dir() and f.name.startswith("synergy-dataset-v"):
            os.rename(f, str(f).replace("synergy-dataset-v", "synergy-dataset-"))


def download_raw_subset(name, path=SYNERGY_ROOT, version=None):
    """Download the raw dataset from the SYNERGY repository.

    Args:
        url (str, optional): URL to the SYNERGY dataset.
        Defaults to latest github release.
        path (str, optional): Path to download the dataset to.
        Defaults to ~/.synergy_dataset_source.
        version (str, optional): The version of the dataset to download.
        source (str, optional): The source to download (github, dataverse).
        Default dataverse.
    """

    version = SYNERGY_VERSION if version is None else version
    url_list = f"https://dataverse.nl/api/datasets/:persistentId/versions/{version}?persistentId=doi:10.34894/HE6NAQ"  # noqa

    r = requests.get(url_list)
    file_list = r.json()["data"]["files"]

    files_subset = filter(
        lambda x: x["directoryLabel"] == f"synergy-dataset-v1.0/{name}", file_list
    )
    ids = ",".join(str(x["dataFile"]["id"]) for x in files_subset)

    url_download = f"https://dataverse.nl/api/access/datafiles/{ids}"
    download_raw_dataset(url=url_download, path=path)


def iter_datasets(path=None, version=None):
    """Iterate over the available datasets.

    Args:
        path (str, optional): Path to download the dataset to.
        Defaults to ~/.synergy_dataset_source.
        version (str, optional): The version of the dataset to download.

    Yields:
        Dataset: Dataset object
    """
    version = SYNERGY_VERSION if version is None else version

    if path is None and not _dataset_available():
        download_raw_dataset(version=version)
        path = _get_path_raw_dataset(version=version)
    elif path is None and _dataset_available():
        path = _get_path_raw_dataset(version=version)
    else:
        version = SYNERGY_VERSION if version is None else version
        path = Path(path, f"synergy-dataset-{version}")

    for dataset in sorted(
        glob.glob(str(Path(path, "*", "metadata.json"))),
        key=lambda x: x.lower(),
    ):
        yield Dataset(Path(dataset).parts[-2])


class Dataset:
    """Dataset object belonging to a systematic review."""

    def __init__(self, name):
        super().__init__()
        self.name = name

    @property
    def cite(self):
        """Citation for the publication."""
        if not hasattr(self, "_cite"):
            with open(
                Path(_get_path_raw_dataset(), self.name, "CITATION.txt"),
                encoding="utf-8",
            ) as f:
                self._cite = f.read()

        return self._cite

    @property
    def cite_collection(self):
        """Citation for the collection."""
        if not hasattr(self, "_cite_collection"):
            with open(
                Path(_get_path_raw_dataset(), self.name, "CITATION_collection.txt"),
                encoding="utf-8",
            ) as f:
                self._cite_collection = f.read()

        return self._cite_collection

    @property
    def metadata(self):
        """Metadata for the dataset."""
        if not hasattr(self, "_metadata"):
            with open(
                Path(_get_path_raw_dataset(), self.name, "metadata.json"),
                encoding="utf-8",
            ) as f:
                self._metadata = json.load(f)
            with open(
                Path(_get_path_raw_dataset(), self.name, "metadata_publication.json"),
                encoding="utf-8",
            ) as f:
                self._metadata["publication"] = json.load(f)

            try:
                with open(
                    Path(
                        _get_path_raw_dataset(), self.name, "metadata_collection.json"
                    ),
                    encoding="utf-8",
                ) as f:
                    self._metadata["collection"] = json.load(f)
            except FileNotFoundError:
                pass

        return self._metadata

    @property
    def labels(self):
        """Metadata on the corresponding publication as work."""
        if not hasattr(self, "_labels"):
            self._labels = {}
            with open(
                Path(_get_path_raw_dataset(), self.name, "labels.csv"),
                newline="",
                encoding="utf-8",
            ) as idfile:
                reader = csv.DictReader(idfile)
                for row in reader:
                    self._labels[row["openalex_id"]] = int(row["label_included"])

            return self._labels

        return self._labels

    def iter(self):
        """Iterate over the works in the dataset.

        Yields:
            Work: pyalex.Work object, label
        """
        p_zipped_works = str(Path(_get_path_raw_dataset(), self.name, "works_*.zip"))

        for f_work in glob.glob(p_zipped_works):
            with zipfile.ZipFile(f_work, "r") as z:
                for work_set in z.namelist():
                    with z.open(work_set) as f:
                        d = json.loads(f.read())

                        for di in d:
                            yield Work(di), self.labels[di["id"]]

    def to_dict(self, variables=WORK_MAPPING):
        """Export the dataset to a dictionary.

        Args:
            variables (list, optional): List of variables to export.
            Defaults to WORK_MAPPING.

        Returns:
            dict: Dictionary of the dataset
        """
        records = {k: None for k, v in self.labels.items()}
        for work, label_included in self.iter():
            if isinstance(variables, dict):
                record = {}
                for key, value in variables.items():
                    if isinstance(value, str):
                        record[key] = work[value]
                    else:
                        record[key] = value(work)
            elif isinstance(variables, list):
                record = {}
                for key in variables:
                    record[key] = work[key]
            else:
                record = work

            # remove newlines
            if "title" in record and record["title"]:
                record["title"] = record["title"].replace("\n", " ").replace("\r", "")
            if "abstract" in record and record["abstract"]:
                record["abstract"] = (
                    record["abstract"].replace("\n", " ").replace("\r", "")
                )

            record["label_included"] = label_included
            records[work["id"]] = record

        return records

    def to_frame(self, *args, **kwargs):
        """Export the dataset to a pandas.DataFrame.

        Args:
            variables (list, optional): List of variables to export.
            Defaults to WORK_MAPPING.

        Returns:
            pandas.DataFrame: DataFrame of the dataset
        """
        try:
            df = pd.DataFrame.from_dict(self.to_dict(*args, **kwargs), orient="index")
            df.index.name = "openalex_id"
            return df
        except NameError as err:
            raise ImportError("Install pandas to export to pandas.DataFrame") from err
