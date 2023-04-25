import csv
import glob
import json
import os
from io import BytesIO
from pathlib import Path
from urllib.request import urlopen
from zipfile import ZipFile

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


def _get_path_raw_dataset(version=SYNERGY_VERSION):
    if SYNERGY_PATH and SYNERGY_PATH == "development":
        return Path(__file__).parent.parent.parent / "synergy-release"
    elif SYNERGY_PATH:
        return Path(SYNERGY_PATH).expanduser()
    else:
        return Path(SYNERGY_ROOT, f"synergy-dataset-{version}")


def _get_download_url(version=None, source="github"):
    if version is None:
        version = SYNERGY_VERSION

    if source == "github":
        github_url = "https://github.com/asreview/synergy-dataset/archive/refs/tags/v{}.zip"  # noqa
        return github_url.format(version)
    else:
        raise ValueError("Unknown source")


def _dataset_available():
    """Check if the dataset is available

    Returns:
        bool: True if the dataset is available
    """

    return _get_path_raw_dataset().exists()


def download_raw_dataset(url=None, path=SYNERGY_ROOT):
    """Download the raw dataset from the SYNERGY repository

    Args:
        url (str, optional): URL to the SYNERGY dataset.
        Defaults to latest github release.
        path (str, optional): Path to download the dataset to.
        Defaults to ~/.synergy_dataset_source.

    """

    if url is None:
        url = _get_download_url()

    print(f"Downloading version {SYNERGY_VERSION} of the SYNERGY dataset...")

    release_zip = ZipFile(BytesIO(urlopen(url).read()))
    release_zip.extractall(path=path)


def iter_datasets():
    """Iterate over the available datasets

    Yields:
        Dataset: Dataset object
    """

    if not _dataset_available():
        download_raw_dataset()

    for dataset in sorted(
        glob.glob(str(Path(_get_path_raw_dataset(), "*", "metadata.json"))),
        key=lambda x: x.lower(),
    ):
        yield Dataset(Path(dataset).parts[-2])


class Dataset(object):
    """Dataset object belonging to a systematic review"""

    def __init__(self, name):
        super(Dataset, self).__init__()
        self.name = name

    @property
    def cite(self):
        """Citation for the publication"""

        if not hasattr(self, "_cite"):
            with open(
                Path(_get_path_raw_dataset(), self.name, "CITATION.txt"),
                "r",
                encoding="utf-8",
            ) as f:
                self._cite = f.read()

        return self._cite

    @property
    def cite_collection(self):
        """Citation for the collection"""

        if not hasattr(self, "_cite_collection"):
            with open(
                Path(_get_path_raw_dataset(), self.name, "CITATION_collection.txt"),
                "r",
                encoding="utf-8",
            ) as f:
                self._cite_collection = f.read()

        return self._cite_collection

    @property
    def metadata(self):
        """Metadata for the dataset"""

        if not hasattr(self, "_metadata"):
            with open(
                Path(_get_path_raw_dataset(), self.name, "metadata.json"),
                "r",
                encoding="utf-8",
            ) as f:
                self._metadata = json.load(f)
            with open(
                Path(_get_path_raw_dataset(), self.name, "metadata_publication.json"),
                "r",
                encoding="utf-8",
            ) as f:
                self._metadata["publication"] = json.load(f)

            try:
                with open(
                    Path(
                        _get_path_raw_dataset(), self.name, "metadata_collection.json"
                    ),
                    "r",
                    encoding="utf-8",
                ) as f:
                    self._metadata["collection"] = json.load(f)
            except FileNotFoundError:
                pass

        return self._metadata

    @property
    def labels(self):
        """Metadata on the corresponding publication as work"""

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
        """Iterate over the works in the dataset

        Yields:
            Work: pyalex.Work object, label
        """

        p_zipped_works = str(Path(_get_path_raw_dataset(), self.name, "works_*.zip"))

        for f_work in glob.glob(p_zipped_works):
            with ZipFile(f_work, "r") as z:
                for work_set in z.namelist():
                    with z.open(work_set) as f:
                        d = json.loads(f.read())

                        for di in d:
                            yield Work(di), self.labels[di["id"]]

    def to_dict(self, variables=WORK_MAPPING):
        """Export the dataset to a dictionary

        Args:
            variables (list, optional): List of variables to export. Defaults to WORK_MAPPING.

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
        """Export the dataset to a pandas.DataFrame

        Args:
            variables (list, optional): List of variables to export. Defaults to WORK_MAPPING.

        Returns:
            pandas.DataFrame: DataFrame of the dataset
        """

        try:
            df = pd.DataFrame.from_dict(self.to_dict(*args, **kwargs), orient="index")
            df.index.name = "openalex_id"
            return df
        except NameError:
            raise ImportError("Install pandas to export to pandas.DataFrame")
