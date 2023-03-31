import csv
import glob
import json
from io import BytesIO
from pathlib import Path
from urllib.request import urlopen
from zipfile import ZipFile

try:
    import pandas as pd
except ImportError:
    pass

from pyalex import Work

from synergy_dataset.config import DOWNLOAD_PATH
from synergy_dataset.config import SYNERGY_PATH
from synergy_dataset.config import RELEASE_URL
from synergy_dataset.config import RELEASE_VERSION
from synergy_dataset.config import WORK_MAPPING



def _dataset_available():
    """Check if the dataset is available

    Returns:
        bool: True if the dataset is available
    """

    return SYNERGY_PATH.exists()


def download_raw_dataset(url=RELEASE_URL, path=DOWNLOAD_PATH):
    """Download the raw dataset from the ODSS repository
    
    Args:
        url (str, optional): URL to the ODSS dataset. Defaults to RELEASE_URL.
        path (str, optional): Path to download the dataset to. Defaults to DOWNLOAD_PATH.

        """

    print(f"Downloading version {RELEASE_VERSION} of the ODSS dataset")

    release_zip = ZipFile(BytesIO(urlopen(url).read()))
    release_zip.extractall(path=path)


def iter_datasets(fp=SYNERGY_PATH):
    """Iterate over the available datasets

    Args:
        fp (str, optional): Path to the dataset. Defaults to SYNERGY_PATH.

    Yields:
        Dataset: Dataset object
    """

    if not _dataset_available():
        download_raw_dataset()

    for dataset in sorted(
        glob.glob(str(Path(fp, "*", "metadata.json"))), key=lambda x: x.lower()
    ):

        yield Dataset(dataset.split("/")[-2])


class Dataset(object):
    """Dataset object belonging to a systematic review"""

    def __init__(self, name):
        super(Dataset, self).__init__()
        self.name = name

    @property
    def metadata(self):
        """Metadata for the dataset
        """

        if not hasattr(self, "_metadata"):
            with open(Path(SYNERGY_PATH, self.name, "metadata.json"), "r") as f:
                self._metadata = json.load(f)

        return self._metadata

    @property
    def metadata_work(self):
        """Metadata on the corresponding publication as work"""

        if not hasattr(self, "_metadata_work"):
            with open(
                Path(SYNERGY_PATH, self.name, "publication_metadata.json"), "r"
            ) as f:
                self._metadata_work = json.load(f)

        return self._metadata_work

    @property
    def labels(self):
        """Metadata on the corresponding publication as work"""

        if not hasattr(self, "_labels"):
            self._labels = {}
            with open(Path(SYNERGY_PATH, self.name, "labels.csv"), newline="") as idfile:
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

        with ZipFile(Path(SYNERGY_PATH, self.name, "works.zip"), "r") as z:

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

        records = {}
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
                record["title"] = record["title"].replace('\n', ' ').replace('\r', '')
            if "abstract" in record and record["abstract"]:
                record["abstract"] = record["abstract"].replace('\n', ' ').replace('\r', '')

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
