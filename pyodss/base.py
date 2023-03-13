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

from pyodss.config import DOWNLOAD_PATH
from pyodss.config import ODSS_PATH
from pyodss.config import RELEASE_URL
from pyodss.config import RELEASE_VERSION

WORK_MAPPING = {
    "id": lambda x: x["id"],
    "doi": lambda x: x["doi"],
    "title": lambda x: x["title"],
    "abstract": lambda x: x["abstract"],
}


def _dataset_available():

    return ODSS_PATH.exists()


def download_raw_dataset(url=RELEASE_URL, path=DOWNLOAD_PATH):

    print(f"Downloading version {RELEASE_VERSION} of the ODSS dataset")

    release_zip = ZipFile(BytesIO(urlopen(url).read()))
    release_zip.extractall(path=path)


def iter_datasets(fp=ODSS_PATH):

    if not _dataset_available():
        download_raw_dataset()

    for dataset in sorted(
        glob.glob(str(Path(fp, "*", "metadata.json"))), key=lambda x: x.lower()
    ):

        yield Dataset(dataset.split("/")[-2])


class Dataset(object):
    """ODSS Dataset"""

    def __init__(self, name):
        super(Dataset, self).__init__()
        self.name = name

    @property
    def metadata(self):

        if not hasattr(self, "_metadata"):
            with open(Path(ODSS_PATH, self.name, "metadata.json"), "r") as f:
                self._metadata = json.load(f)

        return self._metadata

    @property
    def metadata_work(self):

        if not hasattr(self, "_metadata"):
            with open(
                Path(ODSS_PATH, self.name, "publication_metadata.json"), "r"
            ) as f:
                self._metadata_work = json.load(f)

        return self._metadata_work

    def _iter_works(self):

        with ZipFile(Path(ODSS_PATH, self.name, "works.zip"), "r") as z:

            for work_set in z.namelist():
                with z.open(work_set) as f:
                    d = json.loads(f.read())

                    for di in d:
                        yield Work(di)

    def to_dict(self, variables=WORK_MAPPING):

        records = {}
        for work in self._iter_works():

            record = {}
            for key, value in variables.items():
                record[key] = value(work)

            records[work["id"]] = record

        store = {}
        with open(Path(ODSS_PATH, self.name, "labels.csv"), newline="") as idfile:
            reader = csv.DictReader(idfile)
            for row in reader:

                try:
                    store[row["openalex_id"]] = {
                        **records[row["openalex_id"]],
                        "label_included": int(row["label_included"]),
                    }
                except KeyError:
                    store[row["openalex_id"]] = {
                        "label_included": int(row["label_included"])
                    }

        return store

    def to_frame(self, *args, **kwargs):

        try:
            df = pd.DataFrame.from_dict(self.to_dict(*args, **kwargs), orient="index")
            df.index.name = "openalex_id"
            return df
        except NameError:
            raise ImportError("Install pandas to export to pandas.DataFrame")
