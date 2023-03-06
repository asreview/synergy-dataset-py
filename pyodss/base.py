import argparse
import csv
import glob
import json
import logging
from pathlib import Path
from zipfile import ZipFile
from io import BytesIO
from urllib.request import urlopen

import pandas as pd
from pyalex import Work

WORK_MAPPING = {"id": lambda x: x["id"], "title": lambda x: x["title"], "abstract": lambda x: x["abstract"]}

ODSS_PATH = Path("tmp","odss","systematic-review-datasets-release-v0.1")

def iter_datasets(fp=ODSS_PATH):

    for dataset in sorted(glob.glob(str(Path(fp, "*", "metadata.json")))):

        yield Dataset(dataset.split("/")[-2])


def _raw_download_dataset(url="https://github.com/asreview/systematic-review-datasets/archive/refs/tags/release/v0.1.zip", path=ODSS_PATH):

    if not ODSS_PATH.exists():
        release_zip = ZipFile(BytesIO(urlopen(url).read()))
        release_zip.extractall(path=path)


class Dataset(object):
    """docstring for Dataset"""

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
            with open(Path(ODSS_PATH, self.name, "publication_metadata.json"), "r") as f:
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
        with open(
            Path(ODSS_PATH, self.name, "labels.csv"), newline=""
        ) as idfile:
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

        df = pd.DataFrame.from_dict(self.to_dict(*args, **kwargs), orient="index")
        df.index.name = "openalex_id"

        return df
