import argparse
import csv
import glob
import json
import logging
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
from pyalex import Work

WORK_MAPPING = {"title": lambda x: x["title"], "abstract": lambda x: x["abstract"]}


def iter_metadata(
    metadata_fp=Path("..", "odss-release", f"metadata.json"),
    works_fp=Path("..", "odss-release", f"metadata_works.json")
):

    with open(works_fp, "r") as f:
        works = json.load(f)

    with open(metadata_fp, "r") as f:
        metadata = json.load(f)

    for dataset in metadata["datasets"]:

        if "openalex_id" not in dataset["publication"]:
            print("Failed to load dataset", dataset["key"])
            continue

        for w in works:

            # print(dataset["publication"]["openalex_id"], w["id"])
            if dataset["publication"]["openalex_id"] == w["id"]:
                yield dataset, w
                break


class Dataset(object):
    """docstring for Dataset"""

    def __init__(self, name):
        super(Dataset, self).__init__()
        self.name = name

    @property
    def metadata(self):

        if not hasattr(self, "_metadata"):
            with open(Path("..", "odss-release", self.name, "metadata.json"), "r") as f:
                self._metadata = json.load(f)

        return self._metadata

    @property
    def metadata_work(self):

        if not hasattr(self, "_metadata"):
            with open(Path("..", "odss-release", self.name, "metadata_works.json"), "r") as f:
                self._metadata_work = json.load(f)

        return self._metadata_work

    def iter_works(self):

        with ZipFile(Path("..", "odss-release", self.name, "works.zip"), "r") as z:

            for work_set in z.namelist():
                with z.open(work_set) as f:
                    d = json.loads(f.read())

                    for di in d:
                        yield Work(di)

    def to_dict(self, variable=WORK_MAPPING):

        records = {}
        for work in self.iter_works():

            record = {}
            for key, value in variable.items():
                record[key] = value(work)

            records[work["id"]] = record

        store = {}
        with open(
            Path("..", "odss-release", self.name, f"{self.name}.csv"), newline=""
        ) as idfile:
            reader = csv.DictReader(idfile)
            for row in reader:

                try:
                    store[row["openalex_id"]] = {
                        **records[row["openalex_id"]],
                        "label_included": row["label_included"],
                    }
                except KeyError:
                    store[row["openalex_id"]] = {
                        "label_included": row["label_included"]
                    }

        return store

    def to_frame(self, *args, **kwargs):

        df = pd.DataFrame.from_dict(self.to_dict(*args, **kwargs), orient="index")
        df.index.name = "openalex_id"

        return df
