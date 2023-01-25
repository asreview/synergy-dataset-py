import argparse
import csv
import glob
import json
import logging
from pathlib import Path
from zipfile import ZipFile

import pandas as pd
from pyalex import Work


class Dataset(object):
    """docstring for Dataset"""

    def __init__(self, name):
        super(Dataset, self).__init__()
        self.name = name

        self.works = None

        self.fp_ids = pd.read_csv(
            Path("..", "odss-release", name, f"{name}.csv"), index_col=["openalex_id"]
        )

    def iter_works(self):

        with ZipFile(Path("..", "odss-release", self.name, "works.zip"), "r") as z:

            for work_set in z.namelist():
                with z.open(work_set) as f:
                    d = json.loads(f.read())

                    for di in d:
                        yield Work(di)

    def to_dict(self, variable={"title": "title", "abstract": "abstract"}):

        records = {}
        for work in self.iter_works():

            record = {}
            for key, value in variable.items():
                record[key] = work[key]

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
