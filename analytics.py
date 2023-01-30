
import json
from pathlib import Path

from pyodss import Dataset

for dataset, openalex_work in iter_metadata():

    if "concepts" not in openalex_work:
        print(dataset["publication"]["openalex_id"], "No concepts found")
        continue

    concepts = list(filter(lambda x: x["level"] == 0, openalex_work["concepts"]))
    concepts_str = ", ".join([x["display_name"] for x in concepts])
    print(dataset["publication"]["openalex_id"], concepts_str)
