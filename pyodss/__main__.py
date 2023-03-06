import argparse

from pathlib import Path

from pyodss._version import __version__
from pyodss.base import Dataset, iter_datasets, _raw_download_dataset
import json
import sys
from glob import glob


from tabulate import tabulate

def download_dataset(argv):

    parser = argparse.ArgumentParser(
        prog="pyodss",
        description="Python package for ODSS dataset.",
    )
    parser.add_argument(
        "-d",
        "--dataset",
        nargs="*",
        default=None,
        help="Dataset name.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="pyodss_dataset/",
        help="Dataset output path.",
    )
    parser.add_argument(
        "-l",
        "--ignore-legal",
        dest="legal",
        help="Ignore legal message.",
        action="store_true",
    )
    # version
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version="%(prog)s {version}".format(version=__version__),
    )

    args, _ = parser.parse_known_args()

    if not args.legal:
        user_input = input("I agree with building dataset ([Y]es,[N]o):")
        if user_input.lower() not in ["y", "yes"]:
            return

    if args.dataset is not None:
        d = Dataset(args.dataset)
        result = d.to_frame()

        Path(args.output).parent.mkdir(exist_ok=True, parents=True)
        if args.output:
            result.to_csv(args.output, index=False)
    else:
        for dataset in iter_datasets():
            print(f"Download dataset {dataset.name}")
            dataset.to_frame().to_csv(Path(args.output, f"{dataset.name}.csv"), index=False)



def list_datasets(argv):

    parser = argparse.ArgumentParser(
        prog="pyodss",
        description="List datasets.",
    )
    parser.add_argument(
        "--tablefmt",
        default="simple",
        help="Table format.",
    )
    args = parser.parse_args(argv)

    # add if statement
    _raw_download_dataset()

    table_values = []

    for dataset in iter_datasets():

        if "concepts" not in dataset.metadata_work:
            print(dataset.metadata["publication"]["openalex_id"], "No concepts found")
            continue

        concepts = list(filter(lambda x: x["level"] == 0, dataset.metadata_work["concepts"]))
        concepts_str = ", ".join([x["display_name"] for x in concepts])
        table_values.append(["{}".format(dataset.metadata["key"]), concepts_str, dataset.metadata["data"]["n_records"], dataset.metadata["data"]["n_records_included"]])

    print("\n", tabulate(table_values,
                         headers=["Dataset", "Field", "Count", "Included"],
                         tablefmt=args.tablefmt), "\n")


def show_dataset(argv):

    parser = argparse.ArgumentParser(
        prog="pyodss",
        description="Show dataset.",
    )
    parser.add_argument(
        "dataset",
        help="Dataset name.",
    )
    args = parser.parse_args(argv)

    d = Dataset(args.dataset)

    print(d.metadata_work["display_name"])
    print(d.metadata_work["publication_year"])

    concepts = list(filter(lambda x: x["level"] == 0, d.metadata_work["concepts"]))
    concepts_str = ", ".join([x["display_name"] for x in concepts])

    print("Fields:", concepts_str)


if __name__ == "__main__":

    if sys.argv[1] == "list":
        list_datasets(sys.argv[2:])
    elif sys.argv[1] == "show":
        show_dataset(sys.argv[2:])
    elif sys.argv[1] == "download":
        download_dataset(sys.argv[2:])
    else:
        main()
