import argparse
import sys
from pathlib import Path

from tabulate import tabulate

from pyodss._version import __version__
from pyodss.base import Dataset
from pyodss.base import iter_datasets

LEGAL_NOTE = """
Due to legal constraints, paper abstracts in ODSS cannot be published as
plaintext. Abstract are instead stored as an inverted index. Inverted
indexes store information about each word in a body of text, including
the number of occurrences and the position of each occurrence. Read
more:
https://learn.microsoft.com/en-us/academic-services/graph/resources-faq

For machine learning purposes, it can be useful to convert the inverted
abstract back into plaintext locally. Keep in mind that paper abstracts
in ODSS cannot be published as plaintext again. Therefore you can refer
to the version of the ODSS dataset.

Would you like to convert the inverted abstract to plaintext?
"""


def main():

    parser = argparse.ArgumentParser(
        prog="pyodss",
        description="Python package for ODSS dataset. Use the commands download, show or list.",
    )
    # version
    parser.add_argument(
        "-V",
        "--version",
        action="version",
        version="%(prog)s {version}".format(version=__version__),
    )

    args, _ = parser.parse_known_args()

    parser.print_usage()


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
        default="pyodss_dataset",
        help="Dataset output path.",
    )
    parser.add_argument(
        "-l",
        "--ignore-legal",
        dest="legal",
        help="Ignore legal message.",
        action="store_true",
    )

    args, _ = parser.parse_known_args()

    if not args.legal:
        user_input = input(f"{LEGAL_NOTE} ([Y]es,[N]o):")
        print("\n")
        if user_input.lower() not in ["y", "yes"]:
            print("Not possible to build dataset")
            return

    # create output folder
    Path(args.output).mkdir(exist_ok=True, parents=True)

    if args.dataset is not None:
        d = Dataset(args.dataset)
        result = d.to_frame()

        if args.output:
            result.to_csv(args.output, index=False)
    else:
        for dataset in iter_datasets():
            print(f"Collect dataset {dataset.name}")
            dataset.to_frame().to_csv(
                Path(args.output, f"{dataset.name}.csv"), index=False
            )


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

    table_values = []

    for dataset in iter_datasets():

        if "concepts" not in dataset.metadata_work:
            print(dataset.metadata["publication"]["openalex_id"], "No concepts found")
            continue

        concepts = list(
            filter(lambda x: x["level"] == 0, dataset.metadata_work["concepts"])
        )
        concepts_str = ", ".join([x["display_name"] for x in concepts])
        table_values.append(
            [
                "{}".format(dataset.metadata["key"]),
                concepts_str,
                dataset.metadata["data"]["n_records"],
                dataset.metadata["data"]["n_records_included"],
            ]
        )

    print(
        "\n",
        tabulate(
            table_values,
            headers=["Dataset", "Field", "Count", "Included"],
            tablefmt=args.tablefmt,
        ),
        "\n",
    )


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

    if len(sys.argv) == 1:
        main()
    elif sys.argv[1] == "list":
        list_datasets(sys.argv[2:])
    elif sys.argv[1] == "show":
        show_dataset(sys.argv[2:])
    elif sys.argv[1] == "download":
        download_dataset(sys.argv[2:])
    else:
        main()
