import argparse
import sys
from pathlib import Path

from tabulate import tabulate

from pyodss._version import __version__
from pyodss.base import Dataset
from pyodss.base import _dataset_available
from pyodss.base import download_raw_dataset
from pyodss.base import iter_datasets

LEGAL_NOTE = """
Due to legal constraints, paper abstracts in ODSS cannot be published in
plaintext. Abstracts are instead stored as an inverted index. Inverted
indexes store information about each word in a body of text, including
the number of occurrences and the position of each occurrence. Read
more:
https://learn.microsoft.com/en-us/academic-services/graph/resources-faq

For machine learning purposes, it can be helpful to convert the inverted
abstract back into plaintext locally. Keep in mind that paper abstracts
in ODSS cannot be published as plaintext again. Therefore you can refer
to the version of the ODSS dataset.

Would you like to convert the inverted abstract to plaintext?"""


def main():

    if len(sys.argv) == 1:
        info()
    elif sys.argv[1] == "list":
        list_datasets(sys.argv[2:])
    elif sys.argv[1] == "show":
        show_dataset(sys.argv[2:])
    elif sys.argv[1] == "get":
        get_dataset(sys.argv[2:])
    elif sys.argv[1] == "credits":
        credit_dataset(sys.argv[2:])
    else:
        info()


def info():
    parser = argparse.ArgumentParser(
        prog="pyodss",
        description="Python package for ODSS dataset. Use the commands get, show or list.",
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


def get_dataset(argv):

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
        user_input = input(f"{LEGAL_NOTE} ([Y]es,[N]o):\n")
        if user_input.lower() not in ["y", "yes"]:

            if _dataset_available():
                print(
                    "ODSS dataset already downloaded, but not",
                    "possible to build dataset (because of answer No).",
                )
            else:
                print("Downloading dataset, but not possible to build dataset.")
                print("Read more: LINK.")
        else:
            args.legal = True

    # download the dataset if note available
    if not _dataset_available():
        download_raw_dataset()

    if args.legal:

        print("Building dataset")
        # create output folder
        Path(args.output).mkdir(exist_ok=True, parents=True)

        if args.dataset is not None:
            d = Dataset(args.dataset)
            result = d.to_frame()

            if args.output:
                result.to_csv(args.output, index=False)
        else:
            for dataset in iter_datasets():
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
    parser.add_argument(
        "--n-topics",
        default=2,
        type=int,
        help="The number of topics to display in the table.",
    )
    args = parser.parse_args(argv)

    # download the dataset if note available
    if not _dataset_available():
        download_raw_dataset()

    table_values = []

    for i, dataset in enumerate(iter_datasets()):

        if "concepts" not in dataset.metadata_work:
            print(dataset.metadata["publication"]["openalex_id"], "No concepts found")

        concepts = list(
            filter(lambda x: x["level"] == 0, dataset.metadata_work["concepts"])
        )

        n_topics = args.n_topics if args.n_topics != -1 else len(concepts)
        concepts_str = ", ".join([x["display_name"] for x in concepts[0:n_topics]])
        table_values.append(
            [
                i + 1,
                "{}".format(dataset.metadata["key"]),
                concepts_str,
                dataset.metadata["data"]["n_records"],
                dataset.metadata["data"]["n_records_included"],
                round(
                    (
                        dataset.metadata["data"]["n_records_included"]
                        / dataset.metadata["data"]["n_records"]
                    )
                    * 100,
                    1,
                ),
            ]
        )

    print(
        "\n",
        tabulate(
            table_values,
            headers=["Nr", "Dataset", "Field", "Count", "Included", "%"],
            tablefmt=args.tablefmt,
            # showindex="Nr",
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

    # download the dataset if note available
    if not _dataset_available():
        download_raw_dataset()

    d = Dataset(args.dataset)

    print(d.metadata_work["display_name"])
    print(d.metadata_work["publication_year"])

    concepts = list(filter(lambda x: x["level"] == 0, d.metadata_work["concepts"]))
    concepts_str = ", ".join([x["display_name"] for x in concepts])

    print("Fields:", concepts_str, "\n\n")

    print("Citation (APA)\n")
    print(d.metadata["publication"]["citation"]["apa"])


def credit_dataset(argv):

    parser = argparse.ArgumentParser(
        prog="pyodss",
        description="Credit authors of the datasets.",
    )
    parser.parse_args(argv)

    # download the dataset if note available
    if not _dataset_available():
        download_raw_dataset()

    authors = []

    for i, dataset in enumerate(iter_datasets()):
        for a in dataset.metadata_work["authorships"]:
            authors.append(a["author"]["display_name"])

    # # with orcid links
    # authors = []

    # for i, dataset in enumerate(iter_datasets()):
    #     for a in dataset.metadata_work["authorships"]:

    #         if "orcid" in a["author"] and a["author"]["orcid"]:
    #             authors.append(f"[{a['author']['display_name']}]({a['author']['orcid']})")
    #         else:
    #             authors.append(a["author"]["display_name"])

    print(
        "\nWe would like to thank the following authors for openly",
        "sharing the data correponding their systematic review:\n",
    )
    print(", ".join(authors), "\n")

    print("\nReferences:\n")

    for i, dataset in enumerate(iter_datasets()):

        print(
            f"[{dataset.metadata['key']}]",
            dataset.metadata["publication"]["citation"]["apa"],
        )


if __name__ == "__main__":
    main()
