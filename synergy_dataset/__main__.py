import argparse
import os
import sys
from pathlib import Path

from tabulate import tabulate
from tqdm import tqdm

from synergy_dataset._version import __version__
from synergy_dataset.base import WORK_MAPPING
from synergy_dataset.base import Dataset
from synergy_dataset.base import _dataset_available
from synergy_dataset.base import _get_path_raw_dataset
from synergy_dataset.base import download_raw_dataset
from synergy_dataset.base import iter_datasets

LEGAL_NOTE = """
Due to legal constraints, paper abstracts in SYNERGY cannot be published in
plaintext. Abstracts are instead stored as an inverted index. Inverted
indexes store information about each word in a body of text, including
the number of occurrences and the position of each occurrence. Read
more:
- https://learn.microsoft.com/en-us/academic-services/graph/resources-faq
- https://docs.openalex.org/api-entities/works/work-object

For machine learning purposes, it can be helpful to convert the inverted
abstract back into plaintext locally. Keep in mind that paper abstracts
in SYNERGY cannot be published as plaintext again. Therefore you can refer
to the version of the SYNERGY dataset.

Would you like to convert the inverted abstract to plaintext?"""


def main():
    if os.getenv("SYNERGY_PATH") == "development":
        p = _get_path_raw_dataset()
        print(f"Running development version of SYNERGY dataset at {p}.")

    if len(sys.argv) == 1:
        info()
    elif sys.argv[1] == "list":
        list_datasets(sys.argv[2:])
    elif sys.argv[1] == "show":
        show_dataset(sys.argv[2:])
    elif sys.argv[1] == "get":
        build_dataset(sys.argv[2:])
    elif sys.argv[1] == "attribute":
        attribute_dataset(sys.argv[2:])
    else:
        info()


def info():
    parser = argparse.ArgumentParser(
        prog="synergy",
        description="Python package for SYNERGY dataset. "
        "Use the commands 'get', 'list', 'show' or 'attribute'.",
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


def build_dataset(argv):
    parser = argparse.ArgumentParser(
        prog="synergy",
        description="Python package for SYNERGY dataset.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="synergy_dataset",
        help="Dataset output path.",
    )
    parser.add_argument(
        "-v",
        "--vars",
        default=",".join(WORK_MAPPING),
        type=lambda x: x.split(","),
        help="The variables to include. "
        "Default '{}'.".format(",".join(WORK_MAPPING)),
    )
    parser.add_argument(
        "-d",
        "--dataset",
        nargs="*",
        default=None,
        help="Dataset name.",
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
        if user_input.lower() in ["n", "no"]:
            if _dataset_available():
                print(
                    "SYNERGY dataset already downloaded, but not",
                    "possible to build dataset (because of answer No).",
                )
            else:
                print(
                    "Downloading dataset, but not"
                    "possible to build dataset (because of answer No)."
                )
        elif user_input.lower() in ["y", "yes"]:
            args.legal = True
        else:
            print("Not a valid answer.")
            exit(1)

    # download the dataset if note available
    if not _dataset_available():
        download_raw_dataset()

    if args.legal:
        print("Building dataset")

        if Path(args.output).exists() and any(Path(args.output).iterdir()):
            print(f"Folder '{args.output}' is not empty")
            exit(1)

        # create output folder
        Path(args.output).mkdir(exist_ok=True, parents=True)

        if args.dataset is not None:
            for name in args.dataset:
                d = Dataset(name)
                result = d.to_frame()

                if args.output:
                    result.to_csv(Path(args.output, f"{name}.csv"), index=False)
        else:
            for dataset in tqdm(list(iter_datasets())):
                dataset.to_frame(args.vars).to_csv(
                    Path(args.output, f"{dataset.name}.csv"), index=False
                )


def list_datasets(argv):
    parser = argparse.ArgumentParser(
        prog="synergy",
        description="List datasets.",
    )
    parser.add_argument(
        "--tablefmt",
        default="simple",
        help="Table format.",
    )
    parser.add_argument(
        "--n-topics",
        default=3,
        type=int,
        help="The number of topics to display in the table.",
    )
    args = parser.parse_args(argv)

    # download the dataset if note available
    if not _dataset_available():
        download_raw_dataset()

    table_values = []

    n = 0
    n_incl = 0

    for i, dataset in enumerate(iter_datasets()):
        n += dataset.metadata["data"]["n_records"]
        n_incl += dataset.metadata["data"]["n_records_included"]

        concepts = dataset.metadata["data"]["concepts"]["included"]
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
            headers=["Nr", "Dataset", "Topic(s)", "Records", "Included", "%"],
            tablefmt=args.tablefmt,
            # showindex="Nr",
        ),
        "\n",
    )

    try:
        perc = f"{n_incl/n*100:.2f}"
    except ZeroDivisionError:
        perc = "NA"

    print(f"Total records = {n}, total inclusions {n_incl} ({perc}%)\n")


def show_dataset(argv):
    parser = argparse.ArgumentParser(
        prog="synergy",
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

    print(f"\n{d.cite}")

    concepts = list(
        filter(lambda x: x["level"] == 0, d.metadata["publication"]["concepts"])
    )
    concepts_str = ", ".join([x["display_name"] for x in concepts])

    print("Topics:")
    print("\t(level=0):", concepts_str)

    concepts = list(
        filter(lambda x: x["level"] != 0, d.metadata["publication"]["concepts"])
    )
    concepts_str = ", ".join([x["display_name"] for x in concepts])

    print("\t(level=1+):", concepts_str, "\n")

    print("Data for this publication can be found at:")
    if "doi" in d.metadata["data"]:
        print("https://doi.org/" + d.metadata["data"]["doi"])
    if "url" in d.metadata["data"]:
        print(d.metadata["data"]["url"])
    print("")

    try:
        print(f"This dataset is part of a collection: \n{d.cite_collection}")
    except FileNotFoundError:
        pass


def attribute_dataset(argv):
    parser = argparse.ArgumentParser(
        prog="synergy",
        description="Attribute authors of the datasets.",
    )
    parser.add_argument(
        "--format",
        default="text",
        help="Show the attribution in text or markdown. Default text.",
    )
    args = parser.parse_args(argv)

    # download the dataset if note available
    if not _dataset_available():
        download_raw_dataset()

    # without url
    if args.format == "text":
        authors = []

        for i, dataset in enumerate(iter_datasets()):
            for a in dataset.metadata["publication"]["authorships"]:
                authors.append(a["author"]["display_name"])
    elif args.format == "markdown":
        authors = []

        for i, dataset in enumerate(iter_datasets()):
            for a in dataset.metadata["publication"]["authorships"]:
                if "orcid" in a["author"] and a["author"]["orcid"]:
                    authors.append(
                        f"[{a['author']['display_name']}]({a['author']['orcid']})"
                    )
                else:
                    authors.append(a["author"]["display_name"])
    else:
        raise ValueError(f"Format not found '{args.format}'")

    print(
        "\nWe would like to thank the following authors for openly",
        "sharing the data correponding their systematic review:\n",
    )

    print(", ".join(list(set(authors))), "\n")

    print("\nReferences to datasets:\n")
    prefix = "" if args.format == "text" else "> "

    for i, dataset in enumerate(iter_datasets()):
        print(
            f"{prefix}[{dataset.metadata['key']}]",
            dataset.cite,
        )

    print(
        "\nWe thank the authors of the following collections",
        "of systematic reviews:\n",
    )

    collections = []
    for i, dataset in enumerate(iter_datasets()):
        try:
            collections.append(dataset.cite_collection)
        except FileNotFoundError:
            pass

    for c in sorted(list(set(collections))):
        print(f"{prefix}{c}")


if __name__ == "__main__":
    main()
