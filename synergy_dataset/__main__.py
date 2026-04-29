import argparse
import csv
import os
import sys
from pathlib import Path

from tabulate import tabulate
from tqdm import tqdm

from synergy_dataset._version import __version__
from synergy_dataset.base import Dataset
from synergy_dataset.base import _dataset_available
from synergy_dataset.base import _get_path_raw_dataset
from synergy_dataset.base import download_raw_dataset
from synergy_dataset.base import iter_datasets
from synergy_dataset.extractors import DEFAULT_VARS
from synergy_dataset.extractors import WORK_EXTRACTORS
from synergy_dataset.models import WorkModel
from synergy_dataset.splits import SPLITS

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
        version=f"%(prog)s {__version__}",
    )

    args, _ = parser.parse_known_args()

    parser.print_usage()


def _count_filtered_inclusions(dataset, filter_kwargs):
    return sum(label for _, label in dataset.iter(validate=False, **filter_kwargs))


def _write_review_metadata(
    datasets, active_vars, filter_kwargs, min_inclusions, output_path
):
    """Write review_metadata.csv combining metadata.json and metadata_publication.json.

    Counts n_records and n_records_included by iterating with the active filters
    so the numbers reflect the same subset that was exported per dataset.
    """
    extractors = {v: WORK_EXTRACTORS[v] for v in active_vars}
    split_lookup = {name: i + 1 for i, fold in enumerate(SPLITS) for name in fold}

    fieldnames = [
        "key",
        "split",
        "data_doi",
        "n_records",
        "n_records_included",
        "eligibility_criteria",
    ] + list(extractors)

    out = output_path / "review_metadata.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for dataset in datasets:
            # Recount filtered records
            n_records = 0
            n_included = 0
            for _, label in dataset.iter(validate=False, **filter_kwargs):
                n_records += 1
                n_included += label

            if n_included < min_inclusions:
                continue

            # Load the review paper as a WorkModel and apply extractors
            pub_work = WorkModel.model_validate(dataset.metadata["publication"])
            pub_fields = {field: fn(pub_work) for field, fn in extractors.items()}

            row = {
                "key": dataset.name,
                "split": split_lookup.get(dataset.name),
                "data_doi": dataset.metadata.get("data", {}).get("doi"),
                "n_records": n_records,
                "n_records_included": n_included,
                "eligibility_criteria": dataset.metadata.get("publication", {}).get(
                    "eligibility_criteria"
                ),
            }
            row.update(pub_fields)
            writer.writerow(row)


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
        type=lambda x: x if x == "extended" else x.split(","),
        help="The variables to include. "
        'Always included: "openalex_id", "doi", "pmid", "lens_id", "title",'
        ' "abstract", "label_included", and "label_abstract_included"'
        '\n"extended": Include each OpenAlex field.'
        "\nThe following additional variables are available: {}".format(
            ", ".join(
                k for k in WORK_EXTRACTORS.keys() if k not in ("title", "abstract")
            )
        ),
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
    parser.add_argument(
        "--no-abstract-filter",
        dest="require_abstract",
        default=True,
        action="store_false",
        help="Include works without an abstract (SYNERGY+ only).",
    )
    parser.add_argument(
        "--no-oa-filter",
        dest="require_open_access",
        default=True,
        action="store_false",
        help="Include closed-access works (SYNERGY+ only).",
    )
    parser.add_argument(
        "--no-cleaned-abstracts",
        dest="use_cleaned_abstracts",
        default=True,
        action="store_false",
        help=(
            "Use the original abstract inverted index instead of the "
            "cleaned version (SYNERGY+ only)."
        ),
    )
    parser.add_argument(
        "--min-inclusions",
        dest="min_inclusions",
        type=int,
        default=2,
        help="Minimum number of included records required to output a dataset "
        "(default: 2).",
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

        filter_kwargs = dict(
            require_abstract=args.require_abstract,
            require_open_access=args.require_open_access,
            use_cleaned_abstracts=args.use_cleaned_abstracts,
        )

        if args.vars is None:
            active_vars = DEFAULT_VARS
        elif args.vars == "extended":
            active_vars = list(WORK_EXTRACTORS)
        else:
            active_vars = list(args.vars)

        if args.dataset is not None:
            datasets = [Dataset(name) for name in args.dataset]
            for d in datasets:
                if _count_filtered_inclusions(d, filter_kwargs) < args.min_inclusions:
                    continue
                d.to_frame(args.vars, **filter_kwargs).to_csv(
                    Path(args.output, f"{d.name}.csv"), index=False
                )
        else:
            datasets = list(iter_datasets())
            for dataset in tqdm(datasets):
                if (
                    _count_filtered_inclusions(dataset, filter_kwargs)
                    < args.min_inclusions
                ):
                    continue
                dataset.to_frame(args.vars, **filter_kwargs).to_csv(
                    Path(args.output, f"{dataset.name}.csv"), index=False
                )

        _write_review_metadata(
            datasets, active_vars, filter_kwargs, args.min_inclusions, Path(args.output)
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

        pub_meta = dataset.metadata["publication"]
        if "topics" in pub_meta:
            # Group fields by domain: "Medicine: Oncology Cardiology, ..."
            domain_fields = {}
            for t in pub_meta["topics"]:
                domain = t.get("domain", {}).get("display_name", "Unknown")
                field = t.get("field", {}).get("display_name")
                if field and field not in domain_fields.get(domain, []):
                    domain_fields.setdefault(domain, []).append(field)
            n_domains = args.n_topics if args.n_topics != -1 else len(domain_fields)
            parts = [
                f"{d}: {' '.join(fs)}"
                for d, fs in list(domain_fields.items())[:n_domains]
            ]
            concepts_str = "\n".join(parts) if parts else "(not available)"
        elif "concepts" in pub_meta:
            concepts = [
                x["display_name"] for x in pub_meta["concepts"] if x["level"] == 0
            ]
            n_topics = args.n_topics if args.n_topics != -1 else len(concepts)
            concepts_str = (
                ", ".join(concepts[:n_topics]) if concepts else "(not available)"
            )
        else:
            concepts_str = "(not available)"
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
        perc = f"{n_incl / n * 100:.2f}"
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

    pub_meta = d.metadata["publication"]

    if "topics" in pub_meta:
        # SYNERGY+: each topic has domain/field/subfield/display_name
        topics_list = pub_meta["topics"]
        domains = list(
            dict.fromkeys(
                t["domain"]["display_name"] for t in topics_list if "domain" in t
            )
        )
        fields = list(
            dict.fromkeys(
                t["field"]["display_name"] for t in topics_list if "field" in t
            )
        )
        subfields = list(
            dict.fromkeys(
                t["subfield"]["display_name"] for t in topics_list if "subfield" in t
            )
        )
        topics = list(dict.fromkeys(t["display_name"] for t in topics_list))
        print("Topics:")
        print("\t domain:  ", ", ".join(domains))
        print("\t field:   ", ", ".join(fields))
        print("\t subfield:", ", ".join(subfields))
        print("\t topic:   ", ", ".join(topics), "\n")

    elif "concepts" in pub_meta:
        # Legacy SYNERGY: level=0 are primary topics, level!=0 are subtopics
        top = [x["display_name"] for x in pub_meta["concepts"] if x["level"] == 0]
        sub = [x["display_name"] for x in pub_meta["concepts"] if x["level"] != 0]
        print("Topics (concepts):")
        print("\t(level=0):", ", ".join(top))
        print("\t(level=1+):", ", ".join(sub), "\n")
    else:
        print("Topics: (not available)\n")

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

        for dataset in iter_datasets():
            for a in dataset.metadata["publication"]["authorships"]:
                authors.append(a["author"]["display_name"])
    elif args.format == "markdown":
        authors = []

        for dataset in iter_datasets():
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

    for dataset in iter_datasets():
        print(
            f"{prefix}[{dataset.metadata['key']}]",
            dataset.cite,
        )

    print(
        "\nWe thank the authors of the following collections",
        "of systematic reviews:\n",
    )

    collections = []
    for dataset in iter_datasets():
        try:
            collections.append(dataset.cite_collection)
        except FileNotFoundError:
            pass

    for c in sorted(list(set(collections))):
        print(f"{prefix}{c}")


if __name__ == "__main__":
    main()
