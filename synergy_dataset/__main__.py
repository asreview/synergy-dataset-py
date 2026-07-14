import argparse
import csv
import os
import re
import sys
from pathlib import Path

from tabulate import tabulate
from tqdm import tqdm

from synergy_dataset._version import __version__
from synergy_dataset.base import Dataset
from synergy_dataset.base import _dataset_available
from synergy_dataset.base import _get_path_raw_dataset
from synergy_dataset.base import _get_reviews_csv_path
from synergy_dataset.base import _reviews_csv_available
from synergy_dataset.base import download_raw_dataset
from synergy_dataset.base import download_reviews_csv
from synergy_dataset.base import iter_datasets
from synergy_dataset.extractors import DEFAULT_VARS
from synergy_dataset.extractors import WORK_EXTRACTORS
from synergy_dataset.extractors import _clean_str
from synergy_dataset.extractors import _reconstruct_abstract
from synergy_dataset.models import WorkModel
from synergy_dataset.splits import TEST_SPLIT

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

MIN_INCLUSIONS = 3


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


def _count_records(dataset):
    n_records, n_included = 0, 0
    for _, label in dataset.iter(validate=False):
        n_records += 1
        n_included += label
    return n_records, n_included


def _snake_case_header(header):
    """Convert a reviews.csv column header to snake_case, e.g.
    'Paper link' -> 'paper_link', 'Ti-ab screeners' -> 'ti_ab_screeners',
    'Paper inclusion %' -> 'paper_inclusion_pct'."""
    header = header.strip().replace("%", "pct")
    header = re.sub(r"[^0-9a-zA-Z]+", "_", header)
    return header.strip("_").lower()


def _read_reviews_csv(path):
    """Read reviews.csv into {dataset_key.lower(): {snake_case_col: value}}.

    Returns {} if path is None. The join column (the one that snake-cases
    to "key") is excluded from each row's value dict.
    """
    if path is None:
        return {}

    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        snake_fields = {fn: _snake_case_header(fn) for fn in reader.fieldnames}
        key_field = next(fn for fn, s in snake_fields.items() if s == "key")

        by_key = {}
        for row in reader:
            k = (row.get(key_field) or "").strip()
            if not k:
                continue
            by_key[k.lower()] = {
                snake_fields[fn]: v for fn, v in row.items() if fn != key_field
            }

    return by_key


def _write_review_metadata(
    datasets, counts, active_vars, output_path, reviews_csv_path=None
):
    """Write review_metadata.csv combining metadata.json,
    metadata_publication.json, and (if available) reviews.csv."""
    extractors = {v: WORK_EXTRACTORS[v] for v in active_vars}
    # Publication metadata only has abstract_inverted_index (standard OpenAlex),
    # not the SYNERGY-cleaned variant, so fall back to the original here.
    if "abstract" in extractors:
        extractors["abstract"] = lambda w: _clean_str(
            _reconstruct_abstract(w.abstract_inverted_index)
        )
    test_names = set(TEST_SPLIT)
    all_names = test_names | {d.name for d in datasets}
    split_lookup = {
        name: "test" if name in test_names else "train" for name in all_names
    }

    fieldnames = [
        "key",
        "split",
        "data_doi",
        "n_records",
        "n_records_included",
        "eligibility_criteria",
    ] + list(extractors)

    reviews_by_key = _read_reviews_csv(reviews_csv_path)
    if reviews_by_key:
        # Union of reviews.csv columns (same set for every row). Columns
        # that already exist (e.g. title, eligibility_criteria) are not
        # duplicated -- they get overwritten in place below instead.
        reviews_cols = list(next(iter(reviews_by_key.values())))
        fieldnames = fieldnames + [c for c in reviews_cols if c not in fieldnames]

    out = output_path / "review_metadata.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for dataset in datasets:
            n_records, n_included = counts[dataset.name]

            if n_included < MIN_INCLUSIONS:
                continue

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

            review_extra = reviews_by_key.get(dataset.name.lower())
            if review_extra:
                # reviews.csv wins on any column collision (e.g. title,
                # eligibility_criteria).
                row.update(review_extra)

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

    args, _ = parser.parse_known_args()

    if not args.legal:
        user_input = input(f"{LEGAL_NOTE} ([Y]es,[N]o):\n")
        if user_input.lower() in ["n", "no"]:
            print("Not possible to build dataset (because of answer No).")
            return
        elif user_input.lower() in ["y", "yes"]:
            args.legal = True
        else:
            print("Not a valid answer.")
            exit(1)

    # download the dataset if not available
    if not _dataset_available():
        download_raw_dataset()

    if args.legal:
        print("Building dataset")

        if Path(args.output).exists() and any(Path(args.output).iterdir()):
            print(f"Folder '{args.output}' is not empty")
            exit(1)

        # create output folder
        Path(args.output).mkdir(exist_ok=True, parents=True)

        if args.vars is None:
            active_vars = DEFAULT_VARS
        elif args.vars == "extended":
            active_vars = list(WORK_EXTRACTORS)
        else:
            active_vars = list(args.vars)

        counts = {}

        if args.dataset is not None:
            datasets = [Dataset(name) for name in args.dataset]
            for dataset in datasets:
                n_records, n_included = _count_records(dataset)
                counts[dataset.name] = (n_records, n_included)
                if n_included < MIN_INCLUSIONS:
                    continue
                dataset.to_frame(args.vars).to_csv(
                    Path(args.output, f"{dataset.name}.csv"), index=False
                )
        else:
            datasets = list(iter_datasets())
            for dataset in tqdm(datasets):
                n_records, n_included = _count_records(dataset)
                counts[dataset.name] = (n_records, n_included)
                if n_included < MIN_INCLUSIONS:
                    continue
                dataset.to_frame(args.vars).to_csv(
                    Path(args.output, f"{dataset.name}.csv"), index=False
                )

        metadata_path = Path(args.output) / "metadata"
        metadata_path.mkdir(exist_ok=True, parents=True)

        if not _reviews_csv_available():
            if os.getenv("SYNERGY_PATH"):
                print(
                    f"Warning: reviews.csv not found at {_get_reviews_csv_path()}. "
                    "Continuing without additional review metadata columns."
                )
            else:
                download_reviews_csv()
        reviews_csv_path = (
            _get_reviews_csv_path() if _reviews_csv_available() else None
        )

        print("Writing review metadata")
        _write_review_metadata(
            datasets,
            counts,
            active_vars,
            metadata_path,
            reviews_csv_path=reviews_csv_path,
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

    # download the dataset if not available
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
                dataset.metadata["key"],
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

    # download the dataset if not available
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

    # download the dataset if not available
    if not _dataset_available():
        download_raw_dataset()

    if args.format not in ("text", "markdown"):
        raise ValueError(f"Format not found '{args.format}'")

    prefix = "" if args.format == "text" else "> "
    authors = []
    citations = []
    collections = []

    for dataset in iter_datasets():
        for a in dataset.metadata["publication"]["authorships"]:
            author = a["author"]
            if args.format == "markdown" and author.get("orcid"):
                authors.append(f"[{author['display_name']}]({author['orcid']})")
            else:
                authors.append(author["display_name"])
        citations.append((dataset.metadata["key"], dataset.cite))
        try:
            collections.append(dataset.cite_collection)
        except FileNotFoundError:
            pass

    print(
        "\nWe would like to thank the following authors for openly",
        "sharing the data corresponding their systematic review:\n",
    )
    print(", ".join(sorted(set(authors))), "\n")

    print("\nReferences to datasets:\n")
    for key, cite in citations:
        print(f"{prefix}[{key}]", cite)

    print(
        "\nWe thank the authors of the following collections",
        "of systematic reviews:\n",
    )
    for c in sorted(set(collections)):
        print(f"{prefix}{c}")


if __name__ == "__main__":
    main()
