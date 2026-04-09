import csv
import glob
import json
import os
import zipfile
from collections import Counter
from io import BytesIO
from pathlib import Path

import pandas as pd
import requests
import requests_cache
from pyalex import Work

from synergy_dataset.extractors import DEFAULT_VARS
from synergy_dataset.extractors import WORK_EXTRACTORS

SYNERGY_VERSION = (
    os.getenv("SYNERGY_VERSION") if os.getenv("SYNERGY_VERSION") else "1.0"
)
SYNERGY_PATH = os.getenv("SYNERGY_PATH")
SYNERGY_SET = os.getenv("SYNERGY_SET", "synergy+")
SYNERGY_ROOT = Path("~", ".synergy_dataset_source").expanduser()

# Initialize requests-cache with a 24-hour expiration
requests_cache.install_cache("synergy_cache", expire_after=24 * 60 * 60)


def _get_path_raw_dataset(version=None):
    if SYNERGY_PATH and SYNERGY_PATH == "development":
        return Path(__file__).parent.parent.parent / "synergy-release"
    elif SYNERGY_PATH:
        return Path(SYNERGY_PATH).expanduser()
    else:
        version = SYNERGY_VERSION if version is None else version
        if SYNERGY_SET == "synergy+":
            return Path(SYNERGY_ROOT, f"synergy-plus-dataset-{version}")
        else:
            return Path(SYNERGY_ROOT, f"synergy-dataset-{version}")


def _get_download_url(version=None, source="dataverse"):
    if version is None:
        version = SYNERGY_VERSION

    # TODO: Set the URL to the correct version once the DOI for 
    # synergy+ is registered on dataverse.
    if SYNERGY_SET == "synergy+":
        if source == "dataverse":
            return f"https://dataverse.nl/api/access/dataset/:persistentId/versions/{version}?persistentId=doi:PLACEHOLDER_SYNERGY_PLUS"  # noqa
        elif source == "github":
            return f"https://github.com/asreview/synergy-dataset-plus/archive/refs/tags/v{version}.zip"  # noqa
        else:
            raise ValueError("Unknown source")
    else:
        if source == "dataverse":
            return f"https://dataverse.nl/api/access/dataset/:persistentId/versions/{version}?persistentId=doi:10.34894/HE6NAQ"  # noqa
        elif source == "github":
            return f"https://github.com/asreview/synergy-dataset/archive/refs/tags/v{version}.zip"  # noqa
        else:
            raise ValueError("Unknown source")


def _dataset_available(version=SYNERGY_VERSION):
    """Check if the dataset is available.

    Args:
        version (str, optional): The version of the dataset to download.

    Returns:
        bool: True if the dataset is available
    """
    return _get_path_raw_dataset(version=version).exists()


def download_raw_dataset(url=None, path=SYNERGY_ROOT, version=None, source="dataverse"):
    """Download the raw dataset from the SYNERGY repository.

    Args:
        url (str, optional): URL to the SYNERGY dataset.
        Defaults to latest github release.
        path (str, optional): Path to download the dataset to.
        Defaults to ~/.synergy_dataset_source.
        version (str, optional): The version of the dataset to download.
        source (str, optional): The source to download (github, dataverse).
        Default dataverse.
    """
    if url is None:
        url = _get_download_url(version=version, source=source)

    print(f"Downloading version {SYNERGY_VERSION} of the SYNERGY dataset...")

    response = requests.get(url)
    response.raise_for_status()

    release_zip = zipfile.ZipFile(BytesIO(response.content))
    release_zip.extractall(path=path)

    # hack because the version on dataverse has a v prefix
    for f in Path(path).iterdir():
        if f.is_dir() and f.name.startswith("synergy-dataset-v"):
            os.rename(f, str(f).replace("synergy-dataset-v", "synergy-dataset-"))


def download_raw_subset(name, path=SYNERGY_ROOT, version=None):
    """Download the raw dataset from the SYNERGY repository.

    Args:
        url (str, optional): URL to the SYNERGY dataset.
        Defaults to latest github release.
        path (str, optional): Path to download the dataset to.
        Defaults to ~/.synergy_dataset_source.
        version (str, optional): The version of the dataset to download.
        source (str, optional): The source to download (github, dataverse).
        Default dataverse.
    """

    version = SYNERGY_VERSION if version is None else version
    url_list = f"https://dataverse.nl/api/datasets/:persistentId/versions/{version}?persistentId=doi:10.34894/HE6NAQ"  # noqa

    r = requests.get(url_list)
    file_list = r.json()["data"]["files"]

    files_subset = filter(
        lambda x: x["directoryLabel"] == f"synergy-dataset-v1.0/{name}", file_list
    )
    ids = ",".join(str(x["dataFile"]["id"]) for x in files_subset)

    url_download = f"https://dataverse.nl/api/access/datafiles/{ids}"
    download_raw_dataset(url=url_download, path=path)


def iter_datasets(path=None, version=None):
    """Iterate over the available datasets.

    Args:
        path (str, optional): Path to download the dataset to.
        Defaults to ~/.synergy_dataset_source.
        version (str, optional): The version of the dataset to download.

    Yields:
        Dataset: Dataset object
    """
    version = SYNERGY_VERSION if version is None else version

    if path is None and not _dataset_available():
        download_raw_dataset(version=version)
        path = _get_path_raw_dataset(version=version)
    elif path is None and _dataset_available():
        path = _get_path_raw_dataset(version=version)
    else:
        version = SYNERGY_VERSION if version is None else version
        path = Path(path, f"synergy-dataset-{version}")

    for dataset in sorted(
        glob.glob(str(Path(path, "*", "metadata.json"))),
        key=lambda x: x.lower(),
    ):
        yield Dataset(Path(dataset).parts[-2], path=Path(dataset).parent)


class Dataset:
    """Dataset object belonging to a systematic review."""

    def __init__(self, name, path=None):
        super().__init__()
        self.name = name
        self.path = path

        # create a path if not present
        self._path = path if path else Path(_get_path_raw_dataset(), self.name)

    @property
    def cite(self):
        """Citation for the publication."""
        if not hasattr(self, "_cite"):
            with open(
                Path(self._path, "CITATION.txt"),
                encoding="utf-8",
            ) as f:
                self._cite = f.read()

        return self._cite

    @property
    def cite_collection(self):
        """Citation for the collection."""
        if not hasattr(self, "_cite_collection"):
            with open(
                Path(self._path, "CITATION_collection.txt"),
                encoding="utf-8",
            ) as f:
                self._cite_collection = f.read()

        return self._cite_collection

    @property
    def metadata(self):
        """Metadata for the dataset."""
        if not hasattr(self, "_metadata"):
            with open(
                Path(self._path, "metadata.json"),
                encoding="utf-8",
            ) as f:
                self._metadata = json.load(f)
            with open(
                Path(self._path, "metadata_publication.json"),
                encoding="utf-8",
            ) as f:
                self._metadata["publication"] = json.load(f)

            try:
                with open(
                    Path(self._path, "metadata_collection.json"),
                    encoding="utf-8",
                ) as f:
                    self._metadata["collection"] = json.load(f)
            except FileNotFoundError:
                pass

        return self._metadata

    @property
    def labels(self):
        """Labels and identifiers for all works in the dataset.

        Returns:
            dict: Mapping of ``openalex_id`` to a dict with keys:

                - ``doi`` (str or None)
                - ``pmid`` (str or None)
                - ``lens_id`` (str or None)
                - ``label_included`` (int): 1 if included, 0 if excluded.
                - ``label_abstract_included`` (int or None): Abstract
                  screening label if the column exists in labels.csv,
                  otherwise None.
        """
        if not hasattr(self, "_labels"):
            self._labels = {}
            with open(
                Path(self._path, "labels.csv"),
                newline="",
                encoding="utf-8",
            ) as idfile:
                reader = csv.DictReader(idfile)
                fieldnames = reader.fieldnames or []
                has_abstract_label = "label_abstract_included" in fieldnames
                for row in reader:
                    self._labels[row["openalex_id"].lower()] = {
                        "doi": row.get("doi") or None,
                        "pmid": row.get("pmid") or None,
                        "lens_id": row.get("lens_id") or None,
                        "label_included": int(row["label_included"]),
                        "label_abstract_included": (
                            int(row["label_abstract_included"])
                            if has_abstract_label
                            and row.get("label_abstract_included", "") != ""
                            else None
                        ),
                    }

        return self._labels

    def iter(
        self,
        included_only=False,
        excluded_only=False,
        years=None,
        validate=True,
    ):
        """Iterate over the works in the dataset.

        Args:
            included_only (bool): If True, yield only included works
                (label_included == 1). Default False.
            excluded_only (bool): If True, yield only excluded works
                (label_included == 0). Default False.
            years (tuple, optional): A (start, end) tuple to filter works
                by publication year (inclusive). Default None (no filter).
            validate (bool): If True (default), validate each work against
                the OpenAlex schema using Pydantic. Set to False to skip
                validation for performance-sensitive pipelines. Requires
                ``pydantic`` (``pip install synergy-dataset[validation]``).

        Yields:
            tuple: (work, label_included) where work is a WorkModel
                (validate=True) or pyalex.Work (validate=False), and
                label_included is an int (0 or 1).
        """
        if validate:
            from synergy_dataset.models import WorkModel

        p_zipped_works = str(Path(self._path, "works_*.zip"))

        for f_work in glob.glob(p_zipped_works):
            with zipfile.ZipFile(f_work, "r") as z:
                for work_set in z.namelist():
                    with z.open(work_set) as f:
                        works = json.loads(f.read())

                        for w in works:
                            label_info = self.labels[w["id"].lower()]
                            label_included = label_info["label_included"]

                            if included_only and label_included != 1:
                                continue
                            if excluded_only and label_included != 0:
                                continue

                            if years is not None:
                                pub_year = w.get("publication_year")
                                if pub_year is None:
                                    continue
                                if not (years[0] <= pub_year <= years[1]):
                                    continue

                            work = WorkModel(**w) if validate else Work(w)
                            yield work, label_included

    def to_dict(self, vars=None):
        """Export the dataset to a dictionary.

        The base record for each work always contains the fields from
        ``labels.csv``: ``doi``, ``pmid``, ``lens_id``, ``label_included``,
        and ``label_abstract_included`` (when available).

        Args:
            vars (list, str, or None): Work-derived fields to include on top
                of the base record.

                - ``None`` (default): adds ``title`` and ``abstract``
                  (reconstructed from the inverted index).
                - A list of field names from ``WORK_EXTRACTORS``, e.g.
                  ``["cited_by_count", "author_names"]``.
                - ``"extended"``: includes every field in ``WORK_EXTRACTORS``.

                Run ``list(WORK_EXTRACTORS)`` to see all available names.

        Returns:
            dict: Mapping of ``openalex_id`` to record dict.
        """
        if vars is None:
            active_vars = DEFAULT_VARS
        elif vars == "extended":
            active_vars = list(WORK_EXTRACTORS)
        else:
            unknown = [v for v in vars if v not in WORK_EXTRACTORS]
            if unknown:
                raise ValueError(
                    f"Unknown vars: {unknown!r}. Available: {list(WORK_EXTRACTORS)}"
                )
            active_vars = list(vars)

        # Pre-build records from labels so every openalex_id is represented
        # even if the corresponding work JSON is missing. Column order follows
        # the user-visible default: ids → work fields → label columns.
        records = {}
        for openalex_id, label_info in self.labels.items():
            record = {
                "doi": label_info["doi"],
                "pmid": label_info["pmid"],
                "lens_id": label_info["lens_id"],
            }
            for field in active_vars:
                record[field] = None
            record["label_included"] = label_info["label_included"]
            if label_info["label_abstract_included"] is not None:
                record["label_abstract_included"] = label_info[
                    "label_abstract_included"
                ]
            records[openalex_id] = record

        extractors = {v: WORK_EXTRACTORS[v] for v in active_vars}
        for work, _ in self.iter():
            work_id = work.id.lower()
            for field, extractor in extractors.items():
                records[work_id][field] = extractor(work)

        return records

    def to_frame(self, vars=None):
        """Export the dataset to a pandas DataFrame.

        Args:
            vars (list, str, or None): Passed directly to ``to_dict()``.
                See ``to_dict()`` for details.

        Returns:
            pandas.DataFrame: DataFrame indexed by ``openalex_id``.
        """
        try:
            df = pd.DataFrame.from_dict(self.to_dict(vars=vars), orient="index")
            df.index.name = "openalex_id"
            return df.reset_index()
        except NameError as err:
            raise ImportError("Install pandas to export to pandas.DataFrame") from err

    def summary(self):
        """Return summary statistics for the dataset.

        Returns:
            dict: A dictionary with the following keys:

                - ``name`` (str): Dataset name.
                - ``n_total`` (int): Total number of works.
                - ``n_included`` (int): Number of included works.
                - ``n_excluded`` (int): Number of excluded works.
                - ``inclusion_rate`` (float): Fraction of included works.
                - ``year_range`` (tuple or None): (min_year, max_year) of
                  publication years, or None if no years are available.
                - ``languages`` (Counter): Language code -> count.
                - ``primary_topics`` (list): Top 5 primary topic names.
        """
        n_included = sum(1 for v in self.labels.values() if v["label_included"] == 1)
        n_total = len(self.labels)
        n_excluded = n_total - n_included
        inclusion_rate = n_included / n_total if n_total > 0 else 0.0

        years = []
        languages = Counter()
        topic_counter = Counter()

        for work, _ in self.iter(validate=False):
            year = work["publication_year"]
            if year is not None:
                years.append(year)

            lang = work.get("language")
            if lang:
                languages[lang] += 1

            primary_topic = work.get("primary_topic")
            if primary_topic and primary_topic.get("display_name"):
                topic_counter[primary_topic["display_name"]] += 1

        year_range = (min(years), max(years)) if years else None
        primary_topics = [name for name, _ in topic_counter.most_common(5)]

        return {
            "name": self.name,
            "n_total": n_total,
            "n_included": n_included,
            "n_excluded": n_excluded,
            "inclusion_rate": inclusion_rate,
            "year_range": year_range,
            "languages": languages,
            "primary_topics": primary_topics,
        }
