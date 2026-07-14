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
from synergy_dataset.extractors import _reconstruct_abstract
from synergy_dataset.splits import TEST_SPLIT

SYNERGY_VERSION = (
    os.getenv("SYNERGY_VERSION") if os.getenv("SYNERGY_VERSION") else "1.0"
)
SYNERGY_PATH = os.getenv("SYNERGY_PATH")
SYNERGY_SET = os.getenv("SYNERGY_SET", "synergy+")
SYNERGY_ROOT = Path("~", ".synergy_dataset_source").expanduser()

ABSTRACT_MIN_WORDS = 20
ABSTRACT_MIN_CHARS = 100


def _is_valid_abstract(abstract_inverted_index):
    """Return True if the reconstructed abstract meets the minimum length thresholds."""
    abstract = _reconstruct_abstract(abstract_inverted_index)
    if not abstract:
        return False
    return (
        len(abstract.split()) >= ABSTRACT_MIN_WORDS
        or len(abstract) >= ABSTRACT_MIN_CHARS
    )


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


def _get_dataverse_doi(source_set=None):
    """Return the dataverse persistentId DOI for the given SYNERGY set.

    Args:
        source_set (str, optional): "synergy+" or the classic set name.
            Defaults to the SYNERGY_SET environment variable.
    """
    source_set = SYNERGY_SET if source_set is None else source_set
    return "10.34894/DDCVCV" if source_set == "synergy+" else "10.34894/HE6NAQ"


def _get_download_url(version=None, source="dataverse"):
    if version is None:
        version = SYNERGY_VERSION

    if SYNERGY_SET == "synergy+":
        if source == "dataverse":
            return f"https://dataverse.nl/api/access/dataset/:persistentId/versions/{version}?persistentId=doi:{_get_dataverse_doi()}"  # noqa
        elif source == "github":
            return f"https://github.com/asreview/synergy-dataset-plus/archive/refs/tags/v{version}.zip"  # noqa
        else:
            raise ValueError("Unknown source")
    else:
        if source == "dataverse":
            return f"https://dataverse.nl/api/access/dataset/:persistentId/versions/{version}?persistentId=doi:{_get_dataverse_doi()}"  # noqa
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
    url_list = f"https://dataverse.nl/api/datasets/:persistentId/versions/{version}?persistentId=doi:{_get_dataverse_doi()}"  # noqa

    r = requests.get(url_list)
    file_list = r.json()["data"]["files"]

    dir_prefix = (
        "synergy-dataset-plus" if SYNERGY_SET == "synergy+" else "synergy-dataset-v1.0"
    )
    files_subset = filter(
        lambda x: x["directoryLabel"] == f"{dir_prefix}/{name}", file_list
    )
    ids = ",".join(str(x["dataFile"]["id"]) for x in files_subset)

    url_download = f"https://dataverse.nl/api/access/datafiles/{ids}"
    download_raw_dataset(url=url_download, path=path)


def _get_reviews_csv_path(version=None):
    """Path to reviews.csv, the review-level metadata file that lives at the
    top level of the dataverse repository (a sibling of the per-dataset
    folder returned by _get_path_raw_dataset(), not inside it)."""
    return Path(_get_path_raw_dataset(version=version)).parent / "reviews.csv"


def _reviews_csv_available(version=None):
    """Check if reviews.csv is available locally."""
    return _get_reviews_csv_path(version=version).exists()


def download_reviews_csv(path=None, version=None):
    """Download the top-level reviews.csv file from dataverse, without
    downloading the full dataset zip.

    This is a best-effort enrichment: if the file can't be found or
    downloaded (e.g. the SYNERGY+ dataset isn't registered on dataverse
    yet), a warning is printed and None is returned instead of raising.

    Args:
        path (str, optional): Directory to download reviews.csv into.
            Defaults to the parent of _get_path_raw_dataset().
        version (str, optional): The version of the dataset.

    Returns:
        Path or None: path to the downloaded reviews.csv, or None.
    """
    version = SYNERGY_VERSION if version is None else version
    target_dir = (
        Path(path)
        if path is not None
        else Path(_get_path_raw_dataset(version=version)).parent
    )

    url_list = (
        f"https://dataverse.nl/api/datasets/:persistentId/versions/{version}"
        f"?persistentId=doi:{_get_dataverse_doi()}"
    )

    try:
        r = requests.get(url_list)
        r.raise_for_status()
        file_list = r.json()["data"]["files"]
    except (requests.RequestException, KeyError, ValueError) as e:
        print(
            f"Warning: could not fetch reviews.csv listing ({e}). "
            "Continuing without additional review metadata columns."
        )
        return None

    match = next(
        (
            f
            for f in file_list
            if f.get("dataFile", {}).get("filename") == "reviews.csv"
        ),
        None,
    )
    if match is None:
        print(
            "Warning: reviews.csv not found in the dataset repository. "
            "Continuing without additional review metadata columns."
        )
        return None

    try:
        r = requests.get(
            f"https://dataverse.nl/api/access/datafile/{match['dataFile']['id']}"
        )
        r.raise_for_status()
    except requests.RequestException as e:
        print(
            f"Warning: failed to download reviews.csv ({e}). "
            "Continuing without additional review metadata columns."
        )
        return None

    target_dir.mkdir(parents=True, exist_ok=True)
    out_path = target_dir / "reviews.csv"
    with open(out_path, "wb") as f:
        f.write(r.content)
    return out_path


def iter_datasets(path=None, version=None, split=None):
    """Iterate over the available datasets.

    Args:
        path (str, optional): Path to download the dataset to.
            Defaults to ~/.synergy_dataset_source.
        version (str, optional): The version of the dataset to download.
        split (str, optional): If provided, yield only datasets belonging to
            the given split. One of ``"train"`` or ``"test"``. When omitted,
            all datasets are yielded. Only available for SYNERGY+.

    Yields:
        Dataset: Dataset object
    """
    if split is not None:
        if SYNERGY_SET != "synergy+":
            raise ValueError(
                "Train/test splits are only available for SYNERGY+. "
                "Set the SYNERGY_SET environment variable to 'synergy+' "
                "or omit the split argument."
            )
        if split not in ("train", "test"):
            raise ValueError(f"split must be 'train' or 'test', got {split!r}")
        test_names = set(TEST_SPLIT)
    else:
        test_names = None

    version = SYNERGY_VERSION if version is None else version

    if path is None and not _dataset_available():
        download_raw_dataset(version=version)
        path = _get_path_raw_dataset(version=version)
    elif path is None and _dataset_available():
        path = _get_path_raw_dataset(version=version)
    else:
        path = Path(path, f"synergy-dataset-{version}")

    for dataset in sorted(
        glob.glob(str(Path(path, "*", "metadata.json"))),
        key=lambda x: x.lower(),
    ):
        name = Path(dataset).parts[-2]
        if split == "test" and name not in test_names:
            continue
        if split == "train" and name in test_names:
            continue
        yield Dataset(name, path=Path(dataset).parent)


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
            # Preserve fields from metadata.json's publication block (e.g.
            # eligibility_criteria) before overwriting with the full OpenAlex
            # work object from metadata_publication.json.
            pub_meta_extra = self._metadata.pop("publication", {})
            with open(
                Path(self._path, "metadata_publication.json"),
                encoding="utf-8",
            ) as f:
                self._metadata["publication"] = json.load(f)
            for k, v in pub_meta_extra.items():
                self._metadata["publication"].setdefault(k, v)

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
                has_abstract_label = "label_abstract_included" in (
                    reader.fieldnames or []
                )
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

    def iter(self, validate=True):
        """Iterate over the works in the dataset.

        For SYNERGY+, only open-access works with a valid abstract
        (>= 20 words or >= 100 characters) are yielded.

        Args:
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

        is_plus = SYNERGY_SET == "synergy+"
        p_zipped_works = str(Path(self._path, "works_*.zip"))

        for f_work in glob.glob(p_zipped_works):
            with zipfile.ZipFile(f_work, "r") as z:
                for work_set in z.namelist():
                    with z.open(work_set) as f:
                        works = json.loads(f.read())

                        for w in works:
                            label_info = self.labels[w["id"].lower()]
                            label_included = label_info["label_included"]

                            work = WorkModel(**w) if validate else Work(w)

                            if is_plus:
                                if validate:
                                    aii = work.abstract_inverted_index_cleaned
                                    is_oa = (
                                        work.open_access.is_oa
                                        if work.open_access
                                        else False
                                    )
                                else:
                                    aii = work.get("abstract_inverted_index_cleaned")
                                    oa = work.get("open_access") or {}
                                    is_oa = oa.get("is_oa", False)
                                if not is_oa or not _is_valid_abstract(aii):
                                    continue

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

        extractors = {v: WORK_EXTRACTORS[v] for v in active_vars}
        # For old synergy, pre-seed all label keys in CSV order so the
        # returned dict is complete and ordered even if zip files are sparse.
        is_plus = SYNERGY_SET == "synergy+"
        records = {} if is_plus else {k: None for k in self.labels}
        for work, _ in self.iter():
            work_id = work.id.lower()
            label_info = self.labels[work_id]
            record = {
                "doi": label_info["doi"],
                "pmid": label_info["pmid"],
                "lens_id": label_info["lens_id"],
            }
            for field, extractor in extractors.items():
                record[field] = extractor(work)
            record["label_included"] = label_info["label_included"]
            if label_info["label_abstract_included"] is not None:
                record["label_abstract_included"] = label_info[
                    "label_abstract_included"
                ]
            records[work_id] = record

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
            df = pd.DataFrame.from_dict(
                self.to_dict(vars=vars),
                orient="index",
            )
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
