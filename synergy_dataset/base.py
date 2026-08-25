import csv
import glob
import hashlib
import json
import os
import shutil
import time
import zipfile
from collections import Counter
from io import BytesIO
from pathlib import Path

import requests
import requests_cache
from pyalex import Work
from tqdm import tqdm

try:
    import pandas as pd
except ImportError:
    pass

from synergy_dataset.extractors import DEFAULT_VARS
from synergy_dataset.extractors import WORK_EXTRACTORS
from synergy_dataset.extractors import _reconstruct_abstract
from synergy_dataset.splits import TEST_SPLIT

SYNERGY_PATH = os.getenv("SYNERGY_PATH")
SYNERGY_PLUS = "synergy_plus"
SYNERGY_SET = os.getenv("SYNERGY_SET", SYNERGY_PLUS)
_DEFAULT_SYNERGY_VERSION = "2.0" if SYNERGY_SET == SYNERGY_PLUS else "1.0"
SYNERGY_VERSION = os.getenv("SYNERGY_VERSION") or _DEFAULT_SYNERGY_VERSION
SYNERGY_ROOT = Path("~", ".synergy_dataset_source").expanduser()

ABSTRACT_MIN_WORDS = 20
ABSTRACT_MIN_CHARS = 100
MIN_INCLUSIONS = 3

# Process-level cache for Dataset.counts, keyed by dataset path.
_counts_cache = {}


def _is_valid_abstract(abstract_inverted_index):
    """Return True if the reconstructed abstract meets the minimum length thresholds."""
    abstract = _reconstruct_abstract(abstract_inverted_index)
    if not abstract:
        return False
    return (
        len(abstract.split()) >= ABSTRACT_MIN_WORDS
        or len(abstract) >= ABSTRACT_MIN_CHARS
    )


def _is_json_response(response):
    """Only cache genuine API responses.

    dataverse.nl occasionally returns an HTML error page  with a 200 status.
    Without this filter that page would get cached.
    """
    return "json" in response.headers.get("Content-Type", "")


# Initialize requests-cache with a 24-hour expiration
requests_cache.install_cache(
    "synergy_cache", expire_after=24 * 60 * 60, filter_fn=_is_json_response
)

DATAVERSE_MAX_RETRIES = 3
DATAVERSE_RETRY_BACKOFF = 2  # seconds; doubled on each subsequent retry


def _get_path_raw_dataset(version=None):
    if SYNERGY_PATH and SYNERGY_PATH == "development":
        return Path(__file__).parent.parent.parent / "synergy-release"
    elif SYNERGY_PATH:
        return Path(SYNERGY_PATH).expanduser()
    else:
        version = SYNERGY_VERSION if version is None else version
        if SYNERGY_SET == SYNERGY_PLUS:
            return Path(SYNERGY_ROOT, "synergy-dataset-plus")
        else:
            return Path(SYNERGY_ROOT, f"synergy-dataset-{version}")


def _get_dataverse_doi(source_set=None):
    """Return the dataverse persistentId DOI for the given SYNERGY set.

    Args:
        source_set (str, optional): "synergy_plus" or the classic set name.
            Defaults to the SYNERGY_SET environment variable.
    """
    source_set = SYNERGY_SET if source_set is None else source_set
    return "10.34894/DDCVCV" if source_set == SYNERGY_PLUS else "10.34894/HE6NAQ"


def _get_dataverse_file_list(version=None):
    """Fetch the file listing (name, directory, size, checksum) for the
    current SYNERGY_SET's dataverse dataset version.

    Args:
        version (str, optional): The version of the dataset.

    Returns:
        list: The ``data.files`` array from the dataverse API response.
    """
    version = SYNERGY_VERSION if version is None else version
    url_list = (
        f"https://dataverse.nl/api/datasets/:persistentId/versions/{version}"
        f"?persistentId=doi:{_get_dataverse_doi()}"
    )

    last_error = None
    for attempt in range(DATAVERSE_MAX_RETRIES):
        r = requests.get(url_list)
        r.raise_for_status()
        try:
            return r.json()["data"]["files"]
        except requests.exceptions.JSONDecodeError as e:
            last_error = e
            if attempt < DATAVERSE_MAX_RETRIES - 1:
                time.sleep(DATAVERSE_RETRY_BACKOFF * (attempt + 1))
    raise last_error


def _sha1_matches(path, expected_hex):
    """Return True if the SHA-1 checksum of the file at path matches."""
    h = hashlib.sha1()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest() == expected_hex.lower()


def _fetch_binary_with_retry(url):
    """GET a binary file (zip/csv/etc.) with retry-on-transient-failure.

    Returns:
        requests.Response: the validated response.
    """
    last_error = None
    for attempt in range(DATAVERSE_MAX_RETRIES):
        try:
            with requests_cache.disabled():
                r = requests.get(url)
            r.raise_for_status()
            if r.headers.get("Content-Type", "").startswith("text/html"):
                raise requests.exceptions.RequestException(
                    f"Expected a file download but got an HTML response "
                    f"(likely a transient error page) from {url}"
                )
            return r
        except requests.RequestException as e:
            last_error = e
            if attempt < DATAVERSE_MAX_RETRIES - 1:
                time.sleep(DATAVERSE_RETRY_BACKOFF * (attempt + 1))
    raise last_error


def download_raw_dataset_plus(path=SYNERGY_ROOT, version=None):
    """Download SYNERGY+ by fetching every file individually from dataverse.

    SYNERGY+ (~11GB) is too large for dataverse's bulk "download entire
    dataset as zip" endpoint. Fetching each file through the
    single-file access endpoint sidesteps that limit entirely.

    Files that already exist locally with the expected size are skipped, so
    an interrupted or partial download can simply be re-run to pick up where
    it left off. Freshly downloaded files are checked against dataverse's
    published SHA-1 checksum.

    Args:
        path (str, optional): Directory to download the dataset into.
            Defaults to ~/.synergy_dataset_source.
        version (str, optional): The version of the dataset to download.
    """
    version = SYNERGY_VERSION if version is None else version

    dir_prefix = "synergy-dataset-plus"
    target_root = Path(path, dir_prefix)
    file_list = _get_dataverse_file_list(version=version)
    files = [
        f for f in file_list if f.get("directoryLabel", "").startswith(f"{dir_prefix}/")
    ]

    # First check if we have missing files that need to be downloaded
    to_download = []
    for f in files:
        rel_dir = f["directoryLabel"][len(dir_prefix) + 1 :]
        data_file = f["dataFile"]
        expected_size = data_file.get("filesize")
        local_file = target_root / rel_dir / data_file["filename"]

        if local_file.exists() and (
            expected_size is None or local_file.stat().st_size == expected_size
        ):
            continue
        to_download.append((rel_dir, data_file, local_file))

    if not to_download:
        return

    for rel_dir, data_file, local_file in tqdm(
        to_download, desc=f"Downloading version {version} of the SYNERGY+ dataset"
    ):
        filename = data_file["filename"]
        checksum = data_file.get("checksum") or {}

        local_file.parent.mkdir(parents=True, exist_ok=True)
        r = _fetch_binary_with_retry(
            f"https://dataverse.nl/api/access/datafile/{data_file['id']}"
        )
        local_file.write_bytes(r.content)

        if checksum.get("type") == "SHA-1" and checksum.get("value"):
            if not _sha1_matches(local_file, checksum["value"]):
                raise RuntimeError(
                    f"Checksum mismatch after downloading {rel_dir}/{filename}; "
                    "the download may be corrupted. Re-run to retry."
                )

    print(f"Downloaded {len(to_download)} file(s), {len(files)} total.")


def _get_download_url(version=None, source="dataverse"):
    if version is None:
        version = SYNERGY_VERSION

    if SYNERGY_SET == SYNERGY_PLUS:
        if source == "dataverse":
            return f"https://dataverse.nl/api/access/dataset/:persistentId/versions/{version}?persistentId=doi:{_get_dataverse_doi()}"  # noqa
        elif source == "github":
            return f"https://github.com/asreview/synergy-dataset/archive/refs/tags/synergy_plus_v{version}.zip"  # noqa
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


def _ensure_dataset_downloaded(version=None):
    """Make sure the raw dataset is present and complete before it's used.

    For the classic SYNERGY set this is the old "download once" check: if
    the target folder exists, assume it's complete. For SYNERGY+ this is
    handled by the download_raw_dataset_plus function.
    """
    if SYNERGY_SET == SYNERGY_PLUS:
        download_raw_dataset(version=version)
    elif not _dataset_available(version=version):
        download_raw_dataset(version=version)


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
    # SYNERGY+ is too large for the DataverseNL bulk dataset-zip endpoint
    if url is None and source == "dataverse" and SYNERGY_SET == SYNERGY_PLUS:
        download_raw_dataset_plus(path=path, version=version)
        return

    if url is None:
        url = _get_download_url(version=version, source=source)

    version = SYNERGY_VERSION if version is None else version
    set_name = "SYNERGY+" if SYNERGY_SET == SYNERGY_PLUS else "SYNERGY"
    print(f"Downloading version {version} of the {set_name} dataset...")

    response = _fetch_binary_with_retry(url)

    release_zip = zipfile.ZipFile(BytesIO(response.content))
    release_zip.extractall(path=path)

    for f in Path(path).iterdir():
        if f.is_dir() and f.name.startswith("synergy-dataset-v"):
            target = Path(str(f).replace("synergy-dataset-v", "synergy-dataset-"))
            if target.exists():
                for child in f.iterdir():
                    dest = target / child.name
                    if dest.exists():
                        shutil.rmtree(dest) if dest.is_dir() else dest.unlink()
                    shutil.move(str(child), str(dest))
                f.rmdir()
            else:
                os.rename(f, target)


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
    file_list = _get_dataverse_file_list(version=version)

    dir_prefix = (
        "synergy-dataset-plus"
        if SYNERGY_SET == SYNERGY_PLUS
        else "synergy-dataset-v1.0"
    )
    files_subset = filter(
        lambda x: x.get("directoryLabel", "") == f"{dir_prefix}/{name}", file_list
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

    try:
        file_list = _get_dataverse_file_list(version=version)
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
        r = _fetch_binary_with_retry(
            f"https://dataverse.nl/api/access/datafile/{match['dataFile']['id']}"
        )
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


def iter_raw_datasets(path=None, version=None, split=None):
    """Iterate over every downloaded dataset.

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
        if SYNERGY_SET != SYNERGY_PLUS:
            raise ValueError(
                "Train/test splits are only available for SYNERGY+. "
                "Set the SYNERGY_SET environment variable to 'synergy_plus' "
                "or omit the split argument."
            )
        if split not in ("train", "test"):
            raise ValueError(f"split must be 'train' or 'test', got {split!r}")
        test_names = set(TEST_SPLIT)
    else:
        test_names = None

    version = SYNERGY_VERSION if version is None else version

    if path is None:
        _ensure_dataset_downloaded(version=version)
        path = _get_path_raw_dataset(version=version)
    else:
        dir_name = (
            "synergy-dataset-plus"
            if SYNERGY_SET == SYNERGY_PLUS
            else f"synergy-dataset-{version}"
        )
        path = Path(path, dir_name)

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


def _meets_min_inclusions(dataset, min_inclusions):
    """Return True if ``dataset`` qualifies under ``min_inclusions``.

    Only meaningful for SYNERGY+, where open-access/abstract filtering can
    drop a dataset's usable inclusion count below its real total.
    """
    if SYNERGY_SET != SYNERGY_PLUS or min_inclusions is None:
        return True
    return dataset.counts[1] >= min_inclusions


def iter_datasets(path=None, version=None, split=None, min_inclusions=MIN_INCLUSIONS):
    """Iterate over the datasets that ``synergy get`` would write.

    A filtered wrapper around ``iter_raw_datasets()``: for SYNERGY+,
    datasets with fewer than ``min_inclusions`` qualifying inclusions
    (after the open-access/abstract filtering ``Dataset.iter()`` applies)
    are skipped. This matches the ``synergy get`` CLI command's default
    behavior.

    Args:
        path (str, optional): Path to download the dataset to.
            Defaults to ~/.synergy_dataset_source.
        version (str, optional): The version of the dataset to download.
        split (str, optional): If provided, yield only datasets belonging to
            the given split. One of ``"train"`` or ``"test"``. When omitted,
            all datasets are yielded. Only available for SYNERGY+.
        min_inclusions (int, optional): Minimum number of qualifying
            inclusions a SYNERGY+ dataset must have to be yielded. Defaults
            to ``MIN_INCLUSIONS`` (3). Pass ``None`` to disable filtering
            and yield every dataset (same as ``iter_raw_datasets()``).

    Yields:
        Dataset: Dataset object
    """
    for dataset in iter_raw_datasets(path=path, version=version, split=split):
        if _meets_min_inclusions(dataset, min_inclusions):
            yield dataset


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

    @property
    def counts(self):
        """Number of records and qualifying inclusions in this dataset.

        For SYNERGY+, this reflects ``iter()``'s open-access/abstract
        filtering, i.e. it counts only works that would actually be
        yielded, computed by reading through the dataset once.

        Returns:
            tuple: ``(n_records, n_included)``.
        """
        if not hasattr(self, "_counts"):
            cache_key = str(self._path)
            if cache_key in _counts_cache:
                self._counts = _counts_cache[cache_key]
            elif SYNERGY_SET == SYNERGY_PLUS:
                n_records, n_included = 0, 0
                for _, label_included in self.iter():
                    n_records += 1
                    n_included += label_included
                self._counts = (n_records, n_included)
                _counts_cache[cache_key] = self._counts
            else:
                n_records = len(self.labels)
                n_included = sum(v["label_included"] for v in self.labels.values())
                self._counts = (n_records, n_included)
                _counts_cache[cache_key] = self._counts

        return self._counts

    def iter(self):
        """Iterate over the works in the dataset.

        For SYNERGY+, only open-access works with a valid abstract
        (>= 20 words or >= 100 characters) are yielded.

        Yields:
            tuple: (work, label_included) where work is a pyalex.Work
                (dict-like), and label_included is an int (0 or 1).
        """
        is_plus = SYNERGY_SET == SYNERGY_PLUS
        p_zipped_works = str(Path(self._path, "works_*.zip"))

        for f_work in glob.glob(p_zipped_works):
            with zipfile.ZipFile(f_work, "r") as z:
                for work_set in z.namelist():
                    with z.open(work_set) as f:
                        works = json.loads(f.read())

                        for w in works:
                            label_info = self.labels[w["id"].lower()]
                            label_included = label_info["label_included"]

                            work = Work(w)

                            if is_plus:
                                aii = work.get("abstract_inverted_index_cleaned")
                                oa = work.get("open_access") or {}
                                is_oa = oa.get("is_oa", False)
                                if not is_oa or not _is_valid_abstract(aii):
                                    continue

                            yield work, label_included

    def to_dict(self, vars=None):
        """Export the dataset to a dictionary.

        The base record for each work always contains the fields from
        ``labels.csv``: ``doi``, ``lens_id``, ``label_included``, and
        ``label_abstract_included`` (when available). ``pmid`` is not
        included by default but remains available via ``Dataset.labels``.

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
        is_plus = SYNERGY_SET == SYNERGY_PLUS
        records = {} if is_plus else {k: None for k in self.labels}
        for work, _ in self.iter():
            work_id = work["id"].lower()
            label_info = self.labels[work_id]
            record = {
                "doi": label_info["doi"],
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

        for work, _ in self.iter():
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
