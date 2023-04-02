import os
from pathlib import Path

RELEASE_VERSION = os.getenv("SYNERGY_VERSION") if os.getenv("SYNERGY_VERSION") else "v0.2"
RELEASE_URL = f"https://github.com/asreview/systematic-review-datasets/archive/refs/tags/release/{RELEASE_VERSION}.zip"  # noqa
DOWNLOAD_PATH = Path("tmp", "synergy_dataset_raw")

if os.getenv("SYNERGY_PATH") and os.getenv("SYNERGY_PATH") == "development":
    SYNERGY_PATH = Path(__file__).parent.parent.parent / "synergy-release-abstracts"
    print("Running development version of SYNERGY dataset.")
elif os.getenv("SYNERGY_PATH"):
    SYNERGY_PATH = os.getenv("SYNERGY_PATH")
else:
    SYNERGY_PATH = Path(
        DOWNLOAD_PATH, f"systematic-review-datasets-release-{RELEASE_VERSION}"
    )

WORK_MAPPING = ["doi", "title", "abstract"]
