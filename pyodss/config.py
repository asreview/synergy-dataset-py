import os
from pathlib import Path

RELEASE_VERSION = os.getenv("ODSS_VERSION") if os.getenv("ODSS_VERSION") else "v0.2"
RELEASE_URL = f"https://github.com/asreview/systematic-review-datasets/archive/refs/tags/release/{RELEASE_VERSION}.zip"  # noqa
DOWNLOAD_PATH = Path("tmp", "odss")


if os.getenv("ODSS_PATH") and os.getenv("ODSS_PATH") == "development":
    ODSS_PATH = Path("..", "odss-release")
elif os.getenv("ODSS_PATH"):
    ODSS_PATH = os.getenv("ODSS_PATH")
else:
    ODSS_PATH = Path(
        DOWNLOAD_PATH, f"systematic-review-datasets-release-{RELEASE_VERSION}"
    )
