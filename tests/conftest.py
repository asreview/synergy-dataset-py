import pytest

import synergy_dataset.base as _base

# Default all tests to the stable 'synergy' dataset. The synergy+ DOI is not
# yet registered on Dataverse, so synergy+ tests are marked xfail and opt in
# via the ``synergy_plus`` fixture.
_base.SYNERGY_SET = "synergy"


@pytest.fixture()
def synergy_plus(monkeypatch):
    """Switch the active dataset to 'synergy+' for a single test."""
    monkeypatch.setattr(_base, "SYNERGY_SET", "synergy+")
