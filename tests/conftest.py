import hashlib
import shutil
from pathlib import Path

import pytest

from ants_seg_to_nidm import antsutils

#: The data element table shipped in the package. read_ants_stats(force_error=False)
#: and create_ants_mapper() write to antsutils.cde_file, and ants_cde.ttl is
#: regenerated from this file, so a test run must never change it.
TRACKED_CDE_FILE = Path(antsutils.__file__).parent / "mapping_data" / "ants-cdes.json"


def _digest(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


@pytest.fixture(scope="session", autouse=True)
def tracked_cde_file_is_unchanged():
    before = _digest(TRACKED_CDE_FILE)
    yield
    assert _digest(TRACKED_CDE_FILE) == before, (
        f"the test suite modified {TRACKED_CDE_FILE}; tests must write to a copy"
    )


@pytest.fixture(autouse=True)
def cde_file_copy(tmp_path, monkeypatch):
    """Point the converter at a per-test copy of the data element table."""
    copy = tmp_path / "ants-cdes.json"
    shutil.copyfile(TRACKED_CDE_FILE, copy)
    monkeypatch.setattr(antsutils, "cde_file", copy)
    return copy
