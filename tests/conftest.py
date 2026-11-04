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


EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples"


@pytest.fixture
def stats_files(tmp_path):
    """A minimal labelstats / brainvols / image triple for the -f option.

    Label 17 is Left-Hippocampus in the FreeSurfer aseg numbering.
    """
    labelstats = tmp_path / "antslabelstats.csv"
    labelstats.write_text("Label,VolumeInVoxels\n17,1000\n")
    brainvols = tmp_path / "antsbrainvols.csv"
    brainvols.write_text("BVOL,LABELVOL\n1677000.0,904000.0\n")
    image = EXAMPLES_DIR / "antsBrainSegmentation.nii.gz"
    return f"{labelstats},{brainvols},{image}"


def run_main(monkeypatch, argv):
    """Run the antsegstats2nidm entry point with argv; return its exit code."""
    import sys
    from ants_seg_to_nidm import ants_seg_to_nidm as cli

    monkeypatch.setattr(sys, "argv", ["antsegstats2nidm", *argv])
    try:
        cli.main()
    except SystemExit as exc:
        code = exc.code
        if code is None:
            return 0
        return code if isinstance(code, int) else 1
    return 0
