"""BVOL is the brain-extraction mask volume; LABELVOL is the sum of the labels.

The two used to be one number: BVOL was computed as the sum of the labelled
parcels while its data element claimed it was about the brain (UBERON_0000955).
The DKT31_CMA label set has no cerebral white matter label, so that sum is
about half a brain (~0.9 L against ~1.7 L), and read as a brain volume it looks
like severe atrophy. The app now reports both, and each element must say what
it measures.
"""
import json
from pathlib import Path

from ants_seg_to_nidm import antsutils
from ants_seg_to_nidm.antsutils import ANTSDKT, read_ants_stats

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples"
BRAIN = "http://purl.obolibrary.org/obo/UBERON_0000955"
VOLUME = "http://uri.interlex.org/base/ilx_0112559"


def _cdes():
    return json.loads(Path(antsutils.cde_file).read_text())


def _key(structure):
    return str(ANTSDKT(structure=structure, hemi=None, measure="Volume", unit="mm^3"))


def test_bvol_is_about_the_brain():
    bvol = _cdes()[_key("BVOL")]
    assert bvol["id"] == "000002", "BVOL's id is in delivered NIDM; it must not move"
    assert bvol["isAbout"] == BRAIN
    assert "mask" in bvol["label"].lower()


def test_labelvol_is_a_volume_but_not_about_the_brain():
    labelvol = _cdes()[_key("LABELVOL")]
    assert labelvol.get("isAbout") != BRAIN, (
        "the label sum omits cerebral white matter; annotating it as the brain "
        "is the defect this entry exists to avoid"
    )
    assert labelvol["measureOf"] == VOLUME
    assert labelvol["hasUnit"] == "mm^3"


def test_both_volumes_convert_without_minting_elements(tmp_path):
    before = Path(antsutils.cde_file).read_bytes()
    labelstats = tmp_path / "antslabelstats.csv"
    labelstats.write_text("Label,VolumeInVoxels,Volume_mm3\n4,1000,1000.0\n")
    brainvols = tmp_path / "antsbrainvols.csv"
    brainvols.write_text("BVOL,LABELVOL\n1677000.0,904000.0\n")

    measures = dict(read_ants_stats(labelstats, brainvols,
                                    EXAMPLES_DIR / "antsBrainSegmentation.nii.gz",
                                    force_error=True))

    cdes = _cdes()
    assert measures[cdes[_key("BVOL")]["id"]] == "1677000"
    assert measures[cdes[_key("LABELVOL")]["id"]] == "904000"
    assert Path(antsutils.cde_file).read_bytes() == before
