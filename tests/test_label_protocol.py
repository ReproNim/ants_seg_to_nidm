"""Naming a segmentation with the wrong label protocol.

The FreeSurfer aseg and Neuromorphometrics/BrainCOLOR protocols share a
numeric range and disagree about what each number means, so a lookup in the
wrong table returns a name, not an error. These tests pin the per-protocol
tables; tests/test_label_protocol_wiring.py checks that the protocol reaches
the lookup and that the default protocol refuses ids outside its vocabulary.
"""
import json
from pathlib import Path

import pytest

from ants_seg_to_nidm.antsutils import (
    FREESURFER,
    LABEL_PROTOCOLS,
    NEUROMORPHOMETRICS,
    get_id_to_struct,
    neuromorphometrics_file,
)

# Ids whose meaning differs between the two protocols, with the name each
# protocol gives them. Measured from the OASIS-TRT-20 atlases and the
# pre-fused OASIS-30 template in ants_bidsapp on 2026-09-23.
COLLIDING = {
    35: ("Left-mOg", "Brain-Stem"),
    47: ("Right-Cerebellum-Cortex", "Right-Hippocampus"),
    48: ("Right-Thalamus", "Left-Hippocampus"),
    59: ("Right-Substancia-Nigra", "Right-Thalamus-Proper"),
    40: ("Right-Cerebral-Exterior", "Right-Cerebellum-White-Matter"),
    41: ("Right-Cerebral-White-Matter", "Left-Cerebellum-White-Matter"),
    51: ("Right-Putamen", "Right-Lateral-Ventricle"),
    52: ("Right-Pallidum", "Left-Lateral-Ventricle"),
    57: ("Right-Lesion", "Right-Putamen"),
    38: ("Left-Porg", "Right-Cerebellum-Exterior"),
}


def test_default_protocol_is_unchanged():
    """Every caller that predates the protocol argument must keep its old
    behaviour, so the default has to stay FreeSurfer."""
    assert get_id_to_struct(17) == get_id_to_struct(17, protocol=FREESURFER)
    assert get_id_to_struct(17) == "Left-Hippocampus"


@pytest.mark.parametrize("label_id", sorted(COLLIDING))
def test_colliding_ids_resolve_differently_per_protocol(label_id):
    freesurfer_name, neuromorphometrics_name = COLLIDING[label_id]
    assert get_id_to_struct(label_id, protocol=FREESURFER) == freesurfer_name
    assert get_id_to_struct(label_id, protocol=NEUROMORPHOMETRICS) == neuromorphometrics_name
    assert freesurfer_name != neuromorphometrics_name


def test_neuromorphometrics_names_no_impossible_structures():
    """These FreeSurfer LUT entries are what a Neuromorphometrics segmentation
    used to be named. None of them denotes something a brain parcellation
    contains, and they are what made the mismatch visible in delivered TTL."""
    impossible = ("Porg", "Aorg", "Stellate", "F3orb", "Interior",
                  "undetermined", "Lesion", "Line-", "mOg", "pOg")
    table = json.loads(Path(neuromorphometrics_file).read_text())
    for label_id, name in table.items():
        assert not any(tok.lower() in name.lower() for tok in impossible), (
            f"{label_id} -> {name}"
        )


def test_cortical_ids_fall_through_to_freesurfer():
    """DKT cortical numbering is shared, so the Neuromorphometrics table does
    not repeat it and must defer for anything >= 1000."""
    for label_id in (1002, 1035, 2002, 2035):
        assert (
            get_id_to_struct(label_id, protocol=NEUROMORPHOMETRICS)
            == get_id_to_struct(label_id, protocol=FREESURFER)
        )


def test_unknown_subcortical_id_is_unnamed_rather_than_guessed():
    """Id 74 appears in exactly one of the twenty atlases and has no agreed
    structure. Falling back to the FreeSurfer table here would reintroduce the
    original bug, so it must come back unnamed and let the caller skip or
    raise."""
    assert get_id_to_struct(74, protocol=FREESURFER) is not None
    assert get_id_to_struct(74, protocol=NEUROMORPHOMETRICS) is None


def test_unknown_protocol_is_rejected():
    with pytest.raises(ValueError, match="unknown label protocol"):
        get_id_to_struct(17, protocol="braincolor")


def test_lut_lookup_is_exact_not_prefix():
    """The old scan used line.startswith(str(id)), so an id with no line of
    its own could pick up a longer id's name."""
    # 1 has its own line; 19 does not appear as a bare id in most LUTs but must
    # never borrow the name of 190-something.
    name_1 = get_id_to_struct(1)
    assert name_1 is not None and not name_1.startswith("ctx")
    for absent in (9999999,):
        assert get_id_to_struct(absent) is None


def test_every_protocol_is_listed():
    assert set(LABEL_PROTOCOLS) == {FREESURFER, NEUROMORPHOMETRICS}


def test_table_covers_the_atlas_vocabulary():
    """The 37 subcortical ids the OASIS-TRT-20 atlases actually emit, minus the
    single-atlas stray 74."""
    expected = {
        4, 11, 15, 23, 30, 31, 32, 35, 36, 37, 38, 39, 40, 41, 46, 47, 48, 49,
        50, 51, 52, 55, 56, 57, 58, 59, 60, 61, 62, 63, 64, 69, 71, 72, 73, 75, 76,
    }
    table = {int(k) for k in json.loads(Path(neuromorphometrics_file).read_text())}
    assert table == expected
