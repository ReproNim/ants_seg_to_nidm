"""The label protocol has to reach the name lookup, and the default protocol
has to refuse a segmentation numbered in the other one.

Under the default ``freesurfer`` protocol most Neuromorphometrics subcortical
ids still find a FreeSurfer LUT entry with data elements (59 -> Right-
Substancia-Nigra, 38 -> Left-Porg), so a fusion segmentation converted
without --label-protocol used to fail only on the few ids that happen to lack
one (23, 75, 76). Drop those rows and the run exited 0 with every subcortical
structure misnamed.
"""
import json
from pathlib import Path

import pytest
from rdflib import Graph, URIRef

from ants_seg_to_nidm import antsutils
from ants_seg_to_nidm.antsutils import (
    ANTSDKT,
    FREESURFER,
    FREESURFER_IDS,
    NEUROMORPHOMETRICS,
    get_id_to_struct,
    read_ants_stats,
)
from conftest import EXAMPLES_DIR, run_main

IMAGE = EXAMPLES_DIR / "antsBrainSegmentation.nii.gz"
NM_TABLE = {int(k): v for k, v in
            json.loads(Path(antsutils.neuromorphometrics_file).read_text()).items()}
# Cortical DKT ids are shared by both protocols.
CORTICAL = [1002, 1003, 1035, 2002, 2003, 2035]


def _cde_id(structure, hemi, measure, unit):
    cdes = json.loads(Path(antsutils.cde_file).read_text())
    return cdes[str(ANTSDKT(structure=structure, hemi=hemi, measure=measure, unit=unit))]["id"]


def _write_stats(tmp_path, label_ids):
    labelstats = tmp_path / "antslabelstats.csv"
    labelstats.write_text("Label,VolumeInVoxels\n" +
                          "".join(f"{i},100\n" for i in label_ids))
    brainvols = tmp_path / "antsbrainvols.csv"
    brainvols.write_text("BVOL,LABELVOL\n1677000.0,904000.0\n")
    return labelstats, brainvols


def test_fusion_stats_without_the_flag_raise_even_without_23_75_76(tmp_path):
    fusion_ids = sorted(set(NM_TABLE) - {23, 75, 76}) + CORTICAL
    labelstats, brainvols = _write_stats(tmp_path, fusion_ids)

    with pytest.raises(ValueError) as err:
        read_ants_stats(labelstats, brainvols, IMAGE, force_error=True)
    message = str(err.value)
    assert "--label-protocol" in message
    assert "59" in message and "38" in message

    # --collect-missing must not turn it into a skipped row
    with pytest.raises(ValueError, match="--label-protocol"):
        read_ants_stats(labelstats, brainvols, IMAGE, force_error=False,
                        collect_missing=True)

    # The same file is accepted under the protocol it is numbered in.
    measures = read_ants_stats(labelstats, brainvols, IMAGE,
                               label_protocol=NEUROMORPHOMETRICS)
    assert len(measures) == 2 + len(fusion_ids)


def test_every_accepted_freesurfer_id_has_a_name():
    for label_id in FREESURFER_IDS:
        assert get_id_to_struct(label_id, protocol=FREESURFER) is not None, label_id


def test_quick_template_and_example_ids_are_accepted(tmp_path):
    # The ids the pre-fused OASIS-30 template map (--method quick) carries,
    # and the mindboggle example's FreeSurfer-numbered labels.
    quick = {4, 5, 6, 7, 10, 11, 12, 13, 14, 15, 16, 17, 18, 24, 26, 28, 30, 43,
             44, 45, 46, 49, 50, 51, 52, 53, 54, 58, 60, 91, 92, 630, 631, 632}
    quick |= {h + i for h in (1000, 2000) for i in range(2, 36) if i not in (4, 32, 33)}
    assert quick <= FREESURFER_IDS
    import csv
    with open(EXAMPLES_DIR / "antslabelstats.csv") as fp:
        example = {int(row["Label"]) for row in csv.DictReader(fp)}
    assert example <= FREESURFER_IDS


def test_read_ants_stats_names_59_with_the_requested_protocol(tmp_path):
    labelstats, brainvols = _write_stats(tmp_path, [59])
    measures = dict(read_ants_stats(labelstats, brainvols, IMAGE,
                                    label_protocol=NEUROMORPHOMETRICS))
    assert _cde_id("Right-Thalamus-Proper", "Right", "Volume", "mm^3") in measures

    with pytest.raises(ValueError, match="--label-protocol"):
        read_ants_stats(labelstats, brainvols, IMAGE)


def _stats_arg(tmp_path, label_ids):
    labelstats, brainvols = _write_stats(tmp_path, label_ids)
    return f"{labelstats},{brainvols},{IMAGE}"


def test_cli_passes_label_protocol_through(tmp_path, monkeypatch):
    out = tmp_path / "out" / "nidm.ttl"
    code = run_main(monkeypatch, ["-f", _stats_arg(tmp_path, [59]), "-subjid", "sub-01",
                                  "-o", str(out), "--label-protocol", NEUROMORPHOMETRICS])
    assert code == 0
    graph = Graph().parse(str(out), format="turtle")
    thalamus = URIRef(str(antsutils_ants_ns()) + "ants_" +
                      _cde_id("Right-Thalamus-Proper", "Right", "Volume", "mm^3"))
    assert any(p == thalamus for _, p, _ in graph)


def test_cli_default_protocol_refuses_a_neuromorphometrics_id(tmp_path, monkeypatch, capsys):
    out = tmp_path / "out" / "nidm.ttl"
    with pytest.raises(ValueError, match="--label-protocol"):
        run_main(monkeypatch, ["-f", _stats_arg(tmp_path, [59]), "-subjid", "sub-01",
                               "-o", str(out)])
    assert not out.exists()


def antsutils_ants_ns():
    from ants_seg_to_nidm._pynidm import Constants
    return Constants.ANTS


def _accepted_ids_without_data_elements():
    cdes = json.loads(Path(antsutils.cde_file).read_text())
    missing = set()
    for label_id in FREESURFER_IDS:
        name = get_id_to_struct(label_id, protocol=FREESURFER)
        hemi, _, _ = antsutils.get_details("VolumeInVoxels", name)
        if str(ANTSDKT(structure=name, hemi=hemi, measure="Volume", unit="mm^3")) not in cdes:
            missing.add(label_id)
    return missing


def test_help_says_which_accepted_ids_still_fail(monkeypatch, capsys):
    """The default protocol accepts 12 aseg.stats ids that have no data
    elements; a run with one of them still fails, and --help says so."""
    assert _accepted_ids_without_data_elements() == {8, 77, 78, 79, 80, 81, 82,
                                                     251, 252, 253, 254, 255}
    monkeypatch.setattr("sys.argv", ["antsegstats2nidm", "-h"])
    from ants_seg_to_nidm import ants_seg_to_nidm as cli
    with pytest.raises(SystemExit):
        cli.main()
    text = " ".join(capsys.readouterr().out.split())
    assert "8, 77-82 and 251-255 have no data elements and still fail" in text


def test_refusal_names_a_remedy_only_for_ids_it_fits(tmp_path):
    """2/3/41/42 are FreeSurfer's own cerebral white matter and cortex ids,
    which aseg.stats does not report; the message must not call them
    non-FreeSurfer, and must offer --label-protocol neuromorphometrics only
    for ids that protocol names (41 and 59), never for 2, 3, 42."""
    labelstats, brainvols = _write_stats(tmp_path, [2, 3, 17, 41, 42, 59])

    with pytest.raises(ValueError) as err:
        read_ants_stats(labelstats, brainvols, IMAGE)

    message = " ".join(str(err.value).split())
    assert "not in the FreeSurfer" not in message
    assert "not among the ids the 'freesurfer' label protocol accepts" in message
    assert ("41, 59 are subcortical ids of the Neuromorphometrics protocol "
            "(ants-nidm --method fusion); if the segmentation follows that protocol, "
            "--label-protocol neuromorphometrics names them") in message
    assert "2, 3, 42 are accepted under neither protocol" in message
    assert " 17" not in message
