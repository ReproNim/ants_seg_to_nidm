"""The data element files shipped in mapping_data.

PyNIDM reads ants_cde.ttl from this repository to resolve the measures in
ANTs NIDM output, and drops any measure whose data element it cannot find,
so the TTL must say exactly what ants-cdes.json says.
"""
import json
import re
from pathlib import Path

from rdflib import Graph
from rdflib.compare import isomorphic, to_isomorphic, graph_diff

from ants_seg_to_nidm import antsutils
from ants_seg_to_nidm.antsutils import create_cde_graph, read_ants_stats
from conftest import EXAMPLES_DIR, TRACKED_CDE_FILE

COMMITTED_TTL = TRACKED_CDE_FILE.parent / "ants_cde.ttl"


def test_committed_ttl_is_the_graph_of_the_committed_json():
    # cde_file_copy points create_cde_graph at a copy of the tracked JSON
    assert Path(antsutils.cde_file).read_bytes() == TRACKED_CDE_FILE.read_bytes()
    committed = Graph().parse(str(COMMITTED_TTL), format="turtle")
    generated = create_cde_graph()
    if not isomorphic(committed, generated):
        _, only_committed, only_generated = graph_diff(to_isomorphic(committed),
                                                       to_isomorphic(generated))
        raise AssertionError(
            f"{COMMITTED_TTL.name} differs from create_cde_graph(): "
            f"{len(only_committed)} triples only in the file, "
            f"{len(only_generated)} only in the generated graph; regenerate it"
        )


def test_count_is_the_highest_id():
    cdes = json.loads(TRACKED_CDE_FILE.read_text())
    ids = [int(v["id"]) for k, v in cdes.items() if k != "count"]
    assert len(ids) == len(set(ids))
    assert cdes["count"] == max(ids)


def test_every_cortical_parcel_has_volume_and_area_elements():
    cdes = json.loads(TRACKED_CDE_FILE.read_text())
    measures = {}
    for key in cdes:
        match = re.match(r"ANTSDKT\(structure='(ctx-[lr]h-\w+)', hemi='\w+', measure='(\w+)'", key)
        if match:
            measures.setdefault(match.group(1), set()).add(match.group(2))
    assert len(measures) == 66
    missing = {s: m for s, m in measures.items()
               if m != {"VolumeInVoxels", "Volume", "SurfaceAreaInMillimetersSquared"}}
    assert not missing


def test_temporal_and_frontal_pole_areas_convert(tmp_path):
    labelstats = tmp_path / "antslabelstats.csv"
    labelstats.write_text("Label,VolumeInVoxels,SurfaceAreaInMillimetersSquared\n"
                          "1033,100,50.0\n2032,100,50.0\n2033,100,50.0\n")
    brainvols = tmp_path / "antsbrainvols.csv"
    brainvols.write_text("BVOL,LABELVOL\n1677000.0,904000.0\n")
    read_ants_stats(labelstats, brainvols, EXAMPLES_DIR / "antsBrainSegmentation.nii.gz",
                    force_error=True)
