"""Output formats of the command line."""
import json

import pytest
from rdflib import Graph, Literal, URIRef
from rdflib.namespace import RDF

from ants_seg_to_nidm._pynidm import Constants, NIDM_SUBJECTID_URI
from conftest import run_main


@pytest.mark.parametrize("with_input", [False, True], ids=["new-file", "add-to-input"])
def test_jsonld_flag_writes_json_ld(tmp_path, monkeypatch, stats_files, with_input):
    out = tmp_path / "out" / "nidm.json"
    argv = ["-f", stats_files, "-subjid", "sub-01", "-o", str(out), "-j"]
    if with_input:
        nidm_in = tmp_path / "input.ttl"
        graph = Graph()
        agent = URIRef("http://example.org/agent/01")
        graph.add((agent, RDF.type, Constants.PROV["Agent"]))
        graph.add((agent, URIRef(NIDM_SUBJECTID_URI), Literal("01")))
        graph.serialize(destination=str(nidm_in), format="turtle")
        argv += ["--nidm", str(nidm_in)]

    assert run_main(monkeypatch, argv) == 0

    json.loads(out.read_text())
    graph = Graph().parse(str(out), format="json-ld")
    assert (None, URIRef(NIDM_SUBJECTID_URI), None) in graph


def _help(monkeypatch, capsys):
    run_main(monkeypatch, ["--help"])
    return " ".join(capsys.readouterr().out.split())


def test_help_names_brainvols_columns_that_have_data_elements(monkeypatch, capsys):
    from ants_seg_to_nidm import antsutils
    cdes = json.loads(open(antsutils.cde_file).read())
    text = _help(monkeypatch, capsys)
    for column in ("GVol", "WVol"):  # no data elements: the run would fail
        assert column not in text
        assert not any(f"structure='{column}'" in key for key in cdes)
    for column in ("BVOL", "LABELVOL"):
        assert column in text
        assert any(f"structure='{column}'" in key for key in cdes)
    assert "brain extraction mask" in text and "sum of the labelled" in text


def test_help_scopes_the_neuromorphometrics_protocol(monkeypatch, capsys):
    text = _help(monkeypatch, capsys)
    assert "BrainCOLOR protocol, as" not in text
    assert "DKT31_CMA" in text
    assert "wrapper.py" not in text
    # --forcenidm no longer adds an agent when the input has other subjects
    assert "no subject agents" in text
