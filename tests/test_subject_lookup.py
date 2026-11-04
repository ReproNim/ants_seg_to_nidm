"""Which agent in the input NIDM the stats are attached to.

The input stores ndar:src_subject_id in several spellings: PyNIDM 4.x writes
a typed "..."^^xsd:string literal and PyNIDM 5 a plain one, and the id itself
may or may not carry the BIDS "sub-" prefix and leading zeros. Both the
requested id and the stored ids are normalised the same way before they are
compared. Attaching the stats to a newly minted agent when the input already
has subjects would detach them from that subject's demographics and
acquisitions while exiting 0, so that is an error, with or without
--forcenidm.
"""
from pathlib import Path

import pytest
from rdflib import BNode, Graph, Literal, URIRef
from rdflib.namespace import RDF, XSD

from ants_seg_to_nidm._pynidm import Constants, NIDM_SUBJECTID_URI
from ants_seg_to_nidm.ants_seg_to_nidm import SubjectLookupError, add_seg_data
from conftest import run_main

SUBJECT_ID = URIRef(NIDM_SUBJECTID_URI)
SPELLINGS = ["sub-0050002", "0050002", "50002"]


class _StatEntity:
    def __init__(self, uri):
        self.uri = uri


STATS = _StatEntity("http://example.org/entity/stats")


def _graph_with_subjects(*ids, datatype=XSD.string):
    graph = Graph()
    agents = []
    for n, stored in enumerate(ids):
        agent = URIRef(f"http://example.org/agent/{n}")
        graph.add((agent, RDF.type, Constants.PROV["Agent"]))
        graph.add((agent, SUBJECT_ID, Literal(stored, datatype=datatype)))
        agents.append(agent)
    return graph, agents


def _add_acquisition(graph, agent, name):
    acquisition = URIRef(f"http://example.org/acquisition/{name}")
    activity = URIRef(f"http://example.org/activity/{name}")
    graph.add((acquisition, RDF.type, Constants.NIDM["AcquisitionObject"]))
    graph.add((acquisition, Constants.NIDM["hadAcquisitionModality"],
               Constants.NIDM["MagneticResonanceImaging"]))
    graph.add((acquisition, Constants.NIDM["hadImageUsageType"], Constants.NIDM["Anatomical"]))
    graph.add((acquisition, Constants.PROV["wasGeneratedBy"], activity))
    assoc = BNode()
    graph.add((activity, Constants.PROV["qualifiedAssociation"], assoc))
    graph.add((assoc, Constants.PROV["agent"], agent))
    return acquisition


def _subject_agent_of_stats(graph):
    """The agent the stats activity is associated with in the sio:Subject role."""
    found = set()
    for activity in graph.objects(URIRef(STATS.uri), Constants.PROV["wasGeneratedBy"]):
        for assoc in graph.objects(activity, Constants.PROV["qualifiedAssociation"]):
            if (assoc, Constants.PROV["hadRole"], Constants.SIO["Subject"]) in graph:
                found.update(graph.objects(assoc, Constants.PROV["agent"]))
    return found


@pytest.mark.parametrize("requested", SPELLINGS)
@pytest.mark.parametrize("datatype", [XSD.string, None], ids=["typed", "plain"])
@pytest.mark.parametrize("stored", SPELLINGS)
def test_every_spelling_finds_every_other(stored, datatype, requested):
    graph, (agent,) = _graph_with_subjects(stored, datatype=datatype)
    acquisition = _add_acquisition(graph, agent, "t1w")

    add_seg_data(graph, requested, STATS, add_to_nidm=True, forceagent=True)

    assert set(graph.subjects(SUBJECT_ID, None)) == {agent}
    assert _subject_agent_of_stats(graph) == {agent}
    assert (None, Constants.PROV["used"], acquisition) in graph


def test_a_person_typed_participant_is_found():
    graph = Graph()
    agent = URIRef("http://example.org/agent/person")
    graph.add((agent, RDF.type, Constants.PROV["Person"]))
    graph.add((agent, SUBJECT_ID, Literal("51456")))

    add_seg_data(graph, "sub-0051456", STATS, add_to_nidm=True, forceagent=True)

    assert set(graph.subjects(SUBJECT_ID, None)) == {agent}
    assert _subject_agent_of_stats(graph) == {agent}


def test_several_matches_is_an_error_naming_them():
    graph, agents = _graph_with_subjects("50002", "sub-0050002")
    with pytest.raises(SubjectLookupError) as err:
        add_seg_data(graph, "sub-0050002", STATS, add_to_nidm=True, forceagent=True)
    message = str(err.value)
    for agent in agents:
        assert str(agent) in message
    assert "50002" in message and "sub-0050002" in message
    assert _subject_agent_of_stats(graph) == set()


@pytest.mark.parametrize("forceagent", [True, False], ids=["forcenidm", "no-forcenidm"])
def test_no_match_among_existing_subjects_is_an_error(forceagent):
    graph, _ = _graph_with_subjects("51456", "51457")
    with pytest.raises(SubjectLookupError) as err:
        add_seg_data(graph, "sub-0050002", STATS, add_to_nidm=True, forceagent=forceagent)
    message = str(err.value)
    assert "sub-0050002" in message
    assert "51456" in message and "51457" in message
    assert set(graph.subjects(SUBJECT_ID, None)) == {
        URIRef("http://example.org/agent/0"), URIRef("http://example.org/agent/1")}


def test_forcenidm_creates_an_agent_only_when_the_input_has_no_subjects():
    graph = Graph()
    add_seg_data(graph, "sub-0050002", STATS, add_to_nidm=True, forceagent=True)
    (agent,) = set(graph.subjects(SUBJECT_ID, None))
    assert str(graph.value(agent, SUBJECT_ID)) == "sub-0050002"
    assert _subject_agent_of_stats(graph) == {agent}


def test_no_subjects_without_forcenidm_is_an_error():
    with pytest.raises(SubjectLookupError, match="--forcenidm"):
        add_seg_data(Graph(), "sub-0050002", STATS, add_to_nidm=True, forceagent=False)


def test_a_quote_in_the_requested_id_is_compared_not_parsed():
    # Built into a SPARQL string, this id made the filter true for every
    # agent and the stats landed on the last one.
    graph, _ = _graph_with_subjects("51456", "51457")
    with pytest.raises(SubjectLookupError):
        add_seg_data(graph, 'nope" || "a" = "a', STATS, add_to_nidm=True, forceagent=True)
    assert _subject_agent_of_stats(graph) == set()


def test_a_quote_in_a_stored_id_matches_itself():
    graph, (agent, _) = _graph_with_subjects('51"456', "51457")
    add_seg_data(graph, 'sub-51"456', STATS, add_to_nidm=True, forceagent=True)
    assert _subject_agent_of_stats(graph) == {agent}


def _write_input_nidm(path, *ids):
    graph, _ = _graph_with_subjects(*ids, datatype=None)
    graph.serialize(destination=str(path), format="turtle")
    return path


def test_cli_exits_nonzero_and_writes_nothing_when_the_subject_is_absent(
        tmp_path, monkeypatch, stats_files, capsys):
    nidm_in = _write_input_nidm(tmp_path / "input.ttl", "51456")
    out = tmp_path / "out" / "nidm.ttl"

    code = run_main(monkeypatch, ["-f", stats_files, "-subjid", "sub-0050002",
                                  "-o", str(out), "--nidm", str(nidm_in), "--forcenidm"])

    assert code != 0
    assert not out.exists()
    assert not (out.parent / "ants_cde.ttl").exists()
    err = capsys.readouterr().err
    assert "sub-0050002" in err and "51456" in err


def test_cli_attaches_to_the_input_subject(tmp_path, monkeypatch, stats_files):
    nidm_in = _write_input_nidm(tmp_path / "input.ttl", "50002")
    out = tmp_path / "out" / "nidm.ttl"

    code = run_main(monkeypatch, ["-f", stats_files, "-subjid", "sub-0050002",
                                  "-o", str(out), "--nidm", str(nidm_in), "--forcenidm"])

    assert code == 0
    result = Graph().parse(str(out), format="turtle")
    assert set(result.subjects(SUBJECT_ID, None)) == {URIRef("http://example.org/agent/0")}
