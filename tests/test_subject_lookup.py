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


def _add_acquisition(graph, agent, name, usage="Anatomical"):
    acquisition = URIRef(f"http://example.org/acquisition/{name}")
    activity = URIRef(f"http://example.org/activity/{name}")
    graph.add((acquisition, RDF.type, Constants.NIDM["AcquisitionObject"]))
    graph.add((acquisition, Constants.NIDM["hadAcquisitionModality"],
               Constants.NIDM["MagneticResonanceImaging"]))
    graph.add((acquisition, Constants.NIDM["hadImageUsageType"], Constants.NIDM[usage]))
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


# Every node carrying a subject id is a subject, whatever its rdf:type and
# under either NDAR IRI (the rule segstats_jsonld uses too). A participant a
# stricter rule did not see made the input look subject-less, so --forcenidm
# attached the stats to a new agent and the run exited 0.
HTTP_SUBJECT_ID = URIRef(str(SUBJECT_ID).replace("https://", "http://", 1))
SUBJECT_KINDS = ["nidm-subject", "untyped", "http-ndar-iri"]


def _add_participant(graph, kind, stored_id):
    node = URIRef(f"http://example.org/participant/{kind}/{stored_id}")
    if kind == "nidm-subject":
        graph.add((node, RDF.type, Constants.NIDM["Subject"]))
    if kind == "http-ndar-iri":
        graph.add((node, RDF.type, Constants.PROV["Person"]))
    predicate = HTTP_SUBJECT_ID if kind == "http-ndar-iri" else SUBJECT_ID
    graph.add((node, predicate, Literal(stored_id)))
    return node


@pytest.mark.parametrize("kind", SUBJECT_KINDS)
def test_any_node_with_a_subject_id_is_found(kind):
    graph = Graph()
    node = _add_participant(graph, kind, "0050002")

    add_seg_data(graph, "sub-0050002", STATS, add_to_nidm=True, forceagent=True)

    assert _subject_agent_of_stats(graph) == {node}


@pytest.mark.parametrize("forceagent", [True, False], ids=["forcenidm", "no-forcenidm"])
@pytest.mark.parametrize("kind", SUBJECT_KINDS)
def test_a_miss_beside_any_node_with_a_subject_id_is_an_error(kind, forceagent):
    graph = Graph()
    _add_participant(graph, kind, "0050003")

    with pytest.raises(SubjectLookupError) as err:
        add_seg_data(graph, "sub-0050002", STATS, add_to_nidm=True, forceagent=forceagent)

    assert "0050003" in str(err.value)
    assert _subject_agent_of_stats(graph) == set()


def test_a_typed_and_an_untyped_node_with_the_same_id_are_two_matches():
    graph, (typed,) = _graph_with_subjects("0050002")
    untyped = _add_participant(graph, "untyped", "50002")

    with pytest.raises(SubjectLookupError) as err:
        add_seg_data(graph, "sub-0050002", STATS, add_to_nidm=True, forceagent=True)

    assert str(typed) in str(err.value) and str(untyped) in str(err.value)


@pytest.mark.parametrize("kind", SUBJECT_KINDS)
def test_cli_miss_beside_any_node_with_a_subject_id_fails_even_with_forcenidm(
        tmp_path, monkeypatch, stats_files, capsys, kind):
    graph = Graph()
    _add_participant(graph, kind, "51456")
    nidm_in = tmp_path / "input.ttl"
    graph.serialize(destination=str(nidm_in), format="turtle")
    out = tmp_path / "out" / "nidm.ttl"

    code = run_main(monkeypatch, ["-f", stats_files, "-subjid", "sub-0050002",
                                  "-o", str(out), "--nidm", str(nidm_in), "--forcenidm"])

    assert code == 1
    assert not out.exists()
    assert "which has subject IDs: 51456" in capsys.readouterr().err


def _failing_lookup_input(case):
    if case == "miss":
        graph, _ = _graph_with_subjects("51456")
    elif case == "several":
        graph, _ = _graph_with_subjects("50002", "sub-0050002")
    else:  # no subjects, no --forcenidm
        graph = Graph()
        graph.add((URIRef("http://example.org/project"), RDF.type, Constants.NIDM["Project"]))
    return graph


@pytest.mark.parametrize("case", ["miss", "several", "no-subjects"])
def test_a_failed_lookup_leaves_the_graph_unchanged(case):
    """A library caller that catches SubjectLookupError and then serialises
    the graph must not find stray activities or descriptions in it."""
    graph = _failing_lookup_input(case)
    before = set(graph)

    with pytest.raises(SubjectLookupError):
        add_seg_data(graph, "sub-0050002", STATS, add_to_nidm=True, forceagent=case != "no-subjects")

    assert set(graph) == before


@pytest.mark.parametrize("value, expected", [
    ("sub-0050002", "50002"), ("0050002", "50002"), ("50002", "50002"),
    ("0", "0"), ("000", "0"), ("sub-00", "0"), ("sub-", ""), ("", ""),
    (Literal("007", datatype=XSD.string), "7"),
])
def test_normalise_subject_id(value, expected):
    """The same table as segstats_jsonld's, so both converters normalise alike."""
    from ants_seg_to_nidm.ants_seg_to_nidm import normalise_subject_id
    assert normalise_subject_id(value) == expected


def test_an_all_zero_id_matches_its_other_spellings():
    graph, (agent,) = _graph_with_subjects("0")

    add_seg_data(graph, "sub-000", STATS, add_to_nidm=True)

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


# Several nodes with the same subject id: some delivered inputs (ADHD200 NYU,
# Peking_1, Pittsburgh) hold two prov:Agent+prov:Person nodes per subject, one
# owning the anatomical MRI acquisition and one associated only with an
# assessment. The node that owns the acquisition is the one the stats belong to.

def _add_assessment(graph, agent, name):
    """An instrument-based assessment, which is not an anatomical acquisition."""
    assessment = URIRef(f"http://example.org/assessment/{name}")
    activity = URIRef(f"http://example.org/activity/{name}")
    graph.add((assessment, RDF.type, Constants.NIDM["AcquisitionObject"]))
    graph.add((assessment, RDF.type, Constants.NIDM["AssessmentObject"]))
    graph.add((assessment, Constants.PROV["wasGeneratedBy"], activity))
    assoc = BNode()
    graph.add((activity, Constants.PROV["qualifiedAssociation"], assoc))
    graph.add((assoc, Constants.PROV["agent"], agent))


def _two_nodes_with_one_id(owners, functional=(), names=("first", "second")):
    """Nodes storing the same id 0050002, each with an assessment; the nodes
    numbered in owners also own an anatomical MRI acquisition, those in
    functional only a functional one."""
    graph = Graph()
    nodes = []
    for name in names:
        node = URIRef(f"http://example.org/agent/{name}")
        graph.add((node, RDF.type, Constants.PROV["Agent"]))
        graph.add((node, RDF.type, Constants.PROV["Person"]))
        graph.add((node, SUBJECT_ID, Literal("0050002", datatype=XSD.string)))
        _add_assessment(graph, node, f"assessment-{name}")
        nodes.append(node)
    acquisitions = {nodes[n]: _add_acquisition(graph, nodes[n], f"t1w-{n}") for n in owners}
    for n in functional:
        _add_acquisition(graph, nodes[n], f"bold-{n}", usage="Functional")
    return graph, nodes, acquisitions


@pytest.mark.parametrize("owner", [0, 1], ids=["first-owns", "second-owns"])
def test_of_several_matches_the_one_owning_the_anatomical_acquisition_is_chosen(owner, capsys):
    graph, nodes, acquisitions = _two_nodes_with_one_id({owner})

    add_seg_data(graph, "sub-0050002", STATS, add_to_nidm=True, forceagent=True)

    chosen = nodes[owner]
    assert _subject_agent_of_stats(graph) == {chosen}
    assert (None, Constants.PROV["used"], acquisitions[chosen]) in graph
    assert set(graph.subjects(SUBJECT_ID, None)) == set(nodes)
    out = capsys.readouterr().out
    assert str(chosen) in out and "anatomical" in out
    assert str(nodes[1 - owner]) in out


@pytest.mark.parametrize("owners, functional", [(set(), ()), ({0, 1}, ()), (set(), (0,))],
                         ids=["neither-owns", "both-own", "one-owns-only-a-functional-acquisition"])
def test_several_matches_fail_unless_exactly_one_owns_an_anatomical_acquisition(owners, functional):
    """A functional MRI acquisition is not ownership: only an anatomical one is
    what the stats were computed from."""
    graph, nodes, _ = _two_nodes_with_one_id(owners, functional)
    before = set(graph)

    with pytest.raises(SubjectLookupError) as err:
        add_seg_data(graph, "sub-0050002", STATS, add_to_nidm=True, forceagent=True)

    message = str(err.value)
    assert str(nodes[0]) in message and str(nodes[1]) in message
    assert "anatomical" in message and "no output written" in message
    assert ("none of them owns" in message) == (not owners)
    assert set(graph) == before


def test_the_failure_names_the_candidates_that_own_an_anatomical_acquisition():
    graph, nodes, _ = _two_nodes_with_one_id({0, 2}, names=("first", "second", "third"))

    with pytest.raises(SubjectLookupError) as err:
        add_seg_data(graph, "sub-0050002", STATS, add_to_nidm=True, forceagent=True)

    assert ("2 of them own an anatomical MRI acquisition (%s, %s)" % (nodes[0], nodes[2])
            in str(err.value))


@pytest.mark.parametrize("requested", ["sub-01", "01", "1"])
def test_the_owner_rule_does_not_choose_between_different_stored_ids(requested):
    """"sub-01" and "001" are different BIDS labels that only normalise alike,
    so they may be two participants: the owner of the scan is not chosen over
    the node that stores the requested label."""
    graph, (exact, other) = _graph_with_subjects("sub-01", "001")
    _add_assessment(graph, exact, "assessment-exact")
    _add_acquisition(graph, other, "t1w-other")
    before = set(graph)

    with pytest.raises(SubjectLookupError) as err:
        add_seg_data(graph, requested, STATS, add_to_nidm=True, forceagent=True)

    message = str(err.value)
    assert "%s (sub-01)" % exact in message and "%s (001)" % other in message
    assert "differ" in message and "no output written" in message
    assert set(graph) == before


def test_a_miss_lists_each_stored_id_once():
    graph, _, _ = _two_nodes_with_one_id({0})

    with pytest.raises(SubjectLookupError) as err:
        add_seg_data(graph, "sub-0050009", STATS, add_to_nidm=True, forceagent=True)

    assert str(err.value).count("0050002") == 1


# find_subject is the lookup add_seg_data uses, public so that a BIDS app can
# run the same rule before it computes anything. It never changes the graph.

def _find(graph, *args, **kwargs):
    from ants_seg_to_nidm.ants_seg_to_nidm import find_subject
    before = set(graph)
    try:
        return find_subject(graph, *args, **kwargs)
    finally:
        assert set(graph) == before


@pytest.mark.parametrize("requested", SPELLINGS)
def test_find_subject_returns_the_matching_node(requested):
    graph, (agent, _) = _graph_with_subjects("0050002", "0050003", datatype=None)

    assert _find(graph, requested) == agent


@pytest.mark.parametrize("owner", [0, 1])
def test_find_subject_returns_the_acquisition_owner_of_several_matches(owner):
    graph, nodes, _ = _two_nodes_with_one_id({owner})

    assert _find(graph, "sub-0050002") == nodes[owner]


@pytest.mark.parametrize("case, forceagent", [
    ("miss", True), ("miss", False), ("several", True), ("several", False), ("no-subjects", False),
])
def test_find_subject_raises_when_the_subject_cannot_be_identified(case, forceagent):
    graph = _failing_lookup_input(case)

    with pytest.raises(SubjectLookupError):
        _find(graph, "sub-0050002", forceagent=forceagent)


def test_find_subject_returns_none_only_for_forcenidm_without_subjects():
    graph = Graph()

    assert _find(graph, "sub-0050002", forceagent=True) is None
    with pytest.raises(SubjectLookupError, match="--forcenidm"):
        _find(graph, "sub-0050002")


def test_find_subject_does_not_count_agents_created_earlier_as_input_subjects():
    graph, (created,) = _graph_with_subjects("sub-0050002")

    assert _find(graph, "50002", created_agents={created}) == created
    assert _find(graph, "sub-0050009", forceagent=True, created_agents={created}) is None
    with pytest.raises(SubjectLookupError):
        _find(graph, "sub-0050009", forceagent=True)


def test_add_seg_data_looks_the_subject_up_with_find_subject(monkeypatch):
    from ants_seg_to_nidm import ants_seg_to_nidm as module
    calls = []

    def fake(graph, subject_id, forceagent=False, created_agents=()):
        calls.append((subject_id, forceagent))
        raise SubjectLookupError("from find_subject")

    monkeypatch.setattr(module, "find_subject", fake)
    graph, _ = _graph_with_subjects("0050002")
    with pytest.raises(SubjectLookupError, match="from find_subject"):
        add_seg_data(graph, "sub-0050002", STATS, add_to_nidm=True, forceagent=True)
    assert calls == [("sub-0050002", True)]
