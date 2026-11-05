
import pytest

from conftest import run_main
from rdflib import BNode, Graph, URIRef, Literal, Namespace
from rdflib.namespace import RDF, XSD

from ants_seg_to_nidm.ants_seg_to_nidm import add_seg_data
from ants_seg_to_nidm._pynidm import Constants, NIDM_SUBJECTID_URI

NFO = Namespace("http://www.semanticdesktop.org/ontologies/2007/03/22/nfo#")


class _StatEntity:
    def __init__(self, uri):
        self.uri = uri


SUBJECT_ID = "sub-0050002"


def _build_base_graph():
    graph = Graph()
    participant_agent = URIRef("http://example.org/agent/participant")
    graph.add((participant_agent, RDF.type, Constants.PROV['Agent']))
    graph.add(
        (
            participant_agent,
            URIRef(NIDM_SUBJECTID_URI),
            Literal(SUBJECT_ID, datatype=XSD.string),
        )
    )
    return graph, participant_agent


def _add_anatomical_acquisition(graph, participant_agent, acquisition_uri, activity_uri,
                                filename=None):
    acquisition = URIRef(acquisition_uri)
    if filename is not None:
        graph.add((acquisition, NFO['filename'], Literal(filename)))
    graph.add((acquisition, RDF.type, Constants.NIDM['AcquisitionObject']))
    graph.add(
        (
            acquisition,
            Constants.NIDM['hadAcquisitionModality'],
            Constants.NIDM['MagneticResonanceImaging'],
        )
    )
    graph.add(
        (
            acquisition,
            Constants.NIDM['hadImageUsageType'],
            Constants.NIDM['Anatomical'],
        )
    )
    acquisition_activity = URIRef(activity_uri)
    graph.add((acquisition, Constants.PROV['wasGeneratedBy'], acquisition_activity))
    assoc = BNode()
    graph.add((acquisition_activity, Constants.PROV['qualifiedAssociation'], assoc))
    graph.add((assoc, Constants.PROV['agent'], participant_agent))
    return acquisition


def test_add_seg_data_links_to_matching_acquisition():
    graph, participant_agent = _build_base_graph()

    acquisition = _add_anatomical_acquisition(
        graph,
        participant_agent,
        "http://example.org/acquisition/object",
        "http://example.org/activity/acq",
    )

    add_seg_data(
        graph,
        SUBJECT_ID,
        _StatEntity("http://example.org/entity/stats"),
        add_to_nidm=True,
    )

    assert (None, Constants.PROV['used'], acquisition) in graph


def test_add_seg_data_warns_when_acquisition_missing():
    graph, _participant_agent = _build_base_graph()

    with pytest.warns(UserWarning, match="No anatomical MRI AcquisitionObject"):
        add_seg_data(
            graph,
            SUBJECT_ID,
            _StatEntity("http://example.org/entity/stats"),
            add_to_nidm=True,
        )

    assert not list(graph.triples((None, Constants.PROV['used'], None)))


def test_add_seg_data_links_none_when_several_acquisitions_and_no_name():
    # A guess (formerly the lowest IRI) asserts provenance from an image the
    # stats may not have come from; without -acq_filename, link none.
    graph, participant_agent = _build_base_graph()
    for name in ("A", "B"):
        _add_anatomical_acquisition(
            graph, participant_agent,
            f"http://example.org/acquisition/object{name}",
            f"http://example.org/activity/acq{name}",
            filename=f"sub-0050002_run-{name}_T1w.nii.gz",
        )

    with pytest.warns(UserWarning, match="2 anatomical AcquisitionObjects"):
        add_seg_data(graph, SUBJECT_ID, _StatEntity("http://example.org/entity/stats"),
                     add_to_nidm=True)

    assert not list(graph.triples((None, Constants.PROV['used'], None)))


@pytest.mark.parametrize("names, linked", [
    (["sub-0050002_run-B_T1w.nii.gz"], ["B"]),
    (["/data/bids/sub-0050002/anat/sub-0050002_run-B_T1w.nii.gz"], ["B"]),
    (["sub-0050002_run-A_T1w.nii.gz", "sub-0050002_run-B_T1w.nii.gz"], ["A", "B"]),
    (["sub-0050002_run-C_T1w.nii.gz"], []),
    (["sub-0050002_run-A_T1w.nii.gz", "sub-0050002_run-C_T1w.nii.gz"], []),
], ids=["one-name", "path", "both", "unknown-name", "one-unknown"])
def test_add_seg_data_links_the_named_acquisitions(names, linked):
    graph, participant_agent = _build_base_graph()
    acquisitions = {
        name: _add_anatomical_acquisition(
            graph, participant_agent,
            f"http://example.org/acquisition/object{name}",
            f"http://example.org/activity/acq{name}",
            filename=f"bids::sub-0050002/anat/sub-0050002_run-{name}_T1w.nii.gz",
        )
        for name in ("A", "B")
    }

    if linked:
        add_seg_data(graph, SUBJECT_ID, _StatEntity("http://example.org/entity/stats"),
                     add_to_nidm=True, acq_filename=names)
    else:
        with pytest.warns(UserWarning, match="with file name"):
            add_seg_data(graph, SUBJECT_ID, _StatEntity("http://example.org/entity/stats"),
                         add_to_nidm=True, acq_filename=names)

    used = set(graph.objects(None, Constants.PROV['used']))
    assert used == {acquisitions[n] for n in linked}


def test_add_seg_data_links_none_when_a_name_matches_two_acquisitions():
    graph, participant_agent = _build_base_graph()
    for name in ("A", "B"):
        _add_anatomical_acquisition(
            graph, participant_agent,
            f"http://example.org/acquisition/object{name}",
            f"http://example.org/activity/acq{name}",
            filename=f"bids::ses-{name}/anat/sub-0050002_T1w.nii.gz",
        )
    with pytest.warns(UserWarning, match="2 anatomical AcquisitionObjects with file name"):
        add_seg_data(graph, SUBJECT_ID, _StatEntity("http://example.org/entity/stats"),
                     add_to_nidm=True, acq_filename=["sub-0050002_T1w.nii.gz"])
    assert not list(graph.triples((None, Constants.PROV['used'], None)))


@pytest.mark.parametrize("datatype", [XSD.string, None], ids=["xsd-string", "plain"])
@pytest.mark.parametrize("stored_id", ["sub-0050002", "0050002", "50002"])
def test_add_seg_data_finds_subject_whatever_the_literal_form(stored_id, datatype):
    # PyNIDM 4.x's bids2nidm writes src_subject_id as "..."^^xsd:string,
    # PyNIDM 5's as a plain literal. rdflib treats the two as different terms,
    # so a query spelling the datatype matched only the 4.x form, and on 5.x
    # input --forcenidm minted a second subject agent with no link to the
    # input's demographics or acquisitions.
    graph = Graph()
    participant_agent = URIRef("http://example.org/agent/participant")
    graph.add((participant_agent, RDF.type, Constants.PROV['Agent']))
    graph.add((participant_agent, URIRef(NIDM_SUBJECTID_URI),
               Literal(stored_id, datatype=datatype)))
    acquisition = _add_anatomical_acquisition(
        graph,
        participant_agent,
        "http://example.org/acquisition/object",
        "http://example.org/activity/acq",
    )

    add_seg_data(
        graph,
        SUBJECT_ID,
        _StatEntity("http://example.org/entity/stats"),
        add_to_nidm=True,
        forceagent=True,
    )

    agents = set(graph.subjects(URIRef(NIDM_SUBJECTID_URI), None))
    assert agents == {participant_agent}
    assert (None, Constants.PROV['used'], acquisition) in graph


def _write_input(path, filenames):
    graph, participant_agent = _build_base_graph()
    for n, name in enumerate(filenames):
        _add_anatomical_acquisition(graph, participant_agent,
                                    f"http://example.org/acquisition/{n}",
                                    f"http://example.org/activity/{n}",
                                    filename=f"bids::sub-0050002/anat/{name}")
    graph.serialize(destination=str(path), format="turtle")


def test_cli_acq_filename_selects_the_acquisition(tmp_path, monkeypatch, stats_files):
    nidm_in = tmp_path / "input.ttl"
    _write_input(nidm_in, ["sub-0050002_run-1_T1w.nii.gz", "sub-0050002_run-2_T1w.nii.gz"])
    out = tmp_path / "out" / "nidm.ttl"
    code = run_main(monkeypatch, ["-f", stats_files, "-subjid", SUBJECT_ID, "-o", str(out),
                                  "--nidm", str(nidm_in), "--forcenidm",
                                  "-acq_filename", "sub-0050002_run-2_T1w.nii.gz"])
    assert code == 0
    used = set(Graph().parse(str(out), format="turtle").objects(None, Constants.PROV['used']))
    assert used == {URIRef("http://example.org/acquisition/1")}


def test_cli_acq_filename_requires_an_input_nidm(tmp_path, monkeypatch, stats_files):
    out = tmp_path / "out" / "nidm.ttl"
    code = run_main(monkeypatch, ["-f", stats_files, "-subjid", SUBJECT_ID, "-o", str(out),
                                  "-acq_filename", "sub-0050002_T1w.nii.gz"])
    assert code != 0
    assert not out.exists()
