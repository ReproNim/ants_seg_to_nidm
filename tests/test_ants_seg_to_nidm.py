
import pytest
from rdflib import BNode, Graph, URIRef, Literal
from rdflib.namespace import RDF, XSD

from ants_seg_to_nidm.ants_seg_to_nidm import add_seg_data
from ants_seg_to_nidm._pynidm import Constants, NIDM_SUBJECTID_URI


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


def _add_anatomical_acquisition(graph, participant_agent, acquisition_uri, activity_uri):
    acquisition = URIRef(acquisition_uri)
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


def test_add_seg_data_warns_and_chooses_first_when_multiple_acquisitions():
    graph, participant_agent = _build_base_graph()

    first = _add_anatomical_acquisition(
        graph,
        participant_agent,
        "http://example.org/acquisition/objectA",
        "http://example.org/activity/acqA",
    )
    _add_anatomical_acquisition(
        graph,
        participant_agent,
        "http://example.org/acquisition/objectB",
        "http://example.org/activity/acqB",
    )

    with pytest.warns(UserWarning, match="Multiple anatomical AcquisitionObjects"):
        add_seg_data(
            graph,
            SUBJECT_ID,
            _StatEntity("http://example.org/entity/stats"),
            add_to_nidm=True,
        )

    assert (None, Constants.PROV['used'], first) in graph
    assert (None, Constants.PROV['used'], URIRef("http://example.org/acquisition/objectB")) not in graph


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
