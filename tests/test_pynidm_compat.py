"""The PyNIDM constants this converter uses, on whichever PyNIDM is installed.

Fixtures elsewhere build triples from the same symbols the converter
queries with, so a wrong value would pass them; pin the values here.
"""
from ants_seg_to_nidm._pynidm import (
    NIDM_SUBJECTID_LOCALPART,
    NIDM_SUBJECTID_URI,
    PYNIDM_MAJOR,
    Constants,
)


def test_subject_id_term():
    assert NIDM_SUBJECTID_URI == (
        "https://ndar.nih.gov/api/datadictionary/v2/dataelement/src_subject_id"
    )
    assert NIDM_SUBJECTID_LOCALPART == "src_subject_id"


def test_namespaces():
    assert str(Constants.NIIRI) == "http://iri.nidash.org/"
    assert str(Constants.PROV) == "http://www.w3.org/ns/prov#"
    assert str(Constants.NIDM) == "http://purl.org/nidash/nidm#"
    assert PYNIDM_MAJOR in (4, 5)


def test_measure_and_provenance_namespaces():
    # ANTS is the namespace of every measure the converter writes and must be
    # the ants: prefix of mapping_data/ants_cde.ttl; NDAR, SIO and DCT name
    # the subject id, the subject role and the activity's description.
    assert str(Constants.ANTS) == "http://stnava.github.io/ANTs/"
    assert str(Constants.NDAR) == "https://ndar.nih.gov/api/datadictionary/v2/dataelement/"
    assert str(Constants.SIO) == "http://semanticscience.org/ontology/sio.owl#"
    assert str(Constants.DCT) == "http://purl.org/dc/terms/"


def test_cde_file_uses_the_ants_namespace():
    from pathlib import Path

    from rdflib import Graph

    from ants_seg_to_nidm import antsutils

    ttl = Path(antsutils.__file__).parent / "mapping_data" / "ants_cde.ttl"
    prefixes = dict(Graph().parse(str(ttl), format="turtle").namespaces())
    assert str(prefixes["ants"]) == str(Constants.ANTS)
