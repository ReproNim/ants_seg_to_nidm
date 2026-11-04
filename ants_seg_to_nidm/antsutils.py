#!/usr/bin/env python
"""Utilities for extracting information from freesurfer stats files

"""

import json
import os
import sys
from collections import namedtuple
from pathlib import Path
import rdflib as rl
from requests import get
import nibabel as nib
import numpy as np
import pandas as pd

ANTSDKT = namedtuple("ANTSDKT", ["structure", "hemi", "measure", "unit"])
cde_file = Path(os.path.dirname(__file__)) / "mapping_data" / "ants-cdes.json"
map_file = Path(os.path.dirname(__file__)) / "mapping_data" / "antsmap.json"
lut_file = Path(os.path.dirname(__file__)) / "mapping_data" / "FreeSurferColorLUT.txt"
neuromorphometrics_file = (
    Path(os.path.dirname(__file__)) / "mapping_data" / "neuromorphometrics_labels.json"
)

#: Segmentation label protocols this converter can name.
#:
#: ANTs segmentations do not all speak FreeSurfer. The ants_bidsapp's
#: ``--method quick`` warps a template map numbered in FreeSurfer aseg ids,
#: while ``--method fusion`` votes over the OASIS-TRT-20 ``DKT31_CMA`` atlases,
#: whose *subcortical* ids follow the Neuromorphometrics / BrainCOLOR protocol
#: distributed with the MICCAI 2012 Multi-Atlas Labelling Challenge. Both
#: occupy the same small-integer range, so naming one with the other's table
#: produces a well-formed file full of wrong structures -- the right thalamus
#: reported as ``Right-Substancia-Nigra``, cerebellar white matter as
#: ``Right-Cerebral-White-Matter``, and a dozen names that denote nothing a
#: brain contains (``Left-Porg``, ``Right-Stellate``, ``Right-undetermined``).
#: Naming therefore has to be told which protocol it is reading.
FREESURFER = "freesurfer"
NEUROMORPHOMETRICS = "neuromorphometrics"
LABEL_PROTOCOLS = (FREESURFER, NEUROMORPHOMETRICS)

#: Cortical DKT ids are identical in both protocols, so in Neuromorphometrics
#: mode anything at or above this floor is resolved from the FreeSurfer table.
#: Below it, an id absent from the Neuromorphometrics table stays unnamed
#: rather than falling through -- falling through is exactly the bug.
_CORTICAL_ID_FLOOR = 1000

#: Structures the FreeSurfer LUT does not carry a line for.
_UNLISTED = {
    91: "Left basal forebrain",
    92: "Right basal forebrain",
    630: "Cerebellar vermal lobules I - V",
    631: "Cerebellar vermal lobules VI - VII",
    632: "Cerebellar vermal lobules VIII - X",
}

#: Label ids accepted under the ``freesurfer`` protocol: the FreeSurfer aseg
#: and cortical vocabulary that FreeSurfer-numbered ANTs segmentations emit.
#: Most ids in the low range also have a FreeSurferColorLUT line, so without
#: this set a Neuromorphometrics-numbered segmentation named with the default
#: protocol gets well-formed, wrong names (59 -> Right-Substancia-Nigra) and
#: fails only on the few ids whose name happens to lack a data element.
#:
#: Sources:
#: - ``_ASEG_STATS_IDS``: the structures FreeSurfer's aseg.stats reports
#:   (mri_segstats with ASegStatsLUT.txt, FreeSurfer 8.0.0).
#: - ``_QUICK_TEMPLATE_IDS``: the non-cortical ids of the pre-fused
#:   OASIS-TRT-20 joint-fusion map in OASIS-30 space
#:   (OASIS-TRT-20_jointfusion_DKT31_CMA_labels_in_OASIS-30_v2.nii.gz), which
#:   ants-nidm ``--method quick`` warps to the subject. It adds the
#:   cerebellum exterior (6, 45), basal forebrain (91, 92) and vermis
#:   (630-632) labels.
#: - ``_CORTICAL_IDS``: the 33 cortical parcels per hemisphere that
#:   ants-cdes.json has data elements for (the DKT31 set plus frontal and
#:   temporal pole); their numbering is the same in both protocols.
_ASEG_STATS_IDS = frozenset({
    4, 5, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 24, 26, 28, 30, 31, 43,
    44, 46, 47, 49, 50, 51, 52, 53, 54, 58, 60, 62, 63, 72, 77, 78, 79, 80,
    81, 82, 85, 251, 252, 253, 254, 255,
})
_QUICK_TEMPLATE_IDS = frozenset({
    4, 5, 6, 7, 10, 11, 12, 13, 14, 15, 16, 17, 18, 24, 26, 28, 30, 43, 44,
    45, 46, 49, 50, 51, 52, 53, 54, 58, 60, 91, 92, 630, 631, 632,
})
_CORTICAL_IDS = frozenset(
    hemi + parcel for hemi in (1000, 2000) for parcel in range(2, 36) if parcel != 4
)
FREESURFER_IDS = _ASEG_STATS_IDS | _QUICK_TEMPLATE_IDS | _CORTICAL_IDS

_lut_cache = None
_neuromorphometrics_cache = None


def _freesurfer_lut():
    """id -> structure name, parsed once.

    The previous implementation scanned the file with
    ``line.startswith(str(id))``, which is a prefix match: an id with no line
    of its own could silently pick up a longer id's name. It happened not to
    misfire on any id these segmentations emit, but a lookup that can invent a
    plausible wrong answer is the same failure mode this module now exists to
    prevent, so the match is exact.
    """
    global _lut_cache
    if _lut_cache is None:
        table = {}
        with open(lut_file, "r") as fp:
            for line in fp:
                parts = line.split()
                if parts and parts[0].isdigit():
                    table.setdefault(int(parts[0]), parts[1])
        _lut_cache = table
    return _lut_cache


def _neuromorphometrics_labels():
    """Neuromorphometrics / BrainCOLOR subcortical id -> structure name.

    The names are deliberately the FreeSurfer spellings of the same
    structures, so a given structure keys into the same ``ants-cdes.json``
    entry whichever protocol produced it, rather than minting a duplicate CDE
    per protocol.
    """
    global _neuromorphometrics_cache
    if _neuromorphometrics_cache is None:
        with open(neuromorphometrics_file, "r") as fp:
            _neuromorphometrics_cache = {int(k): v for k, v in json.load(fp).items()}
    return _neuromorphometrics_cache


def get_id_to_struct(id, protocol=FREESURFER):
    """Resolve a segmentation label id to a structure name.

    Parameters
    ----------
    id : int
        Label id as it appears in the first column of the ANTs label stats.
    protocol : str
        ``"freesurfer"`` (the default, and what every caller before 2026-09-23
        got) or ``"neuromorphometrics"``.

    Returns
    -------
    str or None
        ``None`` when the id has no name under ``protocol``, which the caller
        treats as a row to skip or to raise on.
    """
    if protocol not in LABEL_PROTOCOLS:
        raise ValueError(
            f"unknown label protocol {protocol!r}; expected one of {LABEL_PROTOCOLS}"
        )
    id = int(id)

    if protocol == NEUROMORPHOMETRICS and id < _CORTICAL_ID_FLOOR:
        return _neuromorphometrics_labels().get(id)

    name = _freesurfer_lut().get(id)
    if name is not None:
        return name
    return _UNLISTED.get(id)


def get_details(key, structure):
    hemi = None
    if "Left" in structure or "lh" in structure:
        hemi = "Left"
    if "Right" in structure or "rh" in structure:
        hemi = "Right"
    if "Area" in key:
        unit = "mm^2"
    else:
        unit = "voxel"
    measure = key
    return hemi, measure, unit


def _ids(ids):
    return ", ".join(map(str, sorted(ids)))


def _freesurfer_refusal(foreign):
    """Why ids outside FREESURFER_IDS are refused, and the remedy where one fits.

    Only ids the Neuromorphometrics table names are pointed at
    --label-protocol neuromorphometrics, and only conditionally: the same
    number means another structure in each protocol (41 is FreeSurfer's
    Right-Cerebral-White-Matter and Neuromorphometrics' Left-Cerebellum-White-
    Matter), so the converter cannot tell which one the segmentation means.
    """
    message = (f"Label ID(s) {_ids(foreign)} are not among the ids the 'freesurfer' label "
               f"protocol accepts (FreeSurfer aseg.stats structures, the subcortical ids of the "
               f"ants-nidm --method quick template, and cortical parcels 1002-1035/2002-2035 "
               f"except 1004/2004).")
    nm_named = foreign & set(_neuromorphometrics_labels())
    if nm_named:
        message += (f" {_ids(nm_named)} are subcortical ids of the Neuromorphometrics protocol "
                    f"(ants-nidm --method fusion); if the segmentation follows that protocol, "
                    f"--label-protocol neuromorphometrics names them.")
    neither = foreign - nm_named
    if neither:
        message += f" {_ids(neither)} are accepted under neither protocol."
    return message


def neuromorphometrics_ids():
    """Label ids accepted under the ``neuromorphometrics`` protocol: the
    subcortical ids of its table and the cortical parcels (numbered the same
    in both protocols, named from the FreeSurfer table)."""
    return frozenset(_neuromorphometrics_labels()) | _CORTICAL_IDS


def _neuromorphometrics_refusal(foreign):
    message = (f"Label ID(s) {_ids(foreign)} are not among the ids the 'neuromorphometrics' label "
               f"protocol accepts (the 37 subcortical ids of the OASIS-TRT-20 DKT31_CMA atlases, "
               f"as ants-nidm --method fusion emits, and cortical parcels 1002-1035/2002-2035 "
               f"except 1004/2004).")
    fs_named = foreign & FREESURFER_IDS
    if fs_named:
        message += (f" {_ids(fs_named)}: the default 'freesurfer' protocol accepts them (as "
                    f"ants-nidm --method quick emits); if the segmentation follows FreeSurfer "
                    f"numbering, omit --label-protocol neuromorphometrics.")
    neither = foreign - fs_named
    if neither:
        message += f" {_ids(neither)} are accepted under neither protocol."
    return message


def read_ants_stats(ants_stats_file, ants_brainvols_file, mri_file, force_error=True,
                    collect_missing=False, label_protocol=FREESURFER):
    """
    Reads in an ANTS stats file along with associated mri_file (for voxel sizes) and converts to a measures dictionary with keys:
    ['structure':XX, 'items': [{'name': 'NVoxels', 'description': 'Number of voxels','value':XX, 'units':'unitless'},
                        {'name': 'Volume_mm3', 'description': ''Volume', 'value':XX, 'units':'mm^3'}]]
    :param ants_stats_file: path to ANTS segmentation output file named "antslabelstats"
    :param ants_brainvols_file: path to the ANTs "antsbrainvols" CSV: one row, columns matched by
        name to data elements (BVOL = brain-extraction mask volume, LABELVOL = sum of the label
        volumes, GMVOL, WMVOL, CSFVOL, ThicknessSum, PearsonCorrelation)
    :param mri_file: mri file to extract voxel sizes from
    :param freesurfer_lookup_table: Lookup table used to map 1st column of ants_stats_file label numbers to structure names
    :param label_protocol: which numbering the label ids follow -- "freesurfer"
        (the default) or "neuromorphometrics". Naming a segmentation with the
        wrong protocol yields a well-formed file describing the wrong structures.
        Label ids the protocol does not accept raise ValueError, also with
        collect_missing.
    :return: measures is a list of dictionaries as defined above
    """

    # fs_lookup_table = loadfreesurferlookuptable(freesurfer_lookup_table)

    # open stats file, brain vols file as pandas dataframes
    ants_stats = pd.read_csv(ants_stats_file)
    brain_vols = pd.read_csv(ants_brainvols_file)

    # load mri_file and extract voxel sizes
    img = nib.load(mri_file)
    vox_size = np.prod(list(img.header.get_zooms()))

    with open(cde_file, "r") as fp:
        ants_cde = json.load(fp)

    measures = []
    changed = False
    missing_labels = []  # Collect all missing labels if collect_missing=True
    # iterate over columns in brain vols
    for key, j in brain_vols.T.iterrows():
        value = j.values[0]
        keytuple = ANTSDKT(
            structure=key if "vol" in key.lower() else "Brain",
            hemi=None,
            measure="Volume" if "vol" in key.lower() else key,
            unit="mm^3"
            if "vol" in key.lower()
            else "mm"
            if "Thickness" in key
            else None,
        )
        if str(keytuple) not in ants_cde:
            if collect_missing:
                missing_labels.append(str(keytuple))
                continue  # Skip this entry but continue processing
            ants_cde["count"] += 1
            ants_cde[str(keytuple)] = {
                "id": f"{ants_cde['count']:0>6d}",
                "label": f"{key} ({keytuple.unit})",
            }
            if force_error:
                raise ValueError(f"Key {keytuple} not found in ANTS data elements file")
            changed = True
        if "vol" in key.lower():
            measures.append((f'{ants_cde[str(keytuple)]["id"]}', str(int(value))))
        else:
            measures.append((f'{ants_cde[str(keytuple)]["id"]}', str(value)))

    # A protocol violation is an error also under --collect-missing: skipping
    # the rows would leave the overlapping ids named with the wrong table.
    label_ids = {int(v) for v in ants_stats["Label"]}
    if label_protocol == FREESURFER:
        foreign = label_ids - FREESURFER_IDS
        if foreign:
            raise ValueError(_freesurfer_refusal(foreign))
    else:
        foreign = label_ids - neuromorphometrics_ids()
        if foreign:
            raise ValueError(_neuromorphometrics_refusal(foreign))

    # iterate over rows in label stats
    for row in ants_stats.iterrows():
        structure = None
        for key, val in row[1].items():
            if key == "Label":
                segid = int(val)
                structure = get_id_to_struct(segid, protocol=label_protocol)
                if structure is None:
                    if collect_missing:
                        print(f"Warning: Label {int(val)} has no name under the "
                              f"{label_protocol!r} label protocol - skipping this row",
                              file=sys.stderr)
                        break  # Skip entire row for this label
                    raise ValueError(
                        f"Label ID {int(val):d} did not return any structure under the "
                        f"{label_protocol!r} label protocol"
                    )
                continue
            if "VolumeInVoxels" not in key and "Area" not in key:
                continue
            hemi, measure, unit = get_details(key, structure)
            key_tuple = ANTSDKT(
                structure=structure, hemi=hemi, measure=measure, unit=unit
            )
            label = f"{structure} {measure} ({unit})"
            if str(key_tuple) not in ants_cde:
                if collect_missing:
                    missing_labels.append(str(key_tuple))
                    continue  # Skip this entry but continue processing
                ants_cde["count"] += 1
                ants_cde[str(key_tuple)] = {
                    "id": f"{ants_cde['count']:0>6d}",
                    "structure_id": segid,
                    "label": label,
                }
                if force_error:
                    raise ValueError(
                        f"Key {key_tuple} not found in ANTS data elements file"
                    )
                changed = True
            # measures.append((f'{ants_cde[str(key_tuple)]["id"]}', str(val)))

            if "VolumeInVoxels" in key:
                measure = "Volume"
                unit = "mm^3"
                key_tuple = ANTSDKT(
                    structure=structure, hemi=hemi, measure=measure, unit=unit
                )
                label = f"{structure} {measure} ({unit})"
                if str(key_tuple) not in ants_cde:
                    if collect_missing:
                        missing_labels.append(str(key_tuple))
                        continue  # Skip this entry but continue processing
                    ants_cde["count"] += 1
                    ants_cde[str(key_tuple)] = {
                        "id": f"{ants_cde['count']:0>6d}",
                        "structure_id": segid,
                        "label": label,
                    }
                    if force_error:
                        raise ValueError(
                            f"Key {key_tuple} not found in ANTS data elements file"
                        )
                    changed = True
                measures.append(
                    (f'{ants_cde[str(key_tuple)]["id"]}', str(val * vox_size))
                )

    if changed:
        with open(cde_file, "w") as fp:
            json.dump(ants_cde, fp, indent=2)

    # Report all missing labels if we were collecting them
    if collect_missing and missing_labels:
        unique_missing_labels = sorted(set(missing_labels))
        num_missing = len(unique_missing_labels)
        print(f"\n{'='*70}", file=sys.stderr)
        print(f"FOUND {num_missing} MISSING LABEL(S):", file=sys.stderr)
        print(f"{'='*70}", file=sys.stderr)
        for label in unique_missing_labels:
            print(f"  {label}", file=sys.stderr)
        print(f"{'='*70}\n", file=sys.stderr)
        raise ValueError(f"Found {num_missing} missing label(s) - see list above")

    return measures


def hemiless(key):
    return (
        key.replace("-lh-", "-")
        .replace("-rh-", "-")
        .replace("_lh_", "-")
        .replace("_rh_", "-")
        .replace("rh", "")
        .replace("lh", "")
        .replace("Left-", "")
        .replace("Right-", "")
        .replace("Left ", "")
        .replace("Right ", "")
    )


def create_ants_mapper():
    """Create FreeSurfer to ReproNim mapping information
    """

    with open(map_file, "r") as fp:
        ants_map = json.load(fp)

    with open(cde_file, "r") as fp:
        ants_cde = json.load(fp)

    s = ants_map["Structures"]
    m = ants_map["Measures"]
    for key in ants_cde:
        if key == "count":
            continue
        key_tuple = eval(key)
        sk = key_tuple.structure
        mk = key_tuple.measure
        hk = hemiless(sk)
        if hk in s:
            if sk not in s[hk]["antskey"]:
                s[hk]["antskey"].append(sk)
        else:
            s[hk] = dict(isAbout=None, antskey=[sk])
        if mk not in m:
            m[mk] = dict(measureOf=None, datumType=None, hasUnit=key_tuple.unit)

        if s[hk]["isAbout"] is not None and (
            "UNKNOWN" not in s[hk]["isAbout"] and "CUSTOM" not in s[hk]["isAbout"]
        ):
            ants_cde[key]["isAbout"] = s[hk]["isAbout"]

        if m[key_tuple.measure]["measureOf"] is not None:
            ants_cde[key].update(**m[key_tuple.measure])

    with open(map_file, "w") as fp:
        json.dump(ants_map, fp, sort_keys=True, indent=2)
        fp.write("\n")

    with open(cde_file, "w") as fp:
        json.dump(ants_cde, fp, indent=2)
        fp.write("\n")

    return ants_map, ants_cde


def create_cde_graph(restrict_to=None):
    """Create an RDFLIB graph with the FreeSurfer CDEs

    Any CDE that has a mapping will be mapped
    """
    with open(cde_file, "r") as fp:
        ants_cde = json.load(fp)
    from ants_seg_to_nidm._pynidm import Constants

    ants = Constants.ANTS
    nidm = Constants.NIDM

    g = rl.Graph()
    g.bind("ants", ants)
    g.bind("nidm", nidm)
    g.bind("uberon", "http://purl.obolibrary.org/obo/UBERON_")
    g.bind("ilx", "http://uri.interlex.org/base/ilx_")

    # added by DBK to create subclass relationship
    g.add((ants["DataElement"], rl.RDFS['subClassOf'], nidm['DataElement']))


    for key, value in ants_cde.items():
        if key == "count":
            continue
        if restrict_to is not None:
            if value["id"] not in restrict_to:
                continue
        for subkey, item in value.items():
            if subkey == "id":
                antsid = "ants_" + item
                g.add((ants[antsid], rl.RDF.type, ants["DataElement"]))
                continue
            if item is None or "unknown" in str(item):
                continue
            if subkey in ["isAbout", "datumType", "measureOf"]:
                g.add((ants[antsid], nidm[subkey], rl.URIRef(item)))
            elif subkey in ["hasUnit"]:
                g.add((ants[antsid], nidm[subkey], rl.Literal(item)))
            # added by DBK to use rdfs:label
            elif subkey in ["label"]:
                g.add((ants[antsid], rl.RDFS['label'], rl.Literal(item)))
            else:
                if isinstance(item, rl.URIRef):
                    g.add((ants[antsid], ants[subkey], item))
                else:
                    g.add((ants[antsid], ants[subkey], rl.Literal(item)))
        key_tuple = eval(key)
        for subkey, item in key_tuple._asdict().items():
            if item is None:
                continue
            if subkey == "hemi":
                g.add((ants[antsid], nidm["hasLaterality"], rl.Literal(item)))
            else:
                g.add((ants[antsid], ants[subkey], rl.Literal(item)))
    return g


def convert_stats_to_nidm(stats):
    """Convert a stats record into a NIDM entity

    Returns the entity and the prov document
    """
    from ants_seg_to_nidm._pynidm import Constants, getUUID
    import prov

    ants = prov.model.Namespace("ants", str(Constants.ANTS))
    niiri = prov.model.Namespace("niiri", str(Constants.NIIRI))
    nidm = prov.model.Namespace("nidm", "http://purl.org/nidash/nidm#")
    doc = prov.model.ProvDocument()
    e = doc.entity(identifier=niiri[getUUID()])
    e.add_asserted_type(nidm["ANTSStatsCollection"])
    e.add_attributes(
        {
            ants["ants_" + val[0]]: prov.model.Literal(
                val[1],
                datatype=prov.model.XSD["float"]
                if "." in val[1]
                else prov.model.XSD["integer"],
            )
            for val in stats
        }
    )
    return e, doc
