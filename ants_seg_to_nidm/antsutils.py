#!/usr/bin/env python
"""Utilities for extracting information from freesurfer stats files

"""

import json
import os
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


def get_id_to_struct(id):
    with open(lut_file, "r") as fp:
        for line in fp.readlines():
            if line.startswith(str(id)):
                return line.split()[1]
    if id == 91:
        return "Left basal forebrain"
    if id == 92:
        return "Right basal forebrain"
    if id == 630:
        return "Cerebellar vermal lobules I - V"
    if id == 631:
        return "Cerebellar vermal lobules VI - VII"
    if id == 632:
        return "Cerebellar vermal lobules VIII - X"
    return None


def get_details(key, structure):
    hemi = None
    if "Left" in structure or "lh" in structure:
        hemi = "Left"
    if "Right" in structure or "rh" in structure:
        hemi = "Right"
    if "Voxels" in key:
        unit = "voxel"
    else:
        unit = "mm"
    measure = key
    return hemi, measure, unit


def read_ants_stats(ants_stats_file, ants_brainvols_file, mri_file, force_error=True):
    """
    Reads in an ANTS stats file along with associated mri_file (for voxel sizes) and converts to a measures dictionary with keys:
    ['structure':XX, 'items': [{'name': 'NVoxels', 'description': 'Number of voxels','value':XX, 'units':'unitless'},
                        {'name': 'Volume_mm3', 'description': ''Volume', 'value':XX, 'units':'mm^3'}]]
    :param ants_stats_file: path to ANTS segmentation output file named "antslabelstats"
    :param ants_brainvols_file: path to ANTS segmentation output for Bvol, Gvol, Wvol, and ThicknessSum (called antsbrainvols"
    :param mri_file: mri file to extract voxel sizes from
    :param freesurfer_lookup_table: Lookup table used to map 1st column of ants_stats_file label numbers to structure names
    :return: measures is a list of dictionaries as defined above
    """

    # fs_lookup_table = loadfreesurferlookuptable(freesurfer_lookup_table)

    # open stats file, brain vols file as pandas dataframes
    ants_stats = pd.read_csv(ants_stats_file)
    brain_vols = pd.read_csv(ants_brainvols_file)

    # load mri_file and extract voxel sizes
    img = nib.load(mri_file)
    vox_size = np.product(list(img.header.get_zooms()))

    with open(cde_file, "r") as fp:
        ants_cde = json.load(fp)

    measures = []
    changed = False
    # iterate over columns in brain vols
    for key, j in brain_vols.T.iterrows():
        value = j.values[0]
        voxkey = ANTSDKT(
            structure=key if "vol" in key.lower() else "Brain",
            hemi=None,
            measure="NVoxels" if "vol" in key.lower() else key,
            unit="voxel"
            if "vol" in key.lower()
            else "mm"
            if "Thickness" in key
            else None,
        )
        if str(voxkey) not in ants_cde:
            ants_cde["count"] += 1
            ants_cde[str(voxkey)] = {
                "id": f"{ants_cde['count']:0>6d}",
                "label": f"{key} ({voxkey.unit})",
            }
            if force_error:
                raise ValueError(f"Key {voxkey} not found in ANTS data elements file")
            changed = True
        if "vol" in key.lower():
            measures.append((f'{ants_cde[str(voxkey)]["id"]}', str(int(value))))
        else:
            measures.append((f'{ants_cde[str(voxkey)]["id"]}', str(value)))

        if "vol" in key.lower():
            volkey = ANTSDKT(
                structure=voxkey.structure, hemi=None, measure="Volume", unit="mm^3"
            )
            if str(volkey) not in ants_cde:
                ants_cde["count"] += 1
                ants_cde[str(volkey)] = {
                    "id": f"{ants_cde['count']:0>6d}",
                    "label": f"{key} ({volkey.unit})",
                }
                if force_error:
                    raise ValueError(
                        f"Key {volkey} not found in ANTS data elements file"
                    )
                changed = True
            measures.append((f'{ants_cde[str(volkey)]["id"]}', str(value * vox_size)))

    # iterate over columns in brain vols
    for row in ants_stats.iterrows():
        structure = None
        for key, val in row[1].items():
            if key == "Label":
                segid = int(val)
                structure = get_id_to_struct(segid)
                if structure is None:
                    raise ValueError(f"{int(val):d} did not return any structure")
                continue
            hemi, measure, unit = get_details(key, structure)
            key_tuple = ANTSDKT(
                structure=structure, hemi=hemi, measure=measure, unit=unit
            )
            label = f"{structure} {measure} ({unit})"
            if str(key_tuple) not in ants_cde:
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
            if "VolumeInVoxels" not in key:
                continue
            measures.append((f'{ants_cde[str(key_tuple)]["id"]}', str(val)))

            if "VolumeInVoxels" in key:
                measure = "Volume"
                unit = "mm^3"
                key_tuple = ANTSDKT(
                    structure=structure, hemi=hemi, measure=measure, unit=unit
                )
                label = f"{structure} {measure} ({unit})"
                if str(key_tuple) not in ants_cde:
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

    return measures, ants_cde


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
            ants_cde[key]["isAbout"] = s[hkey]["isAbout"]

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
    from nidm.core import Constants

    ants = Constants.ANTS
    nidm = Constants.NIDM

    g = rl.Graph()
    g.bind("ants", ants)
    g.bind("nidm", nidm)

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
    from nidm.core import Constants
    from nidm.experiment.Core import getUUID
    import prov

    ants = prov.model.Namespace("ants", str(Constants.ANTS))
    niiri = prov.model.Namespace("niiri", str(Constants.NIIRI))
    nidm = prov.model.Namespace("nidm", "http://purl.org/nidash/nidm#")
    doc = prov.model.ProvDocument()
    e = doc.entity(identifier=niiri[getUUID()])
    e.add_asserted_type(nidm["FSStatsCollection"])
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
