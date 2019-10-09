#!/usr/bin/env python
#!/usr/bin/env python
#**************************************************************************************
#**************************************************************************************
#  ants_seg_to_nidm.py
#  License: GPL
#**************************************************************************************
#**************************************************************************************
# Date: June 6, 2019                 Coded by: Brainhack'ers
# Filename: ants_seg_to_nidm.py
#
# Program description:  This program will load in JSON output from FSL's FAST/FIRST
# segmentation tool, augment the FSL anatomical region designations with common data element
# anatomical designations, and save the statistics + region designations out as
# NIDM serializations (i.e. TURTLE, JSON-LD RDF)
#
#
#**************************************************************************************
# Development environment: Python - PyCharm IDE
#
#**************************************************************************************
# System requirements:  Python 3.X
# Libraries: PyNIDM,
#**************************************************************************************
# Start date: June 6, 2019
# Update history:
# DATE            MODIFICATION				Who
#
#
#**************************************************************************************
# Programmer comments:
#
#
#**************************************************************************************
#**************************************************************************************


from nidm.core import Constants
from nidm.experiment.Core import getUUID
from nidm.experiment.Core import Core
from prov.model import QualifiedName,PROV_ROLE, ProvDocument, PROV_ATTR_USED_ENTITY,PROV_ACTIVITY,PROV_AGENT,PROV_ROLE

from prov.model import Namespace as provNamespace

# standard library
from pickle import dumps
import os
from os.path import join,basename,splitext,isfile
from socket import getfqdn
import glob

import prov.model as prov
import json
import urllib.request as ur
from urllib.parse import urlparse
import re
import pandas as pd
import nibabel as nib

from rdflib import Graph, RDF, URIRef, util, term,Namespace,Literal,BNode

import tempfile

from segstats_jsonld import mapping_data

def loadfreesurferlookuptable(lookup_table):
    lookup_table_dic={}
    with open(lookup_table) as fp:
        line=fp.readline()
        cnt = 1
        while line:
            line_split = line.split()
            if len(line_split) < 2:
                line = fp.readline()
            else:
                lookup_table_dic[line_split[0]] = line_split[1]
                line = fp.readline()


    return lookup_table_dic

def url_validator(url):
    '''
    Tests whether url is a valide url
    :param url: url to test
    :return: True for valid url else False
    '''
    try:
        result = urlparse(url)
        return all([result.scheme, result.netloc, result.path])

    except:
        return False

def add_seg_data(nidmdoc, measure, json_map, subjid, png_file=None, output_file=None, root_act=None, nidm_graph=None):
    '''
    WIP: this function creates a NIDM file of brain volume data and if user supplied a NIDM-E file it will add brain volumes to the
    NIDM-E file for the matching subject ID
    :param nidmdoc:
    :param measure:
    :param header:
    :param json_map:
    :param png_file:
    :param root_act:
    :param nidm_graph:
    :return:
    '''

    niiri=prov.Namespace("niiri","http://iri.nidash.org/")
    #this function can be used for both creating a brainvolumes NIDM file from scratch or adding brain volumes to
    #existing NIDM file.  The following logic basically determines which route to take...

    #if an existing NIDM graph is passed as a parameter then add to existing file
    if nidm_graph is None:
        first_row=True

        #for each of the header items create a dictionary where namespaces are freesurfer
        software_activity = nidmdoc.graph.activity(niiri[getUUID()],other_attributes={Constants.NIDM_PROJECT_DESCRIPTION:"ANTS segmentation statistics"})

        #create software agent and associate with software activity
        #software_agent = nidmdoc.graph.agent(QualifiedName(provNamespace("niiri",Constants.NIIRI),getUUID()),other_attributes={
        software_agent = nidmdoc.graph.agent(niiri[getUUID()],other_attributes={
            QualifiedName(provNamespace("Neuroimaging_Analysis_Software",Constants.NIDM_NEUROIMAGING_ANALYSIS_SOFTWARE),""):Constants.ANTS ,
            prov.PROV_TYPE:prov.PROV["SoftwareAgent"]} )
        #create qualified association with brain volume computation activity
        nidmdoc.graph.association(activity=software_activity,agent=software_agent,other_attributes={PROV_ROLE:Constants.NIDM_NEUROIMAGING_ANALYSIS_SOFTWARE})
        # nidmdoc.graph.wasAssociatedWith(activity=software_activity,agent=software_agent)

        # create agent for participant
        subj_agent = nidmdoc.graph.agent(niiri[getUUID()],other_attributes={
           Constants.NIDM_SUBJECTID:subjid} )
        # create qualified associaton with brain volume computation activity
        nidmdoc.graph.association(activity=software_activity,agent=subj_agent,other_attributes={PROV_ROLE:Constants.NIDM_PARTICIPANT})
        # nidmdoc.graph.wasAssociatedWith(activity=software_activity,agent=subje_agent)



        #print(nidmdoc.serializeTurtle())

        # with open('measure.json', 'w') as fp:
        #    json.dump(measure, fp)

        # with open('json_map.json', 'w') as fp:
        #    json.dump(json_map, fp)


        #datum_entity=nidmdoc.graph.entity(QualifiedName(provNamespace("niiri",Constants.NIIRI),getUUID()),other_attributes={
        datum_entity=nidmdoc.graph.entity(niiri[getUUID()],other_attributes={
                    prov.PROV_TYPE:QualifiedName(provNamespace("nidm","http://purl.org/nidash/nidm#"),"ANTSStatsCollection")})
        nidmdoc.graph.wasGeneratedBy(datum_entity,software_activity)

        #iterate over measure dictionary where measures are the lines in the FS stats files which start with '# Measure' and
        #the whole table at the bottom of the FS stats file that starts with '# ColHeaders
        for measures in measure:

            #check if we have a CDE mapping for the anatomical structure referenced in the FS stats file
            # this part handles the case where FSL exports for csf is lowercase but anatomy term from InterLex / UBERON
            # is upper case (CSF)
            if measures["structure"].lower() in (name.lower() for name in json_map['Anatomy']):
                # hack because of the csf -> CSF problem
                if measures["structure"] == 'csf':
                    measures["structure"] = 'CSF'

                # for the various keys in the ANTS stats file
                for items in measures["items"]:
                    # if the
                    if items['name'] in json_map['Measures'].keys():

                        if not json_map['Anatomy'][measures["structure"]]['label']:
                            continue
                        #region_entity=nidmdoc.graph.entity(QualifiedName(provNamespace("niiri",Constants.NIIRI),getUUID()),other_attributes={prov.PROV_TYPE:
                        region_entity=nidmdoc.graph.entity(niiri[getUUID()],other_attributes={prov.PROV_TYPE:
                                QualifiedName(provNamespace("measurement_datum","http://uri.interlex.org/base/ilx_0738269#"),"")
                                })

                        #construct the custom CDEs to describe measurements of the various brain regions
                        # region_entity.add_attributes({QualifiedName(provNamespace("isAbout","http://uri.interlex.org/ilx_0381385#"),""):URIRef(json_map['Anatomy'][measures["structure"]]['isAbout']),
                        #            QualifiedName(provNamespace("hasLaterality","http://uri.interlex.org/ilx_0381387#"),""):json_map['Anatomy'][measures["structure"]]['hasLaterality'],
                        #            Constants.NIDM_PROJECT_DESCRIPTION:json_map['Anatomy'][measures["structure"]]['definition'],
                        #            QualifiedName(provNamespace("isMeasureOf","http://uri.interlex.org/ilx_0381389#"),""):QualifiedName(provNamespace("GrayMatter",
                        #            "http://uri.interlex.org/ilx_0104768#"),""),
                        #            QualifiedName(provNamespace("rdfs","http://www.w3.org/2000/01/rdf-schema#"),"label"):json_map['Anatomy'][measures["structure"]]['label']})

                        # DBK: removed isMeasureOf because it's statically coded and not correct for many cases
                        # get scheme+domain from isAbout url

                        # if hasLaterality isn't empty then store as an attribute
                        if json_map['Anatomy'][measures["structure"]]['hasLaterality'] != "":
                            region_entity.add_attributes({QualifiedName(provNamespace("hasLaterality","http://uri.interlex.org/ilx_0381387#"),""):json_map['Anatomy'][measures["structure"]]['hasLaterality']})

                        # if definition isn't empty then store as an attribute
                        if json_map['Anatomy'][measures["structure"]]['definition'] != "":
                            region_entity.add_attributes({Constants.NIDM_PROJECT_DESCRIPTION:json_map['Anatomy'][measures["structure"]]['definition']})

                        # if label isn't empty then store as an attribute
                        if json_map['Anatomy'][measures["structure"]]['label'] != "":
                             region_entity.add_attributes({QualifiedName(provNamespace("rdfs","http://www.w3.org/2000/01/rdf-schema#"),"label"):json_map['Anatomy'][measures["structure"]]['label']})

                        # if isAbout isn't empty then store as an attribute
                        if json_map['Anatomy'][measures["structure"]]['isAbout'] != "" :
                            isabout_parts = json_map['Anatomy'][measures["structure"]]['isAbout'].rsplit('/',1)
                            obo = prov.Namespace("obo",isabout_parts[0]+'/')
                            region_entity.add_attributes({QualifiedName(provNamespace("isAbout","http://uri.interlex.org/ilx_0381385#"),""):obo[isabout_parts[1]]})


                            #QualifiedName(provNamespace("hasUnit","http://uri.interlex.org/ilx_0381384#"),""):json_map['Anatomy'][measures["structure"]]['units'],
                            #print("%s:%s" %(key,value))

                        # DBK: Added to convert measureOf and datumType URLs to qnames
                        measureOf_parts = json_map['Measures'][items['name']]["measureOf"].rsplit('/',1)
                        datumType_parts = json_map['Measures'][items['name']]["datumType"].rsplit('/',1)

                        # if both measureOf and datumType have the same scheme+domain then set a "ilk" prefix for that
                        if measureOf_parts[0] == datumType_parts[0]:
                            ilk = prov.Namespace("ilk",measureOf_parts[0] + '/')
                            region_entity.add_attributes({QualifiedName(provNamespace("hasMeasurementType","http://uri.interlex.org/ilx_0381388#"),""):
                                ilk[measureOf_parts[1]], QualifiedName(provNamespace("hasDatumType","http://uri.interlex.org/ilx_0738262#"),""):
                                ilk[datumType_parts[1]]})
                        # if not then we'll add 2 separate prefixes
                        else:
                            measureOf = prov.Namespace("measureOf",measureOf_parts[0] + '/')
                            datumType = prov.Namespace("datumType",datumType_parts[0] + '/')
                            region_entity.add_attributes({QualifiedName(provNamespace("hasMeasurementType","http://uri.interlex.org/ilx_0381388#"),""):
                                measureOf[measureOf_parts[1]], QualifiedName(provNamespace("hasDatumType","http://uri.interlex.org/ilx_0738262#"),""):
                                datumType[datumType_parts[1]]})

                        # if this measure has a unit then use it
                        if "hasUnit" in  json_map['Measures'][items['name']]:
                            unit_parts = json_map['Measures'][items['name']]["hasUnit"].rsplit('/',1)
                            region_entity.add_attributes({QualifiedName(provNamespace("hasUnit","http://uri.interlex.org/base/ilx_0112181#"),""):json_map['Measures'][items['name']]["hasUnit"]})

                        # region_entity.add_attributes({QualifiedName(provNamespace("hasMeasurementType","http://uri.interlex.org/ilx_0381388#"),""):
                        #        json_map['Measures'][items['name']]["measureOf"], QualifiedName(provNamespace("hasDatumType","http://uri.interlex.org/ilx_0738262#"),""):
                        #        json_map['Measures'][items['name']]["datumType"]})

                        datum_entity.add_attributes({region_entity.identifier:items['value']})

    #else we're adding data to an existing NIDM file and attaching it to a specific subject identifier
    else:

            #search for prov:agent with this subject id

            #find subject ids and sessions in NIDM document
            query = """
                    PREFIX ndar:<https://ndar.nih.gov/api/datadictionary/v2/dataelement/>
                    PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                    PREFIX prov:<http://www.w3.org/ns/prov#>
                    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

                    select distinct ?agent
                    where {

                        ?agent rdf:type prov:Agent ;
                        ndar:src_subject_id \"%s\"^^xsd:string .

                    }""" % subjid
            print(query)
            qres = nidm_graph.query(query)
            for row in qres:
                print('Found subject ID: %s in NIDM file (agent: %s)' %(subjid,row[0]))

                #associate the brain volume data with this subject id but here we can't make a link between an acquisition
                #entity representing the T1w image because the Freesurfer *.stats file doesn't have the provenance information
                #to verify a specific image was used for these segmentations

                niiri=Namespace("http://iri.nidash.org/")
                nidm_graph.bind("niiri",niiri)



                software_activity = niiri[getUUID()]
                nidm_graph.add((software_activity,RDF.type,Constants.PROV['Activity']))
                nidm_graph.add((software_activity,Constants.DCT["description"],Literal("ANTS segmentation statistics")))
                fs = Namespace(Constants.FSL)


                #create software agent and associate with software activity
                #software_agent = nidmdoc.graph.agent(QualifiedName(provNamespace("niiri",Constants.NIIRI),getUUID()),other_attributes={
                software_agent = niiri[getUUID()]
                nidm_graph.add((software_agent,RDF.type,Constants.PROV['Agent']))
                neuro_soft=Namespace(Constants.NIDM_NEUROIMAGING_ANALYSIS_SOFTWARE)
                nidm_graph.add((software_agent,Constants.NIDM_NEUROIMAGING_ANALYSIS_SOFTWARE,URIRef(Constants.FSL)))
                nidm_graph.add((software_agent,RDF.type,Constants.PROV["SoftwareAgent"]))
                association_bnode = BNode()
                nidm_graph.add((software_activity,Constants.PROV['qualifiedAssociation'],association_bnode))
                nidm_graph.add((association_bnode,RDF.type,Constants.PROV['Agent']))
                nidm_graph.add((association_bnode,Constants.PROV['hadRole'],Constants.NIDM_NEUROIMAGING_ANALYSIS_SOFTWARE))
                nidm_graph.add((association_bnode,Constants.PROV['wasAssociatedWith'],software_agent))

                #create a blank node and qualified association with prov:Agent for participant
                #row[0]
                association_bnode = BNode()
                nidm_graph.add((software_activity,Constants.PROV['qualifiedAssociation'],association_bnode))
                nidm_graph.add((association_bnode,RDF.type,Constants.PROV['Agent']))
                nidm_graph.add((association_bnode,Constants.PROV['hadRole'],Constants.SIO["Subject"]))
                nidm_graph.add((association_bnode,Constants.PROV['wasAssociatedWith'],row[0]))

                #add freesurfer data
                datum_entity=niiri[getUUID()]
                nidm_graph.add((datum_entity, RDF.type, Constants.PROV['Entity']))
                nidm_graph.add((datum_entity,RDF.type,Constants.NIDM["ANTSStatsCollection"]))
                nidm_graph.add((datum_entity, Constants.PROV['wasGeneratedBy'], software_activity))

                #iterate over measure dictionary where measures are the lines in the FS stats files which start with '# Measure' and
                #the whole table at the bottom of the FS stats file that starts with '# ColHeaders
                for measures in measure:

                    #check if we have a CDE mapping for the anatomical structure referenced in the FS stats file
                    if measures["structure"] in json_map['Anatomy']:

                        #for the various fields in the FS stats file row starting with '# Measure'...
                        for items in measures["items"]:
                            # if the
                            if items['name'] in json_map['Measures'].keys():

                                if not json_map['Anatomy'][measures["structure"]]['label']:
                                    continue
                                #region_entity=nidmdoc.graph.entity(QualifiedName(provNamespace("niiri",Constants.NIIRI),getUUID()),other_attributes={prov.PROV_TYPE:


                                # here we're adding measurement_datum entities.  Let's check to see if we already
                                # have appropriate ones in the NIDM file.  If we do then we can just link to those
                                # entities

                                query = """
                                    PREFIX ndar:<https://ndar.nih.gov/api/datadictionary/v2/dataelement/>
                                    PREFIX rdf:<http://www.w3.org/1999/02/22-rdf-syntax-ns#>
                                    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                                    PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
                                    PREFIX hasDatumType: <http://uri.interlex.org/ilx_0738262#>
                                    PREFIX hasLaterality: <http://uri.interlex.org/ilx_0381387#>
                                    PREFIX hasMeasurementType: <http://uri.interlex.org/ilx_0381388#>
                                    PREFIX iq_measure: <https://github.com/dbkeator/nidm-local-terms/issues/60>
                                    PREFIX isAbout: <http://uri.interlex.org/ilx_0381385#>
                                    PREFIX isMeasureOf: <http://uri.interlex.org/ilx_0381389#>
                                    PREFIX measurement_datum: <http://uri.interlex.org/base/ilx_0738269#>

                                    select distinct ?region_entity
                                    where {

                                        ?region_entity rdf:type measurement_datum: ;
                                            rdfs:label \"%s\" ;
                                            hasDatumType: <%s> ;
                                            isAbout: <%s> ;
                                            hasLaterality: \"%s\" ;
                                            hasMeasurementType: <%s> .
                                        } """ %(json_map['Anatomy'][measures["structure"]]['label'],
                                                json_map['Measures'][items['name']]["datumType"],
                                                json_map['Anatomy'][measures["structure"]]['isAbout'],
                                                json_map['Anatomy'][measures["structure"]]['hasLaterality'],
                                                json_map['Measures'][items['name']]["measureOf"])
                                # execute query
                                # print("searching for existing measurement datum for structure: %s"
                                #      % json_map['Anatomy'][measures["structure"]]['label'])
                                # print(query)
                                qres = nidm_graph.query(query)

                                # check if we have an entity reference returned.  If so, use it else create the entity
                                # needed.
                                if len(qres) >= 1:
                                    # found one or more unique measurement datum so use the first one since they
                                    # are all identical and not sure why they are replicated
                                    for row in qres:
                                        # print("measurement datum entity found: %s" %row)
                                        # parse url
                                        region_entity=URIRef(niiri[str(row[0]).rsplit('/',1)[1]])

                                else:
                                    # nothing found so create
                                    # print("measurement datum entity not found, creating...")
                                    region_entity=URIRef(niiri[getUUID()])

                                    measurement_datum = Namespace("http://uri.interlex.org/base/ilx_0738269#")
                                    nidm_graph.bind("measurement_datum",measurement_datum)

                                    nidm_graph.add((region_entity,RDF.type,Constants.PROV['Entity']))
                                    nidm_graph.add((region_entity,RDF.type,URIRef(measurement_datum)))

                                    #construct the custom CDEs to describe measurements of the various brain regions
                                    isAbout = Namespace("http://uri.interlex.org/ilx_0381385#")
                                    nidm_graph.bind("isAbout",isAbout)
                                    hasLaterality = Namespace("http://uri.interlex.org/ilx_0381387#")
                                    nidm_graph.bind("hasLaterality",hasLaterality)

                                    # if isAbout isn't empty then store as an attribute
                                    if json_map['Anatomy'][measures["structure"]]['isAbout'] != "":
                                        isabout_parts = json_map['Anatomy'][measures["structure"]]['isAbout'].rsplit('/',1)
                                        obo = Namespace(isabout_parts[0]+'/')
                                        nidm_graph.bind("obo",obo)
                                        nidm_graph.add((region_entity,URIRef(isAbout),obo[isabout_parts[1]]))

                                    # if hasLaterality isn't empty then store as an attribute
                                    if json_map['Anatomy'][measures["structure"]]['hasLaterality'] != "":
                                        nidm_graph.add((region_entity,URIRef(hasLaterality),Literal(json_map['Anatomy'][measures["structure"]]['hasLaterality'])))
                                    # if definition isn't empty then store as an attribute
                                    if json_map['Anatomy'][measures["structure"]]['definition'] != "":
                                        nidm_graph.add((region_entity,Constants.DCT["description"],Literal(json_map['Anatomy'][measures["structure"]]['definition'])))

                                    # DBK: removed isMeasureOf because it's statically coded and not correct for many cases
                                    # isMeasureOf = Namespace("http://uri.interlex.org/ilx_0381389#")
                                    # nidm_graph.bind("isMeasureOf",isMeasureOf)
                                    # GrayMatter = Namespace("http://uri.interlex.org/ilx_0104768#")
                                    # nidm_graph.bind("GrayMatter",GrayMatter)
                                    # nidm_graph.add((region_entity,URIRef(isMeasureOf),URIRef(GrayMatter)))

                                    # if label isn't empty then store as an attribute
                                    if json_map['Anatomy'][measures["structure"]]['label'] != "":
                                        nidm_graph.add((region_entity,Constants.RDFS['label'],Literal(json_map['Anatomy'][measures["structure"]]['label'])))

                                    hasMeasurementType = Namespace("http://uri.interlex.org/ilx_0381388#")
                                    nidm_graph.bind("hasMeasurementType",hasMeasurementType)
                                    hasDatumType = Namespace("http://uri.interlex.org/ilx_0738262#")
                                    nidm_graph.bind("hasDatumType",hasDatumType)
                                    hasUnit = Namespace("http://uri.interlex.org/base/ilx_0112181#")
                                    nidm_graph.bind("hasUnit",hasUnit)

                                     # DBK: Added to convert measureOf and datumType URLs to qnames
                                    measureOf_parts = json_map['Measures'][items['name']]["measureOf"].rsplit('/',1)
                                    datumType_parts = json_map['Measures'][items['name']]["datumType"].rsplit('/',1)

                                    # if both measureOf and datumType have the same scheme+domain then set a "ilk" prefix for that
                                    if measureOf_parts[0] == datumType_parts[0]:
                                        ilk = Namespace(measureOf_parts[0] + '/')
                                        nidm_graph.bind("ilk",ilk)
                                        nidm_graph.add((region_entity,URIRef(hasMeasurementType),ilk[measureOf_parts[1]]))
                                        nidm_graph.add((region_entity,URIRef(hasDatumType),ilk[datumType_parts[1]]))

                                    # if not then we'll add 2 separate prefixes
                                    else:
                                        measureOf = Namespace(measureOf_parts[0] + '/')
                                        nidm_graph.bind("measureOf",measureOf)
                                        datumType = Namespace(datumType_parts[0] + '/')
                                        nidm_graph.bind("datumType",datumType)

                                        region_entity.add_attributes({QualifiedName(provNamespace("hasMeasurementType","http://uri.interlex.org/ilx_0381388#"),""):
                                            measureOf[measureOf_parts[1]], QualifiedName(provNamespace("hasDatumType","http://uri.interlex.org/ilx_0738262#"),""):
                                            datumType[datumType_parts[1]]})

                                    # nidm_graph.add((region_entity,URIRef(hasMeasurementType),URIRef(json_map['Measures'][items['name']]["measureOf"])))
                                    # nidm_graph.add((region_entity,URIRef(hasDatumType),URIRef(json_map['Measures'][items['name']]["datumType"])))

                                    # if this measure has a unit then use it
                                    if "hasUnit" in json_map['Measures'][items['name']]:
                                        nidm_graph.add((region_entity,URIRef(hasUnit),Literal(json_map['Measures'][items['name']]["hasUnit"])))

                                #create prefixes for measurement_datum objects for easy reading
                                #nidm_graph.bind(Core.safe_string(Core,string=json_map['Anatomy'][measures["structure"]]['label']),region_entity)

                                nidm_graph.add((datum_entity,region_entity,Literal(items['value'])))

                                # testing
                                #nidm_graph.serialize(destination="/Users/dbkeator/Downloads/test_fsl_add.ttl",format='turtle')
                                #print("output testing TTL file...")



def test_connection(remote=False):
    """helper function to test whether an internet connection exists.
    Used for preventing timeout errors when scraping interlex."""
    import socket
    remote_server = 'www.google.com' if not remote else remote # TODO: maybe improve for China
    try:
        # does the host name resolve?
        host = socket.gethostbyname(remote_server)
        # can we establish a connection to the host name?
        con = socket.create_connection((host, 80), 2)
        return True
    except:
        print("Can't connect to a server...")
        pass
    return False

def read_ants_stats(ants_stats_file,ants_brainvols_file,mri_file,freesurfer_lookup_table):
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

    fs_lookup_table = loadfreesurferlookuptable(freesurfer_lookup_table)

    # open stats file, brain vols file as pandas dataframes
    ants_stats = pd.read_csv(ants_stats_file)
    brain_vols = pd.read_csv(ants_brainvols_file)

    # load mri_file and extract voxel sizes
    img = nib.load(mri_file)
    vox_size = img.header.get_zooms()


    measures=[]

    # iterate over columns in brain vols
    for i, j in brain_vols.iterrows():

        # just do this for BVOL, GVol, and WVol columns

        measures.append({'structure': j.index.values[1], 'items': []})
        # add to measures list
        measures[-1]['items'].append({
            'name' : 'Volume_mm3',
            'description' : 'Volume',
            'value' : j.values[1] * vox_size[1],  # assumes isotropic voxels
            'units' : 'mm^3'})


        measures.append({'structure': j.index.values[2], 'items': []})
        # add to measures list
        measures[-1]['items'].append({
            'name' : 'Volume_mm3',
            'description' : 'Volume',
            'value' : j.values[2] * vox_size[1],  # assumes isotropic voxels
            'units' : 'mm^3'})

        measures.append({'structure': j.index.values[3], 'items': []})
        # add to measures list
        measures[-1]['items'].append({
            'name' : 'Volume_mm3',
            'description' : 'Volume',
            'value' : j.values[3] * vox_size[1],  # assumes isotropic voxels
            'units' : 'mm^3'})


    # iterate over columns in brain vols
    for i, j in ants_stats.iterrows():

        # map all labels that existin in freesurfer lookup tables
        if str(int(j.values[0])) in fs_lookup_table.keys():
            measures.append({'structure': fs_lookup_table[str(int(j.values[0]))], 'items': []})
            # add to measures list
            measures[-1]['items'].append({
                'name' : 'Volume_mm3',
                'description' : 'Volume',
                'value' : j.values[1] * vox_size[1],  # assumes isotropic voxels
                'units' : 'mm^3'})


    return measures




def main():

    import argparse
    parser = argparse.ArgumentParser(prog='ants_seg_to_nidm.py',
                                     description='''This program will load in the ReproNim-style ANTS brain
                                        segmentation outputs, augment the ANTS anatomical region designations with common data element
                                        anatomical designations, and save the statistics + region designations out as
                                        NIDM serializations (i.e. TURTLE, JSON-LD RDF)''',formatter_class=argparse.RawDescriptionHelpFormatter)

    parser.add_argument('-f', '--ants_stats', dest='stats_files',required=True, type=str,help='''A comma separated string of paths to: ANTS \"lablestats\" CSV
                            file (col 1=Label number, col 2=VolumeinVoxels), ANTS \"brainvols\" CSV file (col 2=BVOL, col 3=GVol, col 4=WVol),
                            MRI or Image file to extract voxel sizes OR A comma separated string of path \n OR URLs to a ANTS segmentation files and
                                an image file for voxel sizes: /..../antslabelstats,/..../antsbrainvols,/..../mri_file. \n Note, currently this is tested
                                on ReproNim data''')
    parser.add_argument('-subjid','--subjid',dest='subjid',required=False, help='If a path to a URL or a stats file'
                            'is supplied via the -f/--seg_file parameters then -subjid parameter must be set with'
                            'the subject identifier to be used in the NIDM files')
    parser.add_argument('-fslut','--fslut',dest='fslut',required=True, help='Freesurfer lookuptable LUT file')
    parser.add_argument('-jmap', '--json_map', dest='json_map', required=True,
                        help='If provided, json information will be used instead of scraping InterLex')
    parser.add_argument('-o', '--output_dir', dest='output_dir', type=str,
                        help='Output directory', required=True)
    parser.add_argument('-j', '--jsonld', dest='jsonld', action='store_true', default = False,
                        help='If flag set then NIDM file will be written as JSONLD instead of TURTLE')
    parser.add_argument('-n','--nidm', dest='nidm_file', type=str, required=False,
                        help='Optional NIDM file to add segmentation data to.')
    args = parser.parse_args()

    # test whether user supplied stats file directly and if so they the subject id must also be supplied so we
    # know which subject the stats file is for
    if (args.stats_files and (args.subjid is None)):
        parser.error("-f/--ants_urls and -d/--ants_stats requires -subjid/--subjid to be set!")


    #if user supplied json mapping file
    if args.json_map is not False:
        # read json_map into json map structure
        with open(args.json_map) as json_file:
            json_map = json.load(json_file)

    # WIP: trying to find a way to reference data in module. This does not feel kosher but works
    #datapath = mapping_data.__path__._path[0] + '/'
    # changed by DBK
    datapath = mapping_data.__path__[0] + '/'

    # if we set -s or --subject_dir as parameter on command line...
    # if args.stats_files is not None:

    #    #if user added -jmap parameter
    #    if args.json_map is not False:
            #read in stats file
    #        tableinfo = json.load(args.data_file)
    #    else:
            # online scraping of InterLex for anatomy CDEs and stats file reading
            # [measures,json_map] = remap2json(xlsxfile=join(datapath,'ReproNimCDEs.xlsx'),
            #                     fsl_stat_file=args.data_file,outfile=join(os.path.dirname(os.path.realpath(__file__)),"mapping_data","fslmap.json"))

    #        [measures,json_map] = remap2json(xlsxfile=join(datapath,'ReproNimCDEs.xlsx'),
    #                                 fsl_stat_file=args.stats_files)


        # for measures we need to create NIDM structures using anatomy mappings
        # If user has added an existing NIDM file as a command line parameter then add to existing file for subjects who exist in the NIDM file
    #    if args.nidm_file is None:

    #        print("Creating NIDM file...")
            # If user did not choose to add this data to an existing NIDM file then create a new one for the CSV data

            # create an empty NIDM graph
    #        nidmdoc = Core()

            # print(nidmdoc.serializeTurtle())

            # add seg data to new NIDM file
    #        add_seg_data(nidmdoc=nidmdoc,measure=measures,json_map=json_map,subjid=args.subjid)

            #serialize NIDM file
    #        if args.jsonld is not False:
    #            with open(join(args.output_dir,splitext(basename(args.data_file))[0]+'.json'),'w') as f:
    #                print("Writing NIDM file...")
    #                f.write(nidmdoc.serializeJSONLD())
    #        else:
    #            with open(join(args.output_dir,splitext(basename(args.data_file))[0]+'.ttl'),'w') as f:
    #                print("Writing NIDM file...")
    #                f.write(nidmdoc.serializeTurtle())

    #        nidmdoc.save_DotGraph(join(args.output_dir,splitext(basename(args.data_file))[0] + ".pdf"), format="pdf")


    # WIP: ANTS URL forms as comma spearated string in args.stats_urls:
    # 1: https://fcp-indi.s3.amazonaws.com/data/Projects/ABIDE/Outputs/mindboggle_swf/mindboggle/ants_subjects/sub-0050002/antslabelstats.csv
    # 2: https://fcp-indi.s3.amazonaws.com/data/Projects/ABIDE/Outputs/mindboggle_swf/mindboggle/ants_subjects/sub-0050002/antsbrainvols.csv
    # 3: https://fcp-indi.s3.amazonaws.com/data/Projects/ABIDE/Outputs/mindboggle_swf/mindboggle/ants_subjects/sub-0050002/antsBrainSegmentation.nii.gz

    # split input string argument into the 3 URLs above
    url_list = args.stats_files.split(',')


    # check url 1, the labelstats.csv
    url = url_validator(url_list[0])
    # if user supplied a url as a segfile
    if url is not False:

        #try to open the url and get the pointed to file...for labelstats file
        try:
            #open url and get file
            opener = ur.urlopen(url_list[0])
            # write temporary file to disk and use for stats
            temp = tempfile.NamedTemporaryFile(delete=False)
            temp.write(opener.read())
            temp.close()
            labelstats= temp.name
        except:
            print("ERROR! Can't open url: %s" %url)
            exit()

        #try to open the url and get the pointed to file...for brainvols file
        try:
            #open url and get file
            opener = ur.urlopen(url_list[1])
            # write temporary file to disk and use for stats
            temp = tempfile.NamedTemporaryFile(delete=False)
            temp.write(opener.read())
            temp.close()
            brainvols= temp.name
        except:
            print("ERROR! Can't open url: %s" %url)
            exit()

         #try to open the url and get the pointed to file...for brainvols file
        try:
            #open url and get file
            opener = ur.urlopen(url_list[2])
            # write temporary file to disk and use for stats
            temp = tempfile.NamedTemporaryFile(delete=False, suffix=".nii.gz")
            temp.write(opener.read())
            temp.close()
            imagefile= temp.name

        except:
            print("ERROR! Can't open url: %s" %url)
            exit()

    # else these must be a paths to the stats files
    else:

        # split input string argument into the 3 URLs above
        file_list = args.stats_files.split(',')

        labelstats=file_list[0]
        brainvol=file_list[1]
        imagefile=file_list[2]




    #if user added -jmap parameter
    if args.json_map is not False:
        #read in stats file
            measures = read_ants_stats(labelstats,brainvols,imagefile,args.fslut)
    else:
        # online scraping of InterLex for anatomy CDEs and stats file reading
            [measures,json_map] = remap2json(xlsxfile=join(datapath,'ReproNimCDEs.xlsx'),
                                 fsl_stat_file=stats_file)


    # for measures we need to create NIDM structures using anatomy mappings
    # If user has added an existing NIDM file as a command line parameter then add to existing file for subjects who exist in the NIDM file
    if args.nidm_file is None:

        print("Creating NIDM file...")

        # name output NIDM file by subjid
        # args.subjid + "_" + [everything after the last / in the first supplied URL]
        output_filename = args.subjid + "_NIDM"
        # If user did not choose to add this data to an existing NIDM file then create a new one for the CSV data

        # create an empty NIDM graph
        nidmdoc = Core()

        add_seg_data(nidmdoc=nidmdoc,measure=measures, json_map=json_map,subjid=args.subjid)

        #serialize NIDM file
        if args.jsonld is not False:
            with open(join(args.output_dir,output_filename +'.json'),'w') as f:
                print("Writing NIDM file...")
                f.write(nidmdoc.serializeJSONLD())
        else:
            with open(join(args.output_dir,output_filename + '.ttl'),'w') as f:
                print("Writing NIDM file...")
                f.write(nidmdoc.serializeTurtle())

        #nidmdoc.save_DotGraph(join(args.output_dir,output_filename + ".pdf"), format="pdf")
    # we adding these data to an existing NIDM file
    else:
        #read in NIDM file with rdflib
        rdf_graph = Graph()
        rdf_graph_parse = rdf_graph.parse(args.nidm_file,format=util.guess_format(args.nidm_file))

        #search for prov:agent with this subject id
        #associate the brain volume data with this subject id but here we can't make a link between an acquisition
        #entity representing the T1w image because the Freesurfer *.stats file doesn't have the provenance information
        #to verify a specific image was used for these segmentations
        add_seg_data(nidmdoc=rdf_graph_parse,measure=measures,json_map=json_map,nidm_graph=rdf_graph_parse,subjid=args.subjid)

        #serialize NIDM file
        #if args.jsonld is not False:
        #   print("Writing NIDM file...")
        #    rdf_graph_parse.serialize(destination=join(args.output_dir,output_filename + '.json'),format='json-ld')

        print("Writing NIDM file...")
        rdf_graph_parse.serialize(destination=args.nidm_file,format='turtle')





if __name__ == "__main__":
    main()
