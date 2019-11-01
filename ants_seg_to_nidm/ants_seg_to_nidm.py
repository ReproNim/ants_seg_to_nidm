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

from pathlib import Path

from rdflib import Graph, RDF, URIRef, util, term,Namespace,Literal,BNode
from ants_seg_to_nidm.antsutils import read_ants_stats, create_cde_graph, convert_stats_to_nidm
from io import StringIO

import tempfile

# cde_file = Path(os.path.dirname(__file__)) / "mapping_data" / "ants-cdes.ttl"



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

def add_seg_data(nidmdoc,subjid,stats_entity_id, add_to_nidm=False):
    '''
    WIP: this function creates a NIDM file of brain volume data and if user supplied a NIDM-E file it will add brain volumes to the
    NIDM-E file for the matching subject ID
    :param nidmdoc:
    :param header:
    :param add_to_nidm:
    :return:
    '''


    #for each of the header items create a dictionary where namespaces are freesurfer
    niiri=Namespace("http://iri.nidash.org/")
    nidmdoc.bind("niiri",niiri)



    software_activity = niiri[getUUID()]
    nidmdoc.add((software_activity,RDF.type,Constants.PROV['Activity']))
    nidmdoc.add((software_activity,Constants.DCT["description"],Literal("ANTS segmentation statistics")))
    fs = Namespace(Constants.ANTS)


    #create software agent and associate with software activity
    #search and see if a software agent exists for this software, if so use it, if not create it
    for software_uid in nidmdoc.subjects(predicate=Constants.NIDM_NEUROIMAGING_ANALYSIS_SOFTWARE,object=URIRef(Constants.ANTS) ):
        software_agent = software_uid
        break
    else:
        software_agent = niiri[getUUID()]
    nidmdoc.add((software_agent,RDF.type,Constants.PROV['Agent']))
    neuro_soft=Namespace(Constants.NIDM_NEUROIMAGING_ANALYSIS_SOFTWARE)
    nidmdoc.add((software_agent,Constants.NIDM_NEUROIMAGING_ANALYSIS_SOFTWARE,URIRef(Constants.ANTS)))
    nidmdoc.add((software_agent,RDF.type,Constants.PROV["SoftwareAgent"]))
    association_bnode = BNode()
    nidmdoc.add((software_activity,Constants.PROV['qualifiedAssociation'],association_bnode))
    nidmdoc.add((association_bnode,RDF.type,Constants.PROV['Agent']))
    nidmdoc.add((association_bnode,Constants.PROV['hadRole'],Constants.NIDM_NEUROIMAGING_ANALYSIS_SOFTWARE))
    nidmdoc.add((association_bnode,Constants.PROV['wasAssociatedWith'],software_agent))

    if not add_to_nidm:
        # create a new agent for subjid
        participant_agent = niiri[getUUID()]
        nidmdoc.add((participant_agent,RDF.type,Constants.PROV['Agent']))
        nidmdoc.add((participant_agent,URIRef(Constants.NIDM_SUBJECTID.uri),Literal(subjid)))

    else:
        # query to get agent id for subjid
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
            #print(query)
            qres = nidmdoc.query(query)
            if len(qres) == 0:
                print('Subject ID (%s) was not found in existing NIDM file.  No output written...' %subjid)
                exit()
            else:
                 for row in qres:
                    print('Found subject ID: %s in NIDM file (agent: %s)' %(subjid,row[0]))
                    participant_agent = row[0]

    #create a blank node and qualified association with prov:Agent for participant
    association_bnode = BNode()
    nidmdoc.add((software_activity,Constants.PROV['qualifiedAssociation'],association_bnode))
    nidmdoc.add((association_bnode,RDF.type,Constants.PROV['Agent']))
    nidmdoc.add((association_bnode,Constants.PROV['hadRole'],Constants.SIO["Subject"]))
    nidmdoc.add((association_bnode,Constants.PROV['wasAssociatedWith'],participant_agent))

    # add association between ANTSStatsCollection and computation activity
    nidmdoc.add((URIRef(stats_entity_id.uri),Constants.PROV['wasGeneratedBy'],software_activity))


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
            brainvol= temp.name
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


    measures = read_ants_stats(labelstats,brainvol,imagefile)
    [e,doc] = convert_stats_to_nidm(measures)
    g = create_cde_graph()

    # convert nidm stats graph to rdflib
    g2 = Graph()
    g2.parse(source=StringIO(doc.serialize(format='rdf',rdf_format='turtle')),format='turtle')


    # for measures we need to create NIDM structures using anatomy mappings
    # If user has added an existing NIDM file as a command line parameter then add to existing file for subjects who exist in the NIDM file
    if args.nidm_file is None:

        print("Creating NIDM file...")

        # name output NIDM file by subjid
        # args.subjid + "_" + [everything after the last / in the first supplied URL]
        output_filename = args.subjid + "_NIDM"
        # If user did not choose to add this data to an existing NIDM file then create a new one for the CSV data


        nidmdoc = g+g2

        # print(nidmdoc.serializeTurtle())

        # add seg data to new NIDM file
        add_seg_data(nidmdoc=nidmdoc,subjid=args.subjid,stats_entity_id=e.identifier)

        #serialize NIDM file
        print("Writing NIDM file...")
        if args.jsonld is not False:
            nidmdoc.serialize(destination=join(args.output_dir,output_filename +'.json'),format='jsonld')
        else:
            nidmdoc.serialize(destination=join(args.output_dir,output_filename +'.ttl'),format='turtle')


        #nidmdoc.save_DotGraph(join(args.output_dir,output_filename + ".pdf"), format="pdf")
    # we adding these data to an existing NIDM file
    else:
        #read in NIDM file with rdflib
        print("Reading in NIDM graph....")
        g1 = Graph()
        g1.parse(args.nidm_file,format=util.guess_format(args.nidm_file))

        print("Combining graphs...")
        nidmdoc = g + g1 + g2

        add_seg_data(nidmdoc=nidmdoc,subjid=args.subjid,stats_entity_id=e.identifier,add_to_nidm=True)


        #serialize NIDM file
        print("Writing Augmented NIDM file...")
        if args.jsonld is not False:
            nidmdoc.serialize(destination=args.nidm_file + '.json',format='jsonld')
        else:
            nidmdoc.serialize(destination=args.nidm_file,format='turtle')




if __name__ == "__main__":
    main()
