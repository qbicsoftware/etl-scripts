from __future__ import print_function

"""
@author: Sven Fillinger

Dropbox for data concerning the Molecular Tumor Board in Tübingen.

Basically handles to types of incoming data:
    1.) Raw data: Fastq files for tumor sample and blood
    2.) MTB archive: A ZIP archive containing 6 TSV. These contain information
        about SNVs, CNVs and metadata. The specification can be found on the mtbparser
        GitHub repository:
        https://github.com/qbicsoftware/qbic.mtbparser
        
1. Raw data
The sequencing facilities CeGaT and the human genetics department at UKT only see the QBiC barcode
of the tumor sample. However, as part of the whole MTB process, during patient registration in openBIS
an additional sample for PBMC is generated and attached as sample to the patient. It carries
an unique barcode, that is different from the tumor's one.

So in this dropbox we query the patient to whome the tumor sample barcode belongs and can access the
PBMC barcode from there. 

The incoming FASTQ file specification for CeGaT and human genetics for the MTB project (only!) is:

<QBiC-Barcode>_normal.1.fastq.gz
<QBiC-Barcode>_normal.2.fastq.gz
<QBiC-Barcode>_tumor.1.fastq.gz
<QBiC-Barcode>_tumor.2.fastq.gz


2. MTB Archive (zip)
Will be processed by qbicsoftware/qbic.mtbconverter (https://github.com/qbicsoftware/qbic.mtbconverter).
Please check the README on the GitHub repo.

Step 1 - 'convert' command: Takes the ZIP Archive and the patient ID and creates a XML file
    that is valid for the CentraXX XML Scheme.

Step 2 - 'push' command: Takes the XML and submits it to CentraXX

Note:
print statements go to: ~openbis/servers/datastore_server/log/startup_log.txt
"""
import re
import os
import sys
import mtbutils
import logging
import ConfigParser
import ch.systemsx.cisd.etlserver.registrator.api.v2
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SampleFetchOption
import ch.ethz.sis.openbis.generic.asapi.v3.dto.common.search.SearchResult
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.Sample
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.fetchoptions.SampleFetchOptions
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.search.SampleSearchCriteria
from ch.ethz.sis.openbis.generic.asapi.v3 import IApplicationServerApi
from ch.systemsx.cisd.common.spring import HttpInvokerUtils

# Path to checksum.py
sys.path.append('/home-link/qeana10/bin')

QCODE_REG = re.compile('Q\w{4}[0-9]{3}[a-zA-Z]\w')

PROPERTIES = '/etc/openbis.properties'

cmd_status = mtbutils.mtbconverter('-h')

# Print the return code from the subprocess command
#print('Return code is {}'.format(cmd_status))
if cmd_status != 0:
    raise mtbutils.MTBdropboxerror("Mtbconverter could not be loaded: " + cmd_status)

print(mtbutils.log_stardate("Mtbconverter executable found."))

config = ConfigParser.ConfigParser()
config.read(PROPERTIES)

print(config.get('openbis', 'url'))

api = HttpInvokerUtils.createServiceStub(IApplicationServerApi, config.get('openbis','url') + IApplicationServerApi.SERVICE_URL, 5000)
 
sessionToken = api.login(config.get('openbis','user'), config.get('openbis','password'))

def process(transaction):
    """The main dropbox funtion.
    openBIS executes this function during an incoming file event.
    """
    incoming_path = transaction.getIncoming().getAbsolutePath()
    file_name = transaction.getIncoming().getName()
    print(mtbutils.log_stardate('Incoming file event: {}'.format(file_name)))
    file_list = getfiles(incoming_path)
    getentityandpbmc(file_list[0], transaction)
    raise mtbutils.MTBdropboxerror('Diese Datei entfernst du nicht, openBIS')





def getfiles(path):
    """Retrieve all the absolute paths recursively from
    a given directory.

    Returns: list of abs file paths
    """
    if not os.path.isdir(path):
        raise mtbutils.MTBdropboxerror('The incoming data is not a directory.')
    file_list = []
    for path, subdirs, files in os.walk(path):
        for name in files:
            file_list.append(os.path.join(path, name))
    return file_list

def getentityandpbmc(path, transcation):
    qcode_findings = QCODE_REG.findall(path)
    if not qcode_findings:
        raise mtbutils.MTBdropboxerror('Could not find a barcode in {}.'.format(path))
    if len(qcode_findings) > 1:
        raise mtbutils.MTBdropboxerror('More than one barcode found barcode in {}.'.format(path))
    qcode = qcode_findings[0]

    entity_id = getentity(qcode, transcation)
    pbmc_id = getpbmc(entity_id, transcation)

    print(mtbutils.log_stardate('Found parent with id {}'.format(entity_id)))

    return (path, entity_id, pbmc_id)

def getentity(qcode, transaction):

    tumor_sample = getsample(qcode, transaction)
    parent_ids = tumor_sample.getParentSampleIdentifiers()
    
    grandparents_found = []
    for parent in parent_ids:
        grandparents_found.append(getsample(parent, transaction).getParentSampleIdentifiers())

    if not grandparents_found:
        raise mtbutils.MTBdropboxerror('No corresponding patient for tumor sample found.')
    if len(grandparents_found) > 1:
        raise mtbutils.MTBdropboxerror("More than one patient "
            "id found for tumor sample: {}".format(grandparents_found))  
    
    grandparent = grandparents_found[0][0]
    print(grandparent)

    return(grandparent.split('/')[-1])

def getpbmc(qcode_entity, transaction):
    
    descendand_samples = getallchildren(qcode_entity, transaction)
    print(descendand_samples)
    
    return ""

def getallchildren(qcode, transaction):
    """Fetch all children samples of a given
    sample code and return them as a list

    Returns: List of sample objects
    """
    sample = getsample(qcode, transaction)
    print(sample.getSample().getChildren())
    sserv = transaction.getSearchService()
    fetch_opt = SampleFetchOption()
    fetch_opt.withChildrenUsing(fetch_opt)

    scrit = SearchCriteria()
    scrit.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(
        SearchCriteria.MatchClauseAttribute.CODE, qcode))
    sserv.searchForSamples(scrit, fetch_opt)

    return sample


def getsample(qcode, transaction):
    sserv = transaction.getSearchService()
    scrit = SearchCriteria()
    scrit.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(
        SearchCriteria.MatchClauseAttribute.CODE, qcode))
    result = sserv.searchForSamples(scrit)
    
    if not result:
        raise mtbutils.MTBdropboxerror('No matching sample found in openBIS for code {}.'.format(qcode))
    if len(result) > 1:
        raise mtbutils.MTBdropboxerror('More than one sample found in openBIS for code {}.'.format(qcode))
    
    return result[0]

