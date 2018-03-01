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
from ch.ethz.sis.openbis.generic.asapi.v3.dto.common.search import SearchResult
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.Sample
from ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.fetchoptions import SampleFetchOptions
from ch.ethz.sis.openbis.generic.asapi.v3 import IApplicationServerApi
from ch.systemsx.cisd.common.spring import HttpInvokerUtils
from ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.search import SampleSearchCriteria


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

api = HttpInvokerUtils.createServiceStub(IApplicationServerApi, config.get('openbis','url') + IApplicationServerApi.SERVICE_URL, 5000)
 
sessionToken = api.login(config.get('openbis','user'), config.get('openbis','password'))

if sessionToken:
    mtbutils.log_stardate("Successfully authenticated against the openBIS API.")
else:
    raise mtbutils.MTBdropboxerror("Could not authenticate against openBIS.")

def process(transaction):
    """The main dropbox funtion.
    openBIS executes this function during an incoming file event.
    """
    incoming_path = transaction.getIncoming().getAbsolutePath()
    file_name = transaction.getIncoming().getName()
    print(mtbutils.log_stardate('Incoming file event: {}'.format(file_name)))
    # Iterate through the incoming path and get all files
    file_list = getfiles(incoming_path)
    
    # Determine the types of incoming files and route the process
    unknown_file_types = []
    for in_file in file_list:
        if 'fastq' in in_file:
            proc_fastq(in_file)
        if in_file.endswith('.zip'):
            proc_mtb(in_file)
        else:
            unknown_file_types.append(in_file)

    # Check, if there are files of unknown type left
    if unknown_file_types:
        for file_name in unknown_file_types:
            mtbutils.log_stardate('Unknown file type: {}'.format(file_name))
        raise mtbutils.MTBdropboxerror('We have found files that could not be processed!'
            'Manual intervention needed.')

    mtbutils.log_stardate('Processing finished.')

def proc_fastq(fastq_file):
    """Register fastq as dataset in openBIS"""
    qbiccode = QCODE_REG.findall(fastq_file)
    if not qbiccode:
        raise mtbutils.MTBdropboxerror('No QBiC Barcode found in {}'.format(fastq_file))
    if len(qbiccode) > 1:
        raise mtbutils.MTBdropboxerror('More than one QBiC Barcode found in {}'.format(fastq_file))
    space = space_and_project(qbiccode[0])
    pass

def space_and_project(qbiccode):
    """Determines the space and project of a given
    sample id.
    
    Returns: Tuple (space, project)
    """
    sample = getsamplev3(qbiccode)

    return "",""


def proc_mtb(zip_archive):
    """Register archive and submit to CentraXX"""
    pass

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
    """Parses the QBiC barcode from the incoming file path
    and requests the corresponding QBiC patient id and
    PBMC sample id for further processing the registration 
    of tumor and blood sequencing data.

    Returns: Tuple of (path, entity_id, pbmc_id)
    """
    qcode_findings = QCODE_REG.findall(path)
    if not qcode_findings:
        raise mtbutils.MTBdropboxerror('Could not find a barcode in {}.'.format(path))
    if len(qcode_findings) > 1:
        raise mtbutils.MTBdropboxerror('More than one barcode found barcode in {}.'.format(path))
    qcode = qcode_findings[0]

    entity_id = getentity(qcode, transcation)
    pbmc_id = getpbmc(entity_id, transcation)

    print(mtbutils.log_stardate('Found parent with id {}'.format(entity_id)))
    print(mtbutils.log_stardate('Found corresponding PBMC id {}'.format(pbmc_id)))

    return (path, entity_id, pbmc_id)

def getentity(qcode, transaction):
    """Find the corresponding patient id (Q_BIOLOGICAL_ENTITY)
    for a given tumor sample DNA (Q_TEST_SAMPLE)
    
    Returns: The qbic barcode for the patient
    """
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
    
    return(grandparent.split('/')[-1])

def getpbmc(qcode_entity, transaction):
    """Searches for corresponding blood samples (type PBMC)
    for a given patient ID. It is expected, that a given patient
    has only one DNA sample extracted from PBMC as reference for
    somatic variant calling.

    Returns: The QBiC barcode of the corresponding PBMC Q_TEST_SAMPLE
    """
    pbmc_samples = []
    descendand_samples = getallchildren(qcode_entity)
    for sample in descendand_samples:
        if sample.getProperty('Q_PRIMARY_TISSUE') == 'PBMC':
            pbmc_samples.append(sample)
    if not pbmc_samples:
        raise mtbutils.MTBdropboxerror("Could not find any PBMC sample.")
    if len(pbmc_samples) > 1:
        raise mtbutils.MTBdropboxerror("More than 1 PBMC sample found for entity {}"
            .format(qcode_entity))
    
    # Get the id of the attached Q_TEST_SAMPLE
    pbmc_id = ""
    try:
        children = pbmc_samples[0].getChildren()
        pbmc_id = children[0].getCode()
    except Exception as exc:
        mtbutils.MTBdropboxerror("Could not access Q_TEST_SAMPLE code for PBMC in {}"
            .format(qcode_entity))    
    return pbmc_id

def getallchildren(qcode):
    """Fetch all children samples of a given
    sample code and return them as a list

    Returns: List of sample objects
    """ 
    fetch_opt = SampleFetchOptions()
    fetch_opt.withChildrenUsing(fetch_opt)
    fetch_opt.withProperties()
	
    scrit = SampleSearchCriteria()
    scrit.withCode().thatEquals(qcode)
   
    children_samples = []

    result = api.searchSamples(sessionToken, scrit, fetch_opt)
    
    for sample in result.getObjects():
        # Q_BIOLOGICAL_SAMPLE level
        for kid in sample.getChildren():
            children_samples.append(kid)
            # Q_TEST_SAMPLE level
            for grandkid in kid.getChildren():
                children_samples.append(grandkid)

    return children_samples


def getsample(qcode, transaction):
    """Resturns a immutable sample object for a given
    QBIC barcode."""
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

def getsamplev3(qcode):
    """Get a sample object of a given identifier
    in API V3 style

    Returns: A sample (v3) object
    """
    scrit = SampleSearchCriteria()
    scrit.withCode().thatEquals(qcode)

    fetchOptions = SampleFetchOptions()
    fetchOptions.withProperties()

    result = api.searchSamples(sessionToken, scrit, fetchOptions)
    samples = []
    for sample in result.getObjects():
        samples.append(sample)
    if len(sample) > 1:
        raise mtbutils.MTBdropboxerror('More than one sample found with identifier {}'.format(qcode))
    return sample[0]
    

