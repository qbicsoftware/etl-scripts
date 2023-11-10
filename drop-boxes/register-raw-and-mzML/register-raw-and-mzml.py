"""
This etl script registers a pair consisting of a proteomics raw file or folder and its converted mzML and registers
both to the same openBIS sample. Some metadata is parsed from the mzML and stored.

Incoming raw files (of different vendor formats) are supported.

This script reuses some logic from the raw file conversion etl script

The stdout of this file is redirected to
`~openbis/servers/datastore_server/log/startup_log.txt`
"""

import sys
sys.path.append('/home-link/qeana10/bin/')
import os
import time
import re
import subprocess
import datetime
import xml.etree.ElementTree
from functools import partial
import ch.systemsx.cisd.etlserver.registrator.api.v2
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import (
    SearchCriteria, SearchSubCriteria
)
######## Sample Tracking related import
from life.qbic.sampletracking import SampleTracker
from life.qbic.sampletracking import ServiceCredentials
from java.net import URL

import sample_tracking_helper_qbic as tracking_helper
#### Setup Sample Tracking service
SERVICE_CREDENTIALS = ServiceCredentials()
SERVICE_CREDENTIALS.user = tracking_helper.get_service_user()
SERVICE_CREDENTIALS.password = tracking_helper.get_service_password()
SERVICE_REGISTRY_URL = URL(tracking_helper.get_service_reg_url())
DATA_AVAILABLE_JSON = tracking_helper.get_data_available_status_json()

### We need this object to update the sample status later
SAMPLE_TRACKER = SampleTracker.createLocationIndependentSampleTracker(SERVICE_REGISTRY_URL, SERVICE_CREDENTIALS)

# *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
barcode_pattern = re.compile('Q[a-zA-Z0-9]{4}[0-9]{3}[A-Z][a-zA-Z0-9]')
ms_pattern = re.compile('MS[0-9]*Q[A-Z0-9]{4}[0-9]{3}[A-Z][A-Z0-9]')
ms_prefix_pattern = re.compile('MS[0-9]*')

VENDOR_FORMAT_EXTENSIONS = {'.raw':'RAW_THERMO', '.d':'D_BRUKER','.wiff':'WIFF_SCIEX'}
WATERS_FORMAT = "RAW_WATERS"

def extract_barcode(filename):
    """Extract valid barcodes from the filename.

    Return project_id, experiment_id and the whole barcode.
    If no barcode was found, raise a ValueError. If the file
    contains a qbic barcode with an invalid checksum, raise
    a RuntimeError.

    """
    try:
        code = barcode[0:-1]
        return checksum.checksum(code) == barcode[-1]
    except:
        return True

def parse_timestamp_easy(mzml_path):
    with open(mzml_path, 'r') as mzml:
        time = None
        for line in mzml:
            if "<run id=" in line:
                for token in line.split(" "):
                    if "startTimeStamp=" in token:
                        xsdDateTime = token.split('"')[1]
                        time = datetime.datetime.strptime(xsdDateTime, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
                break
        return time

def parse_instrument_accession(mzml_path):
    with open(mzml_path, 'r') as mzml:
        accession = None
        out = True
        for line in mzml:
            if "<instrumentConfigurationList" in line or 'id="CommonInstrumentParams">' in line:
                out = False
            if "</referenceableParamGroup>" in line or "</instrumentConfiguration>" in line:
                out = True
            if not out and '<cvParam cvRef="MS"' in line:
                line = line.split(" ")
                for token in line:
                    if "accession=" in token:
                        accession = token.split('"')[1]
                break
        print "accession for "+mzml_path+": "+accession
        return accession

def parse_timestamp_from_mzml(mzml_path):
    schema = '{http://psi.hupo.org/ms/mzml}'
    for event, element in xml.etree.ElementTree.iterparse(mzml_path):
        if element.tag == schema+'run':
            xsdDateTime = element.get('startTimeStamp')
            element.clear()
            break
        element.clear() # remove unused xml elements
    time = None
    try:
        time = datetime.datetime.strptime(xsdDateTime, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
    except TypeError:
        print "no startTimeStamp found"
    return time

def createSimilarMSExperiment(tr, space, project, existing):
    ID_STRING = "/{space}/{project}/{project}E{number}"
    numberOfExperiments = len(existing)
    newExpID = ID_STRING.format(space = space, project = project, number = numberOfExperiments)
    while newExpID in existing:
        numberOfExperiments += 1 
        newExpID = ID_STRING.format(space = space, project = project, number = numberOfExperiments)
    existing.append(newExpID)
    newExp = tr.createNewExperiment(newExpID, "Q_MS_MEASUREMENT")
    newExp.setPropertyValue('Q_CURRENT_STATUS', 'FINISHED')
    newExp.setPropertyValue('Q_ADDITIONAL_INFO', "Automatically created experiment: instrument ID did not fit existing experiment")
    return newExp

def createSimilarMSSample(tr, space, exp, properties, parents):
    ID_STRING = "/{space}/MS{run}{code}"
    code = None
    for p in parents:
        code = p.split("/")[2]
    run = 0
    sampleExists = True
    newSampleID = None
    while sampleExists:
        run += 1
        newSampleID = ID_STRING.format(space = space, run = run, code = code)
        sampleExists = tr.getSampleForUpdate(newSampleID)
    newMSSample = tr.createNewSample(newSampleID, "Q_MS_RUN")
    newMSSample.setParentSampleIdentifiers(parents)
    newMSSample.setExperiment(exp)
    newMSSample.setPropertyValue('Q_PROPERTIES', properties)
    return newMSSample 


def handleSampleTracking(barcode):
    wait_seconds = 1
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            SAMPLE_TRACKER.updateSampleStatus(barcode, DATA_AVAILABLE_JSON)
            break
        except:
            print "Updating location for sample "+barcode+" failed on attempt "+str(attempt+1)
            if attempt < max_attempts -1:
                time.sleep(wait_seconds)
                continue
            else:
                raise

def createRawDataSet(transaction, incomingPath, sample, format, time_stamp):
    rawDataSet = transaction.createNewDataSet("Q_MS_RAW_DATA")
    rawDataSet.setPropertyValue("Q_MS_RAW_VENDOR_TYPE", format)
    if time_stamp:
        rawDataSet.setPropertyValue("Q_MEASUREMENT_START_DATE", time_stamp)
    rawDataSet.setMeasuredData(False)
    rawDataSet.setSample(sample)
    transaction.moveFile(incomingPath, rawDataSet)

def GZipAndMoveMZMLDataSet(transaction, filepath, sample, file_exists = False):
    mzmlDataSet = transaction.createNewDataSet("Q_MS_MZML_DATA")
    #TODO more properties from mzml?
    time_stamp = parse_timestamp_easy(filepath)

    mzmlDataSet.setMeasuredData(False)
    mzmlDataSet.setSample(sample)
    if time_stamp:
        mzmlDataSet.setPropertyValue("Q_MEASUREMENT_START_DATE", time_stamp)
    if not file_exists:
        subprocess.call(["gzip", "-f", filepath])
    zipped = filepath+".gz"
    transaction.moveFile(zipped, mzmlDataSet)
    return time_stamp

'''Metadata extraction written by Chris. Support for batch upload (no metadata here) by Andreas'''
def process(transaction):

    context = transaction.getRegistrationContext().getPersistentMap()

    # Get the incoming path of the transaction
    incomingPath = transaction.getIncoming().getAbsolutePath()

    key = context.get("RETRY_COUNT")
    if (key == None):
        key = 1

    # Get the name of the incoming file
    name = transaction.getIncoming().getName()
    dataPath = os.path.join(incomingPath, name)

    code = barcode_pattern.findall(name)[0]
    if extract_barcode(code):
        project = code[:5]
        experiment = code[1:5]
        parentCode = code[:10]
    else:
        raise ValueError("Invalid barcode: %s" % code)

    search_service = transaction.getSearchService()
    sc = SearchCriteria()
    sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, code))
    found = search_service.searchForSamples(sc)
    space = found[0].getSpace()
    # find existing experiments
    existingExperimentIDs = []
    existingExperiments = search_service.listExperiments("/" + space + "/" + project)
    for eexp in existingExperiments:
        existingExperimentIDs.append(eexp.getExperimentIdentifier())

    raw_path = None
    mzml_path = None
    openbis_format_code = None
    for root, subFolders, files in os.walk(incomingPath):
        if subFolders:
            subFolder = subFolders[0]
        for f in files:
            stem, ext = os.path.splitext(f)
            if ext.lower()=='.mzml':
                if mzml_path:
                    raise ValueError("More than one mzML found. Only one pair of raw data and mzML can be registered at a time")
                mzml_path = f
            if ext.lower() in VENDOR_FORMAT_EXTENSIONS:
                if raw_path:
                    raise ValueError("More than one raw file found. Only one pair of raw data and mzML can be registered at a time")
                raw_path = f
                openbis_format_code = VENDOR_FORMAT_EXTENSIONS[ext.lower()]
    if not raw_path or not mzml_path:
        raise ValueError("Did not find pair of raw data and mzML data - make sure both are contained in the folder")
    prefixes = ms_prefix_pattern.findall(name)
    if prefixes:
        prefix = prefixes[0]
        for p in prefixes:
            if len(p) > prefix:
                prefix = p
        ms_code = prefix+code
    else:
        ms_code = "MS"+code
    sc = SearchCriteria()
    sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, ms_code))
    foundSamples = search_service.searchForSamples(sc)
    ms_samp = transaction.getSampleForUpdate(foundSamples[0].getSampleIdentifier())

    raw_path = os.path.join(dataPath, raw_path)
    mzml_path = os.path.join(dataPath, mzml_path)

    instrument_accession = parse_instrument_accession(mzml_path)
    if instrument_accession:
        expID = ms_samp.getExperiment().getExperimentIdentifier()
        exp = transaction.getExperimentForUpdate(expID)
        old_accession = exp.getPropertyValue('Q_ONTOLOGY_INSTRUMENT_ID')
        if old_accession and old_accession != instrument_accession:
            print "Found instrument accession "+instrument_accession+" in mzML, but "+old_accession+" in experiment! Creating new sample and experiment."
            parents = foundSamples[0].getParentSampleIdentifiers()
            properties = ms_samp.getPropertyValue("Q_PROPERTIES")
            newExp = createSimilarMSExperiment(transaction, space, project, existingExperimentIDs)
            ms_samp = createSimilarMSSample(transaction, space, newExp, properties, parents)
            newExp.setPropertyValue('Q_ONTOLOGY_INSTRUMENT_ID', instrument_accession)
        else:
            exp.setPropertyValue('Q_ONTOLOGY_INSTRUMENT_ID', instrument_accession)
    time_stamp = GZipAndMoveMZMLDataSet(transaction, mzml_path, ms_samp)
    createRawDataSet(transaction, raw_path, ms_samp, openbis_format_code, time_stamp)
    handleSampleTracking(parentCode)
