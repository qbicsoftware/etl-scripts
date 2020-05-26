'''

Note:
print statements go to: ~openbis/servers/datastore_server/log/startup_log.txt
'''
import sys
sys.path.append('/home-link/qeana10/bin/')
#sys.path.append('/home/qeana10/openbis/servers/core-plugins/QBIC/1/dss/drop-boxes/register-nanopore-dropbox/lib/data-model-lib-1.8.0.jar')
#sys.path.append('/home/qeana10/openbis/servers/core-plugins/QBIC/1/dss/drop-boxes/register-nanopore-dropbox/lib/core-utils-lib-1.4.0.jar')

import checksum
import re
import os
import shutil
from datetime import datetime
import ch.systemsx.cisd.etlserver.registrator.api.v2
from java.io import File
from org.apache.commons.io import FileUtils
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria

# Parsing related imports
from java.nio.file import Paths
from life.qbic.utils import NanoporeParser
from life.qbic.datamodel.datasets import OxfordNanoporeExperiment
from life.qbic.utils import NanoporeParser

######## Sample Tracking related import
#from life.qbic.sampletracking import SampleTracker
#from life.qbic.sampletracking import ServiceCredentials
#from java.net import URL

#import sample_tracking_helper_qbic as tracking_helper

######## imports for fastq/5 file validation
#import subprocess

#### Setup Sample Tracking service
#SERVICE_CREDENTIALS = ServiceCredentials()
#SERVICE_CREDENTIALS.user = tracking_helper.get_service_user()
#SERVICE_CREDENTIALS.password = tracking_helper.get_service_password()
#SERVICE_REGISTRY_URL = URL(tracking_helper.get_service_reg_url())
#QBIC_LOCATION = tracking_helper.get_qbic_location_json()

### We need this object to update the sample location later
#SAMPLE_TRACKER = SampleTracker.createQBiCSampleTracker(SERVICE_REGISTRY_URL, SERVICE_CREDENTIALS, QBIC_LOCATION)

# ETL script for registration of VCF files
# expected:
# *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
pattern = re.compile('Q\w{4}[0-9]{3}[a-xA-X]\w')
alt_pattern = re.compile('Q\w{4}ENTITY-[1-9][0-9]*')

NANOPORE_EXP_TYPE_CODE = "Q_NGS_NANOPORE_RUN"
NANOPORE_SAMPLE_TYPE_CODE = "Q_NGS_NANOPORE_SINGLE_SAMPLE_RUN"
NANOPORE_LOG_CODE = "Q_NGS_NANOPORE_RUN_LOGS"
NANOPORE_FASTQ_CODE = "Q_NGS_NANOPORE_RUN_FASTQ"
NANOPORE_FAST5_CODE = "Q_NGS_NANOPORE_RUN_FAST5"
NANOPORE_SAMPLE_PREFIX = "NGS"

# needed for pooled samples with multiple measurements
usedSampleIdentifiers = set()
usedExperimentIdentifiers = set()

def createNewSample(transaction, space, parentSampleCode):
    run = 0
    sampleExists = True
    newSampleID = None
    while sampleExists:
        run += 1
        newSampleID = '/' + space + '/' + NANOPORE_SAMPLE_PREFIX + str(run) + parentSampleCode
        sampleExists = transaction.getSampleForUpdate(newSampleID) or newSampleID in usedSampleIdentifiers
    usedSampleIdentifiers.add(newSampleID)
    return transaction.createNewSample(newSampleID, NANOPORE_SAMPLE_TYPE_CODE)

def createNewExperiment(transaction, space, project):
    search_service = transaction.getSearchService()
    existingExperiments = search_service.listExperiments("/" + space + "/" + project)
    for eexp in existingExperiments:
        usedExperimentIdentifiers.add(eexp.getExperimentIdentifier())
    run = len(usedExperimentIdentifiers)
    expExists = True
    newExpID = None
    while expExists:
        run += 1
        newExpID = '/' + space + '/' +project+ '/' + project+str(run)
        expExists = newExpID in usedExperimentIdentifiers
    usedExperimentIdentifiers.add(newExpID)
    return transaction.createNewExperiment(newExpID, NANOPORE_EXP_TYPE_CODE)

# return first line of file
def getDatahandlerMetadata(incomingPath, fileName):
    path = os.path.realpath(os.path.join(incomingPath, fileName))
    with open(path) as f:
        return f.readline()

def getTimeStamp():
    now = datetime.now()
    ts = str(now.minute)+str(now.second)+str(now.microsecond)
    return ts

# return absolute path of copied folder containing all log files. used to add log files to each involved sample
def copyLogs(parentPath, fileList):
    ts = getTimeStamp()
    newLogFolder = os.path.join(parentPath, ts+"/logs")
    os.makedirs(newLogFolder)
    for logFile in fileList:
        src = os.path.join(parentPath, logFile)
        shutil.copy2(src, newLogFolder)
    return newLogFolder

def handleMeasurement(transaction, space, project, measurement, origin, rawDataPerSample):
    #reminder: incoming path is of the absolute path of the folder created by the datahandler.
    #joining this path with any relative path returned by the nanopore object will give the absolute path of that file/folder.
    incomingPath = transaction.getIncoming().getAbsolutePath()
    currentPath = os.path.join(incomingPath, measurement.getRelativePath())
    # handle metadata of experiment level
    runExperiment = createNewExperiment(transaction, space, project)

    #do we get these automatically? from where?
    runExperiment.setPropertyValue("Q_ASIC_TEMPERATURE", measurement.getAsicTemp())
    runExperiment.setPropertyValue("Q_NGS_BASE_CALLER", measurement.getBaseCaller()+ " " +measurement.getBaseCallerVersion())
    runExperiment.setPropertyValue("Q_SEQUENCER_DEVICE", measurement.getDeviceType())
    runExperiment.setPropertyValue("Q_FLOWCELL_BARCODE", measurement.getFlowcellId())
    runExperiment.setPropertyValue("Q_FLOWCELL_POSITION", measurement.getFlowCellPosition())
    runExperiment.setPropertyValue("Q_FLOWCELL_TYPE", measurement.getFlowCellType())
    runExperiment.setPropertyValue("Q_LIBRARY_PREPKIT", measurement.getLibraryPreparationKit())
    runExperiment.setPropertyValue("Q_NGS_NANOPORE_HOSTNAME", measurement.getMachineHost())
    runExperiment.setPropertyValue("Q_DATA_GENERATION_FACILITY", origin)
    runExperiment.setPropertyValue("Q_MEASUREMENT_START_DATE", measurement.getStartDate())
    # runExperiment.setPropertyValue("Q_EXTERNALDB_ID",) best skip and parse sample information at sample level, no experiment-wide ID from what I can tell
    # handle measured samples
    for barcode in rawDataPerSample.keySet():
        datamap = rawDataPerSample.get(barcode)
        print measurement.getLogFiles()
        newLogFolder = copyLogs(currentPath, measurement.getLogFiles())
        handleSingleSample(transaction, space, barcode, datamap, runExperiment, currentPath, newLogFolder)

def handleSingleSample(transaction, space, parentSampleCode, mapWithDataForSample, openbisExperiment, currentPath, absLogPath):
    sample = createNewSample(transaction, space, parentSampleCode)
    sample.setExperiment(openbisExperiment)
    #sample.setPropertyValue("Q_EXTERNALDB_ID",) this should already be set for the parent. where do we get it on this level, if it's needed?

    topFolderFastq = os.path.join(currentPath, parentSampleCode+"_fastq")
    print currentPath
    print os.path.exists(currentPath)
    print os.listdir(currentPath)
    os.makedirs(topFolderFastq)
    folder = mapWithDataForSample.get("fastqfail")
    name = folder.getName()
    src = os.path.join(currentPath, name)
    print src
    print os.path.exists(src)
    #print topFolderFastq
    #print os.path.exists(topFolderFastq)
    os.rename(src, topFolderFastq+'/'+name)

    folder = mapWithDataForSample.get("fastqpass")
    name = folder.getName()
    src = os.path.join(currentPath, folder.getName())
    os.rename(src, topFolderFastq+'/'+name)

    topFolderFast5 = os.path.join(currentPath, parentSampleCode+"_fast5")
    os.makedirs(topFolderFast5)
    folder = mapWithDataForSample.get("fast5pass")
    name = folder.getName()
    src = os.path.join(currentPath, folder.getName())
    os.rename(src, topFolderFast5+'/'+name)

    folder = mapWithDataForSample.get("fast5fail")
    name = folder.getName()
    src = os.path.join(currentPath, folder.getName())
    os.rename(src, topFolderFast5+'/'+name)

    fast5DataSet = transaction.createNewDataSet(NANOPORE_FAST5_CODE)
    fastQDataSet = transaction.createNewDataSet(NANOPORE_FASTQ_CODE)
    fast5DataSet.setSample(sample)
    fastQDataSet.setSample(sample)
    transaction.moveFile(topFolderFast5, fast5DataSet)
    transaction.moveFile(topFolderFastq, fastQDataSet)

    logDataSet = transaction.createNewDataSet(NANOPORE_LOG_CODE)
    logDataSet.setSample(sample)
    transaction.moveFile(absLogPath, logDataSet)

    # Updates the sample location of the measured sample
    SAMPLE_TRACKER.updateSampleLocationToCurrentLocation(parentSampleCode)

def process(transaction):
    context = transaction.getRegistrationContext().getPersistentMap()

    # Get the incoming path of the transaction
    incomingPath = transaction.getIncoming().getAbsolutePath()

    key = context.get("RETRY_COUNT")
    if (key == None):
        key = 1

    # Get metadata from datahandler as well as the original facility object
    nanoporeFolder = None
    for f in os.listdir(incomingPath):
        currentPath = os.path.realpath(os.path.join(incomingPath,f))
        if os.path.isdir(currentPath):
            nanoporeFolder = currentPath

    origin = getDatahandlerMetadata(incomingPath, "source_dropbox.txt")
    # Use file structure parser to create structure object
    nanoporeObject = NanoporeParser.parseFileStructure(Paths.get(nanoporeFolder))
    sampleCode = nanoporeObject.getSampleCode()

    search_service = transaction.getSearchService()
    sc = SearchCriteria()
    sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, sampleCode))
    found_samples = search_service.searchForSamples(sc)
    sample = found_samples[0]
    space = sample.getSpace()
    projectCode = sampleCode[:5]
    for measurement in nanoporeObject.getMeasurements():
        rawData = measurement.getRawDataPerSample(nanoporeObject)
        handleMeasurement(transaction, space, projectCode, measurement, origin, rawData)

def validateFastq(filePath):
    """
    This function validates fastq files using the 'fastq_info' tool
    of the set of utilities 'fastq_utils'
    (https://github.com/nunofonseca/fastq_utils).

    This function assumes 'fastq_utils' is installed

    Example:
        validateFastq('10xv1a_I1.fastq.gz')

    Args:
        filePath (string): the path to the fastq file to validate

    Returns:
        Boolean: True if 'fastq_info' exited succesfully (i.e. exit code 0),
                False otherwise.
    """

    import subprocess

    success_flag = False

    cmd = "fastq_info " + filePath

    proc = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            shell=True,
                            universal_newlines=True)

    std_out, std_err = proc.communicate()

    if int(proc.returncode) == 0:
        success_flag = True

    return success_flag


def validateFast5(filePath, schemaPath='fast5_test_data/schema.yml'):
    """
    This function validates fast5 files using the 'H5 Validator' tool
    (https://github.com/nanoporetech/ont_h5_validator).

    This function assumes 'H5 Validator' is installed

    Example:
        validateFast5('fast5_test_data/test.fast5', 'fast5_test_data/schema.yml')

    Args:
        filePath (string): the path to the fast5 file to validate
        schemaPath (string): path to the file schema maintained by Oxford Nanopore Technologies

    Returns:
        Boolean: True if the 'H5 Validator' exited succesfully (i.e. exit code 0),
                False otherwise.
    """

    import subprocess

    success_flag = False

    cmd = "h5_validate " + schemaPath + ' ' + filePath + ' -v'

    proc = subprocess.Popen(cmd,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            shell=True,
                            universal_newlines=True)

    std_out, std_err = proc.communicate()

    if int(proc.returncode) == 0:
        success_flag = True

    return success_flag
