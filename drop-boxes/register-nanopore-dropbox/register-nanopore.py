'''

Note:
print statements go to: ~openbis/servers/datastore_server/log/startup_log.txt
'''
import sys
sys.path.append('/home-link/qeana10/bin/')

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
from life.qbic.datamodel.datasets import OxfordNanoporeExperiment
from life.qbic.utils import NanoporeParser

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
QBIC_LOCATION = tracking_helper.get_qbic_location_json()

### We need this object to update the sample location later
SAMPLE_TRACKER = SampleTracker.createQBiCSampleTracker(SERVICE_REGISTRY_URL, SERVICE_CREDENTIALS, QBIC_LOCATION)

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
checksumMap = {}

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
        newExpID = '/' + space + '/' +project+ '/' + project+'E'+str(run)
        expExists = newExpID in usedExperimentIdentifiers
    usedExperimentIdentifiers.add(newExpID)
    return transaction.createNewExperiment(newExpID, NANOPORE_EXP_TYPE_CODE)

# return first line of file
def getDatahandlerMetadata(incomingPath, fileName):
    path = os.path.realpath(os.path.join(incomingPath, fileName))
    with open(path) as f:
        return f.readline()

def convertTime(input):
    input = input.split("+")[0] # time zone parsing does not work correctly in the jython version used here
    objDate = datetime.strptime(input, '%Y-%m-%dT%H:%M:%S.%f')
    return datetime.strftime(objDate,'%Y-%m-%d %H:%M:%S')

def getTimeStamp():
    now = datetime.now()
    ts = str(now.minute)+str(now.second)+str(now.microsecond)
    return ts

def copyLogFilesTo(logFiles, filePath, targetFolderPath):
    for logFile in logFiles:
        src = os.path.join(filePath, logFile.getName())
        shutil.copy2(src, targetFolderPath)
    copiedContent = os.listdir(targetFolderPath)
    if len(copiedContent) != len(logFiles):
        raise AssertionError("Not all log files have been copied successfully to target log folder.")

def createLogFolder(targetPath):
    ts = getTimeStamp()
    newLogFolder = os.path.join(targetPath, os.path.join(ts, "logs"))
    os.makedirs(newLogFolder)
    return newLogFolder

def createExperimentFromMeasurement(transaction, currentPath, space, project, measurement, origin, rawDataPerSample):
    # handle metadata of experiment level
    runExperiment = createNewExperiment(transaction, space, project)

    #do we get these automatically? from where?
    runExperiment.setPropertyValue("Q_ASIC_TEMPERATURE", measurement.getAsicTemp())
    runExperiment.setPropertyValue("Q_NGS_BASE_CALLER", measurement.getBaseCaller())
    runExperiment.setPropertyValue("Q_NGS_BASE_CALLER_VERSION", measurement.getBaseCallerVersion())
    runExperiment.setPropertyValue("Q_SEQUENCER_DEVICE", measurement.getDeviceType())
    runExperiment.setPropertyValue("Q_FLOWCELL_BARCODE", measurement.getFlowcellId())
    runExperiment.setPropertyValue("Q_FLOWCELL_POSITION", measurement.getFlowCellPosition())
    runExperiment.setPropertyValue("Q_FLOWCELL_TYPE", measurement.getFlowCellType())
    runExperiment.setPropertyValue("Q_LIBRARY_PREPKIT", measurement.getLibraryPreparationKit())
    runExperiment.setPropertyValue("Q_NANOPORE_HOSTNAME", measurement.getMachineHost())
    runExperiment.setPropertyValue("Q_DATA_GENERATION_FACILITY", origin)
    runExperiment.setPropertyValue("Q_MEASUREMENT_START_DATE", convertTime(measurement.getStartDate()))
    if measurement.getAdapter():
        runExperiment.setPropertyValue("Q_SEQUENCING_ADAPTER", measurement.getAdapter())
    # handle measured samples
    for barcode in rawDataPerSample.keySet():
        datamap = rawDataPerSample.get(barcode)
        newLogFolder = createLogFolder(currentPath)
        copyLogFilesTo(measurement.getLogFiles(), currentPath, newLogFolder)
        createSampleWithData(transaction, space, barcode, datamap, runExperiment, currentPath, newLogFolder)

# fills the global dictionary containing all checksums for paths from the global checksum file
def fillChecksumMap(checksumFilePath):
    with open(checksumFilePath, 'r') as chf:
        for line in chf:
            tokens = line.strip().split(" *")
            checksumMap[tokens[1]] = tokens[0]

# creates a file containing checksums and paths for files contained in the passed path using the global checksum dictionary
def createChecksumFileForFolder(incomingPath, folderPath):

    relativePath = os.path.relpath(folderPath, incomingPath)

    pathEnd = os.path.basename(os.path.normpath(folderPath))
    checksumFile = os.path.join(folderPath, pathEnd+'.sha256sum')

    with open(checksumFile, 'w') as f:
        for key, value in checksumMap.items():
            if key.startswith(relativePath):
                f.write(value+' *'+key+'\n')

def prepareDataFolder(incomingPath, currentPath, targetPath, dataObject, suffix):
    name = dataObject.getName()
    rel = dataObject.getRelativePath()
    src = os.path.join(os.path.dirname(currentPath), rel)
    createChecksumFileForFolder(incomingPath, src)
    target = os.path.join(targetPath, name+"_"+suffix)
    os.rename(src, target)

def createSampleWithData(transaction, space, parentSampleCode, mapWithDataForSample, openbisExperiment, currentPath, absLogPath):
    # needed to create relative path used in checksums file
    incomingPath = transaction.getIncoming().getAbsolutePath()

    search_service = transaction.getSearchService()
    sc = SearchCriteria()
    sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, parentSampleCode))
    foundSamples = search_service.searchForSamples(sc)
    parentID = foundSamples[0].getSampleIdentifier()

    sample = createNewSample(transaction, space, parentSampleCode)
    sample.setExperiment(openbisExperiment)
    sample.setParentSampleIdentifiers([parentID])

    topFolderFastq = os.path.join(currentPath, parentSampleCode+"_fastq")
    os.makedirs(topFolderFastq)

    fastqFail = mapWithDataForSample.get("fastqfail")
    prepareDataFolder(incomingPath, currentPath, topFolderFastq, fastqFail, "fail")
    fastqPass = mapWithDataForSample.get("fastqpass")
    prepareDataFolder(incomingPath, currentPath, topFolderFastq, fastqPass, "pass")

    topFolderFast5 = os.path.join(currentPath, parentSampleCode+"_fast5")
    os.makedirs(topFolderFast5)

    fast5Fail = mapWithDataForSample.get("fast5fail")
    prepareDataFolder(incomingPath, currentPath, topFolderFast5, fast5Fail, "fail")
    fast5Pass = mapWithDataForSample.get("fast5pass")
    prepareDataFolder(incomingPath, currentPath, topFolderFast5, fast5Pass, "pass")

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
        if currentPath.endswith('.sha256sum'):
            fillChecksumMap(currentPath)

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
        currentPath = os.path.join(nanoporeFolder, measurement.getRelativePath())
        createExperimentFromMeasurement(transaction, currentPath, space, projectCode, measurement, origin, rawData)
