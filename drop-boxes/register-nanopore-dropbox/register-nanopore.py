'''

Note:
print statements go to: ~openbis/servers/datastore_server/log/startup_log.txt
'''
import sys
sys.path.append('/home-link/qeana10/bin/')

import checksum
import re
import time
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
DATA_AVAILABLE_JSON = tracking_helper.get_data_available_status_json()

### We need this object to update the sample status later
SAMPLE_TRACKER = SampleTracker.createLocationIndependentSampleTracker(SERVICE_REGISTRY_URL, SERVICE_CREDENTIALS)

# ETL script for registration of VCF files
# expected:
# *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
pattern = re.compile('Q\w{4}[0-9]{3}[a-xA-X]\w')
alt_pattern = re.compile('Q\w{4}ENTITY-[1-9][0-9]*')

NANOPORE_EXP_TYPE_CODE = "Q_NGS_NANOPORE_RUN"
NANOPORE_SAMPLE_TYPE_CODE = "Q_NGS_NANOPORE_SINGLE_SAMPLE_RUN"
NANOPORE_LOG_CODE = "Q_NGS_NANOPORE_RUN_LOGS"
NANOPORE_SAMPLE_PREFIX = "NGS"

NANOPORE_DATASET_CODE_DICT = {
  "fast5": "Q_NGS_NANOPORE_RUN_FAST5", 
  "fastq": "Q_NGS_NANOPORE_RUN_FASTQ", 
  "pod5": "Q_NGS_NANOPORE_RUN_POD5"
}

# needed for pooled samples with multiple measurements
usedSampleIdentifiers = set()
usedExperimentIdentifiers = set()
checksumMap = {}

# facilities that want us to remove certain log files
blacklistedByFacility = {"qeana03-imgagdna": ["DutyTimeLog", "FinalSummaryLog", "ReportMdLog", "ReportPDFLog", "ThroughputLog", "DriftCorrectionLog", "MuxScanDataLog"]}

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

def copyFileTo(file, filePath, targetFolderPath):
    sourcePath = os.path.join(filePath, file.getName())
    shutil.copy2(sourcePath, targetFolderPath)
    src = os.path.join(filePath, file.getName())
    shutil.copy2(src, targetFolderPath)

# copies log files from a folder that may contain other files to another path
# log files that are blacklisted are not copied and thus not registered (after metadata extraction)
def copyLogFilesTo(logFiles, filePath, targetFolderPath, facilityName):
    # return list of files to remove for this facility or empty list
    blacklist = blacklistedByFacility.get(facilityName, [])
    numIgnoredFiles = 0
    for logFile in logFiles:
        fileType = logFile.__class__.__name__
        if fileType in blacklist:
            numIgnoredFiles += 1
        else :
            copyFileTo(logFile, filePath, targetFolderPath)
    copiedContent = os.listdir(targetFolderPath)
    if len(copiedContent) + numIgnoredFiles != len(logFiles):
        raise AssertionError("Not all log files have been copied successfully to target log folder.")

def createLogFolder(targetPath):
    ts = getTimeStamp()
    newLogFolder = os.path.join(targetPath, os.path.join(ts, "logs"))
    os.makedirs(newLogFolder)
    return newLogFolder

def containsUnclassifiedData(unclassifiedMap):
    return not all(v is None for v in unclassifiedMap.values())

def createExperimentFromMeasurement(transaction, currentPath, space, project, measurement, origin, rawDataPerSample):
    """ Register the experiment with samples in openBIS.
    In order to register the Nanopore experiment with its measurements in openBIS, 
    we need to perform the following steps:
    
    1.) Create a new experiment in openBIS
    2.) Enrich it with metadata about the sequencing run (base caller, adapter, library kit, etc.)
    3.) Aggregate all log files into an own log folder per measurement
    4.) Create a new sample for every measurement
    
    The Map rawDataPerSample contains all DataFolders per sample code:
    
    [
       "QBiC sample id":
           [
            "fast5fail": DataFolder,
            "fast5pass": DataFolder,
            "fastqfail": DataFolder,
            "fastqpass": DataFolder
           ],
      "Other sample id":   // In case of pooled samples
         ...
    ]
    """
    runExperiment = createNewExperiment(transaction, space, project)

    # 2.) Enrich it with metadata about the sequencing run (base caller, adapter, library kit, etc.)
    runExperiment.setPropertyValue("Q_ASIC_TEMPERATURE", measurement.getAsicTemp())
    runExperiment.setPropertyValue("Q_NGS_BASE_CALLER", measurement.getBaseCaller())
    runExperiment.setPropertyValue("Q_NGS_BASE_CALLER_VERSION", measurement.getBaseCallerVersion())
    runExperiment.setPropertyValue("Q_SEQUENCER_DEVICE", measurement.getDeviceType())
    # TODO flow cell position might not be available for MINION - check for MINION in the future and set to default value
    runExperiment.setPropertyValue("Q_FLOWCELL_BARCODE", measurement.getFlowcellId())
    runExperiment.setPropertyValue("Q_FLOWCELL_POSITION", measurement.getFlowCellPosition())
    runExperiment.setPropertyValue("Q_FLOWCELL_TYPE", measurement.getFlowCellType())
    # TODO parsing issue with new library prep kit: this should be fixed in core-utils-lib!
    prepkitStripped = measurement.getLibraryPreparationKit().split(":")[0]
    runExperiment.setPropertyValue("Q_LIBRARY_PREPKIT", prepkitStripped)
    runExperiment.setPropertyValue("Q_NANOPORE_HOSTNAME", measurement.getMachineHost())
    runExperiment.setPropertyValue("Q_DATA_GENERATION_FACILITY", origin)
    runExperiment.setPropertyValue("Q_MEASUREMENT_START_DATE", convertTime(measurement.getStartDate()))
    if measurement.getAdapter():
        runExperiment.setPropertyValue("Q_SEQUENCING_ADAPTER", measurement.getAdapter())
    # handle measured samples
    for barcode in rawDataPerSample.keySet():
        datamap = rawDataPerSample.get(barcode)
        newLogFolder = createLogFolder(currentPath)
        # 3.) Aggregate all log files into an own log folder per measurement
        copyLogFilesTo(measurement.getLogFiles(), currentPath, newLogFolder, origin)
        createSampleWithData(transaction, space, barcode, datamap, runExperiment, currentPath, newLogFolder)
    # TODO find out and fix why this call can lead to nullpointer exception:
    unclassifiedMap = measurement.getUnclassifiedData()
    if containsUnclassifiedData(unclassifiedMap):
        registerUnclassifiedData(transaction, unclassifiedMap, runExperiment, currentPath, measurement.getFlowcellId())

# fills the global dictionary containing all checksums for paths from the global checksum file
def fillChecksumMap(checksumFilePath):
    with open(checksumFilePath, 'r') as chf:
        for line in chf:
            # remove asterisk from paths, so they can be compared later on
            tokens = line.strip().split(" *")
            path = tokens[1]
            checksum = tokens[0]
            checksumMap[path] = checksum

# creates a file containing checksums and paths for files contained in the passed path using the global checksum dictionary
def createChecksumFileForFolder(incomingPath, folderPath):

    relativePath = os.path.relpath(folderPath, incomingPath)

    pathEnd = os.path.basename(os.path.normpath(folderPath))
    checksumFilePath = os.path.join(folderPath, pathEnd+'.sha256sum')
    if not os.path.isfile(checksumFilePath):
        with open(checksumFilePath, 'w') as f:
            for key, value in checksumMap.items():
                # for each file in our dictionary that starts with the currently handled path, we add the known checksums and the paths, along with the asterisk we removed earlier
                if key.startswith(relativePath):
                    f.write(value+' *'+key+'\n')
    return checksumFilePath

# prepares unclassified data folder (e.g. unclassified fast5_pass) including checksums and moves folder to target destination folder
def prepareUnclassifiedData(transaction, unclassifiedDataObject, currentPath, destinationPath):
    incomingPath = transaction.getIncoming().getAbsolutePath()
    relativePath = unclassifiedDataObject.getRelativePath()
    # the source path of the currently handled data object (e.g. unclassified fast5_fail folder)
    unclassifiedSourcePath = os.path.join(os.path.dirname(currentPath), relativePath)
    unclassifiedChecksumFile = createChecksumFileForFolder(incomingPath, unclassifiedSourcePath)
    # we move the unclassified object to its destination (e.g. the unclassified fast5 top folder)
    os.rename(unclassifiedSourcePath, destinationPath)

# attaches unclassified data to the run experiment without sample
def registerUnclassifiedData(transaction, unclassifiedDataMap, runExperiment, currentPath, flowcellBarcode):
    topFolderFastq = os.path.join(currentPath, flowcellBarcode+"_unclassified_fastq")
    topFolderFast5 = os.path.join(currentPath, flowcellBarcode+"_unclassified_fast5")
    os.makedirs(topFolderFastq)
    os.makedirs(topFolderFast5)

    #create checksum files and move unclassified folders to their top folder
    prepareUnclassifiedData(transaction, unclassifiedDataMap.get("fastqfail"), currentPath, os.path.join(topFolderFastq, "fastq_fail"))
    prepareUnclassifiedData(transaction, unclassifiedDataMap.get("fastqpass"), currentPath, os.path.join(topFolderFastq, "fastq_pass"))

    prepareUnclassifiedData(transaction, unclassifiedDataMap.get("fast5fail"), currentPath, os.path.join(topFolderFast5, "fast5_fail"))
    prepareUnclassifiedData(transaction, unclassifiedDataMap.get("fast5pass"), currentPath, os.path.join(topFolderFast5, "fast5_pass"))

    fast5DataSet = transaction.createNewDataSet(NANOPORE_DATASET_CODE_DICT["fast5"])
    fastQDataSet = transaction.createNewDataSet(NANOPORE_DATASET_CODE_DICT["fastq"])
    fast5DataSet.setExperiment(runExperiment)
    fastQDataSet.setExperiment(runExperiment)
    transaction.moveFile(topFolderFast5, fast5DataSet)
    transaction.moveFile(topFolderFastq, fastQDataSet)

# moves a subset of nanopore data to a new target path, needed to add fail and pass subfolders to the same dataset
def prepareDataFolder(incomingPath, currentPath, destinationPath, dataObject, suffix):
    name = dataObject.getName()
    # if pooled data, folder is named using barcode and needs to be adapted
    if not "_" in name:
        name = name + "_" + suffix
    relativePath = dataObject.getRelativePath()
    # the source path of the currently handled data object (e.g. fast5_fail folder)
    sourcePath = os.path.join(os.path.dirname(currentPath), relativePath)
    checksumFile = createChecksumFileForFolder(incomingPath, sourcePath)
    # destination path containing data type (fastq or fast5), as well as the parent sample code, so pooled samples can be handled
    destination = os.path.join(destinationPath, name)
    os.rename(sourcePath, destination)

# creates checksums file in the basecalling folder, returns full path
def prepareBasecallingFolder(incomingPath, currentPath, dataObject):
    relativePath = dataObject.getRelativePath()
    # the source path of the currently handled data object (e.g. fast5_fail folder)
    sourcePath = os.path.join(os.path.dirname(currentPath), relativePath)
    checksumFile = createChecksumFileForFolder(incomingPath, sourcePath)
    return sourcePath

# test if data contains certain sequencing format, consolidate in top folder and move to destination
def registerDataOfType(transaction, incomingPath, currentPath, file_extension, parentSampleCode, sample, mapWithDataForSample):
    failData = mapWithDataForSample.get(file_extension+"fail")
    passData = mapWithDataForSample.get(file_extension+"pass")
    skipData = mapWithDataForSample.get(file_extension+"skip")
    hasData = failData or passData or skipData

    if hasData:
        topFolder = os.path.join(currentPath, parentSampleCode+"_"+file_extension)
        os.makedirs(topFolder)

    if failData:
        prepareDataFolder(incomingPath, currentPath, topFolder, failData, "fail")
    if passData:
        prepareDataFolder(incomingPath, currentPath, topFolder, passData, "pass")
    if skipData:
        prepareDataFolder(incomingPath, currentPath, topFolder, skipData, "skip")
    if hasData:
        dataSet = transaction.createNewDataSet(NANOPORE_DATASET_CODE_DICT[file_extension])
        dataSet.setSample(sample)
        transaction.moveFile(topFolder, dataSet)

def createSampleWithData(transaction, space, parentSampleCode, mapWithDataForSample, openbisExperiment, currentPath, absLogPath):
    """ Aggregates all measurement related files and registers them in openBIS.
    
    The Map mapWithDataForSample contains all DataFolders created for one sample code:
     [
        "fast5fail": DataFolder,
        "fast5pass": DataFolder,
        "fastqfail": DataFolder,
        "fastqpass": DataFolder
     ]   
    """
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

    # If fastq files were transfered, aggregate the folders fastqfail and fastqpass and fastq_skip under a common folder "<sample code>_fastq"
    registerDataOfType(transaction, incomingPath, currentPath, "fastq", parentSampleCode, sample, mapWithDataForSample)
    # If fast5 files were transfered, aggregate the folders fast5fail and fast5pass and fast5_skip under a common folder "<sample code>_fast5"
    registerDataOfType(transaction, incomingPath, currentPath, "fast5", parentSampleCode, sample, mapWithDataForSample)
    # If pod5 files were transfered, aggregate the folders pod5fail and pod5pass and pod5_skip under a common folder "<sample code>_fast5"
    registerDataOfType(transaction, incomingPath, currentPath, "pod5", parentSampleCode, sample, mapWithDataForSample)


    logDataSet = transaction.createNewDataSet(NANOPORE_LOG_CODE)
    logDataSet.setSample(sample)
    transaction.moveFile(absLogPath, logDataSet)

    # Check if extra basecalling folder exists and handle it. basecalling is always fastq
    basecalling = mapWithDataForSample.get("basecalling")
    if basecalling:
        basecallingFolder = prepareBasecallingFolder(incomingPath, currentPath, basecalling)
        basecallingDataSet = transaction.createNewDataSet(NANOPORE_DATASET_CODE_DICT["fastq"])
        basecallingDataSet.setSample(sample)
        transaction.moveFile(basecallingFolder, basecallingDataSet)

    # Updates the sample location of the measured sample
    wait_seconds = 1
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            SAMPLE_TRACKER.updateSampleStatus(parentSampleCode, DATA_AVAILABLE_JSON)
            break
        except:
            print "Updating location for sample "+parentSampleCode+" failed on attempt "+str(attempt+1)
            if attempt < max_attempts -1:
                time.sleep(wait_seconds)
                continue
            else:
                 raise

def process(transaction):
    """Main ETL routine entry point"""
    context = transaction.getRegistrationContext().getPersistentMap()

    # Get the incoming path of the transaction
    incomingPath = transaction.getIncoming().getAbsolutePath()

    key = context.get("RETRY_COUNT")
    if (key == None):
        key = 1

    # Get metadata from dropboxhandler as well as the original sequencing facility object
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
