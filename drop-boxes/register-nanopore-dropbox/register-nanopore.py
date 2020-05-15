'''

Note:
print statements go to: ~openbis/servers/datastore_server/log/startup_log.txt
'''
import sys
sys.path.append('/home-link/qeana10/bin/')

import checksum
import re
import os
import ch.systemsx.cisd.etlserver.registrator.api.v2
from java.io import File
from org.apache.commons.io import FileUtils
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria

# Parsing related imports
from life.qbic import NanoporeParser
from life.qbic import NanoporeObject

######## Sample Tracking related import
from life.qbic.sampletracking import SampleTracker
from life.qbic.sampletracking import ServiceCredentials
from java.net import URL

import sample_tracking_helper_qbic as tracking_helper

######## imports for fastq/5 file validation
#import subprocess

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

#TODO
def createNewSample(transaction, parentSampleCode):
    # 1. get parent sample
    # 2. i = 0
    # 3. increment i in PREFIX+i+parentSampleCode and create sample identifier
    # 4. if sample identifier exist in openbis or usedSampleIdentifiers go to 3
    # 5. usedSampleIdentifiers.append(identifier)
    # 6. create and return sample using identifier

#TODO
def createNewExperiment(transaction, projectCode):
    # 1. get experiments in this project
    # 2. i = len(experiments)
    # 3. increment i in projectCode+E+i and create experiment identifier
    # 4. if experiment identifier exist in openbis or usedExperimentIdentifiers go to 3
    # 5. usedExperimentidentifiers.append(identifier)
    # 6. create and return experiment using identifier

# return first line of file
def getDatahandlerMetadata(incomingPath, fileName):
        path = os.path.realpath(os.path.join(incomingPath,file_name))
        with open(path) as f:
            return f.readline()

def handleMeasurement(transaction, measurement, origin, projectCode):
        #reminder: incoming path is of the absolute path of the folder created by the datahandler.
        #joining this path with any relative path returned by the nanopore object will give the absolute path of that file/folder.
        incomingPath = transaction.getIncoming().getAbsolutePath()

        # handle log files
        newLogFolder = os.path.join(measurement.getRelativePath(), "logs")
        os.mkdir(newLogFolder)
        for logFile in measurement.getLogFiles():
            logFilePath = os.path.join(incomingPath, logFile.getRelativePath())
            os.rename(logFilePath, os.path.join(newLogFolder, logFile.getFileName()))
        logDataSet = transaction.createNewDataSet(NANOPORE_LOG_CODE)
        # either create log sample type or add this to every sample
        logDataSet.setSample(logSampleToBeCreated?)

        transaction.moveFile(newLogFolder, logDataSet)

        # handle metadata of experiment level
        runExperiment = createNewExperiment(transaction, projectCode)

        #do we get these automatically? from where?
        runExperiment.setPropertyValue("Q_LIBRARY_PREPKIT)", TBD)
        runExperiment.setPropertyValue("Q_MEASUREMENT_START_DATE", measurement.getStartTime())
        runExperiment.setPropertyValue("Q_FLOWCELL_POSITION",) # parse from folder name or parse from log file? next token after time, example: "1-E3-H3"
        runExperiment.setPropertyValue("Q_DATA_GENERATION_FACILITY", origin)
        runExperiment.setPropertyValue("Q_FLOWCELL_BARCODE", ) # parse from folder name or log file? next token after position, example: "PAE26974"
        runExperiment.setPropertyValue("Q_SEQUENCER_DEVICE",) # ID in summary file, but no name of the sequencer --> need additional property?
        # runExperiment.setPropertyValue("Q_EXTERNALDB_ID",) best skip and parse sample information at sample level, no experiment-wide ID from what I can tell
        # handle measured samples
        for sample in measurement.getMeasuredSamples():
            handleSingleSample(transaction, sample, runExperiment)

def handleSingleSample(transaction, sampleWithDataFolders, openbisExperiment):
        incomingPath = transaction.getIncoming().getAbsolutePath()

        parentSampleCode = sampleWithDataFolders.getBarcode()
        sample = createNewSample(transaction, parentSampleCode)
        sample.setExperiment(openbisExperiment)
        sample.setPropertyValue("Q_EXTERNALDB_ID",) # this should already be set for the parent. where do we get it on this level, if it's needed?
        # maybe we need to get the object and then the path
        fast5PathForSample = sampleWithDataFolders.getFast5Folder()
        fastQPathForSample = sampleWithDataFolders.getFastQPath() # careful, this could be one fastq.gz or a folder

        fast5DataSet = transaction.createNewDataSet(NANOPORE_FAST5_CODE)
        fastQDataSet = transaction.createNewDataSet(NANOPORE_FASTQ_CODE)
        fast5DataSet.setSample(sample)
        fastQDataSet.setSample(sample)
        transaction.moveFile(os.path.join(incomingPath, fast5PathForSample), fast5DataSet)
        transaction.moveFile(os.path.join(incomingPath, fastQPathForSample), fastQDataSet)

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
        for f in os.listdir(incomingPath):
            if f.startswith('Q'):
                nanoporeFolder = os.path.realpath(os.path.join(incomingPath,f))

        origin = getDatahandlerMetadata(incomingPath, "source_dropbox.txt")
        # Use file structure parser to create structure object
        nanoporeObject = NanoporeParser.parseFileStructure(nanoporeFolder)

        for measurement in nanoporeObject.getMeasurements():
            handleMeasurement(transaction, measurement, origin)

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

