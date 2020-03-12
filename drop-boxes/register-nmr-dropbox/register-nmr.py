'''

Note:
print statements go to: ~openbis/servers/datastore_server/log/startup_log.txt
'''
import sys
sys.path.append('/home-link/qeana10/bin/')

import checksum
import re
import string
import os
import ch.systemsx.cisd.etlserver.registrator.api.v2
from java.io import File
from org.apache.commons.io import FileUtils
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria
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
pattern = re.compile('Q\w{4}[0-9]{3}[a-zA-Z]\w')

def isExpected(identifier):
    try:
        id = identifier[0:9]
        #also checks for old checksums with lower case letters
        return checksum.checksum(id)==identifier[9]
    except:
        return False

def getNextBarcode(oldcode):
    letters = string.ascii_uppercase[:-2] # Y and Z should not be used due to international input devices switching these letters
    project = oldcode[0:5]
    letter = oldcode[8]
    number = int(oldcode[5:8])+1
    if(number > 999):
        index = letters.find(letter)
        letter = letters[index+1]
    code = project + str(number).zfill(3) + letter 
    return code + checksum.checksum(code)

def createSmallMoleculeSample(tr, space, project, exp, parentID):
    code = project+"000AX"
    sampleExists = True
    newSampleID = None
    while sampleExists:
        code = getNextBarcode(code)
        newSampleID = '/' + space + '/' + code
        sampleExists = tr.getSampleForUpdate(newSampleID)
    sample = tr.createNewSample(newSampleID, "Q_TEST_SAMPLE")
    sample.setExperiment(exp)
    sample.setPropertyValue('Q_SAMPLE_TYPE', "SMALLMOLECULES")
    sample.setParentSampleIdentifiers([parentID])
    return sample

def process(transaction):
    context = transaction.getRegistrationContext().getPersistentMap()

    # Get the incoming path of the transaction
    incomingPath = transaction.getIncoming().getAbsolutePath()

    key = context.get("RETRY_COUNT")
    if (key == None):
        key = 1


    # Get the name of the incoming file
    name = transaction.getIncoming().getName()

    identifier = pattern.findall(name)[0]
    if isExpected(identifier):
        project = identifier[:5]
        parentCode = identifier[:10]
    else:
        print "The identifier "+identifier+" did not match the pattern Q[A-Z]{4}\d{3}\w{2} or checksum"
        
    search_service = transaction.getSearchService()

    #find specific sample
    sc = SearchCriteria()
    sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, parentCode))
    foundSamples = search_service.searchForSamples(sc)

    barcodeSample = foundSamples[0]
    parentSampleIdentifier = barcodeSample.getSampleIdentifier()
    space = barcodeSample.getSpace()
    sa = transaction.getSampleForUpdate(parentSampleIdentifier)

    sType = barcodeSample.getSampleType()
    # as it should be
    if(sType == "Q_TEST_SAMPLE"):
        # register new experiment and sample
        experiments = search_service.listExperiments("/" + space + "/" + project)
        exp = None
        for e in experiments:
            if e.getExperimentType() == "Q_NMR_MEASUREMENT":
                exp = e
        if not exp:
            numberOfExperiments = len(experiments) + 1
            exp = transaction.createNewExperiment('/' + space + '/' + project + '/' + project + 'E' + str(numberOfExperiments), "Q_NMR_MEASUREMENT")
    # found sample is tissue sample and test sample needs to be created
    else:
        exp = None
        existingExperiments = search_service.listExperiments("/" + space + "/" + project)
        for e in existingExperiments:
            eType = e.getExperimentType()
            if eType == "Q_SAMPLE_PREPARATION":
                # we assign a random experiment of the correct type for our small molecule sample...
                exp = e
        sa = createSmallMoleculeSample(transaction, space, project, exp, sa.getSampleIdentifier())

    newSample = transaction.createNewSample('/' + space + '/' + 'NMR'+ sa.getCode(), "Q_NMR_SAMPLE_RUN")
    newSample.setParentSampleIdentifiers([sa.getSampleIdentifier()])
    newSample.setExperiment(exp) 
    # create new dataset 
    dataSet = transaction.createNewDataSet("Q_NMR_RAW_DATA")
    dataSet.setMeasuredData(False)
    dataSet.setSample(newSample)

    f = "source_dropbox.txt"
    sourceLabFile = open(os.path.join(incomingPath,f))
    sourceLab = sourceLabFile.readline().strip()
    sourceLabFile.close()
    os.remove(os.path.realpath(os.path.join(incomingPath,f)))

    for f in os.listdir(incomingPath):
        if ".origlabfilename" in f:
            os.remove(os.path.realpath(os.path.join(incomingPath,f)))
    transaction.moveFile(incomingPath, dataSet)

    #sample tracking section
    SAMPLE_TRACKER.updateSampleLocationToCurrentLocation(parentCode)
