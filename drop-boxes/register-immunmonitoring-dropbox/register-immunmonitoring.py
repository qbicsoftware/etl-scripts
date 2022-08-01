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
DATA_AVAILABLE_JSON = tracking_helper.get_data_available_status_json()

### We need this object to update the sample status later
SAMPLE_TRACKER = SampleTracker.createLocationIndependentSampleTracker(SERVICE_REGISTRY_URL, SERVICE_CREDENTIALS, DATA_AVAILABLE_JSON)

# *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
pattern = re.compile('Q\w{4}[0-9]{3}[a-zA-Z]\w')
expType = "Q_NGS_IMMUNE_MONITORING"
sType = "Q_NGS_IMMUNE_MONITORING"
dsType = "Q_NGS_IMMUNE_MONITORING_DATA"

def isExpected(identifier):
    try:
        id = identifier[0:9]
        #also checks for old checksums with lower case letters
        return checksum.checksum(id)==identifier[9]
    except:
        return False

def process(transaction):
    context = transaction.getRegistrationContext().getPersistentMap()

    # Get the incoming path of the transaction
    incomingPath = transaction.getIncoming().getAbsolutePath()
    name = transaction.getIncoming().getName()
    key = context.get("RETRY_COUNT")
    if (key == None):
        key = 1


    # Get the name of the incoming file
    #name = transaction.getIncoming().getName()

    parents = []
    identifier = pattern.findall(name)[0]
    if isExpected(identifier):
        experiment = identifier[1:5]
        project = identifier[:5]
        parentCode = identifier[:10]
    else:
        print "The identifier "+identifier+" did not match the pattern Q[A-Z]{4}\d{3}\w{2} or checksum"
    
    search_service = transaction.getSearchService()
    sc = SearchCriteria()
    sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, parentCode))
    foundSamples = search_service.searchForSamples(sc)

    parentSampleIdentifier = foundSamples[0].getSampleIdentifier()
    space = foundSamples[0].getSpace()
    sa = transaction.getSampleForUpdate(parentSampleIdentifier)

    # register new experiment and sample
    existingExperimentIDs = []
    existingExperiments = search_service.listExperiments("/" + space + "/" + project)

    for eexp in existingExperiments:
        existingExperimentIDs.append(eexp.getExperimentIdentifier())

    suffixNum = 1
    newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(suffixNum)
    while newExpID in existingExperimentIDs:
        suffixNum += 1
        newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(suffixNum)

    newPCRExperiment = transaction.createNewExperiment(newExpID, expType)

    newPCRSample = transaction.createNewSample('/' + space + '/' + 'IM'+ parentCode, sType)
    newPCRSample.setParentSampleIdentifiers([sa.getSampleIdentifier()])
    newPCRSample.setExperiment(newPCRExperiment)

    # create new dataset 
    dataSet = transaction.createNewDataSet(dsType)
    dataSet.setMeasuredData(False)
    dataSet.setSample(newPCRSample)

    transaction.moveFile(incomingPath, dataSet)

    #sample tracking section
    wait_seconds = 1
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            SAMPLE_TRACKER.updateSampleLocationToCurrentLocation(parentCode)
            break
        except:
            print "Updating location for sample "+parentCode+" failed on attempt "+str(attempt+1)
            if attempt < max_attempts -1:
                time.sleep(wait_seconds)
                continue
            else:
                raise
