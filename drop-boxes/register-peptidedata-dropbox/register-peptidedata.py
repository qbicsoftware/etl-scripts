'''
ETL script for registration of peptide data files containing peptide sequences

Note:
print statements go to: ~openbis/servers/datastore_server/log/startup_log.txt
'''

import sys
sys.path.append('/home-link/qeana10/bin/')

import checksum
import time
import re
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

# expected code: *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
pattern = re.compile('Q\w{4}[0-9]{3}[a-zA-Z]\w')

# Check barcode for integrity
def isExpected(identifier):
    try:
        id = identifier[0:9]
        return checksum.checksum(id)==identifier[9]
    except:
        return False

# Main function which will be triggered upon registration
def process(transaction):
    context = transaction.getRegistrationContext().getPersistentMap()

    # Get the incoming path of the transaction
    incomingPath = transaction.getIncoming().getAbsolutePath()

    key = context.get("RETRY_COUNT")
    if (key == None):
        key = 1

    # Get the name of the incoming file
    name = transaction.getIncoming().getName()

    # Parse experiment, project and sample code       
    identifier = pattern.findall(name)[0]
    if isExpected(identifier):
        experiment = identifier[1:5]
        project = identifier[:5]
        parentCode = identifier[:10]
    else:
        print "The identifier "+identifier+" did not match the pattern Q[A-Z]{4}\d{3}\w{2} or checksum"

    # Initialize search service and search for sample using the provided code    
    search_service = transaction.getSearchService()
    sc = SearchCriteria()
    sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, parentCode))
    foundSamples = search_service.searchForSamples(sc)

    # Get sample ID and retrieve the sample for update
    parentSampleIdentifier = foundSamples[0].getSampleIdentifier()
    space = foundSamples[0].getSpace()
    parentSample = transaction.getSampleForUpdate(parentSampleIdentifier)

    # Create new peptide dataset and attach it to the found sample
    dataSet = transaction.createNewDataSet("Q_PEPTIDE_DATA")
    dataSet.setMeasuredData(False)
    dataSet.setSample(parentSample)

    # Move the file(s) to the new dataset
    transaction.moveFile(incomingPath, dataSet)

    #sample tracking section
    wait_seconds = 1
    max_attempts = 3
    for attempt in range(max_attempts):
        try:
            SAMPLE_TRACKER.updateSampleStatus(parentCode)
            break
        except:
            print "Updating location for sample "+parentCode+" failed on attempt "+str(attempt+1)
            if attempt < max_attempts -1:
                time.sleep(wait_seconds)
                continue
            else:
                 raise
