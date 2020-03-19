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

def process(transaction):
        context = transaction.getRegistrationContext().getPersistentMap()

        # Get the incoming path of the transaction
        incomingPath = transaction.getIncoming().getAbsolutePath()
        search_service = transaction.getSearchService()

        key = context.get("RETRY_COUNT")
        if (key == None):
                key = 1
        parentCodes = []
        for name in os.listdir(incomingPath):
                code = None
                searchID = pattern.findall(name)
                if isExpected(searchID[0]):
                        code = searchID[0]
                        project = code[:5]
                else:
                        print "The code "+code+" did not match the pattern Q[A-Z]{4}\d{3}\w{2} or checksum"
                sc = SearchCriteria()
                sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, "MA"+code))
                foundSamples = search_service.searchForSamples(sc)
                parentCodes.append(code)
                sampleIdentifier = foundSamples[0].getSampleIdentifier()
                space = foundSamples[0].getSpace()
                sa = transaction.getSampleForUpdate(sampleIdentifier)

                # create new dataset 
                dataSet = transaction.createNewDataSet("Q_MA_CHIP_IMAGE")
                dataSet.setMeasuredData(False)
                dataSet.setSample(sa)

                image = os.path.realpath(os.path.join(incomingPath,name))
                transaction.moveFile(image, dataSet)
        for code in parentCodes:
                #sample tracking section
                SAMPLE_TRACKER.updateSampleLocationToCurrentLocation(code)
