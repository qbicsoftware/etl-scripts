'''

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

        key = context.get("RETRY_COUNT")
        if (key == None):
                key = 1


        # Get the name of the incoming file
        name = transaction.getIncoming().getName()
        
        identifier = pattern.findall(name)[0]
        if isExpected(identifier):
                project = identifier[:5]
                #parentCode = identifier[:10]
        else:
                print "The identifier "+identifier+" did not match the pattern Q[A-Z]{4}\d{3}\w{2} or checksum"
        
        search_service = transaction.getSearchService()
        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, identifier))
        foundSamples = search_service.searchForSamples(sc)

        sampleIdentifier = foundSamples[0].getSampleIdentifier()
        space = foundSamples[0].getSpace()
        sa = transaction.getSampleForUpdate(sampleIdentifier)
        #numberOfExperiments = len(search_service.listExperiments("/" + space + "/" + project)) + 1
        #newVariantCallingExperiment = transaction.createNewExperiment('/' + space + '/' + project + '/' + project + 'E' + str(numberOfExperiments), "Q_NGS_VARIANT_CALLING")

        #newVariantCallingSample = transaction.createNewSample('/' + space + '/' + 'VC'+ parentCode, "Q_NGS_VARIANT_CALLING")
        #newVariantCallingSample.setParentSampleIdentifiers([sa.getSampleIdentifier()])
      
	#newVariantCallingSample.setExperiment(newVariantCallingExperiment) 
        # create new dataset 
        dataSet = transaction.createNewDataSet("IDXML")
        dataSet.setMeasuredData(False)
        dataSet.setSample(sa)

       	#cegat = False
        f = "source_dropbox.txt"
        sourceLabFile = open(os.path.join(incomingPath,f))
       	sourceLab = sourceLabFile.readline().strip() 
        sourceLabFile.close()
        #if sourceLab == 'dmcegat':
                #cegat = True
        os.remove(os.path.realpath(os.path.join(incomingPath,f)))

        for f in os.listdir(incomingPath):
		if ".testorig" in f:
			os.remove(os.path.realpath(os.path.join(incomingPath,f)))
               	#elif f.endswith('vcf') and cegat:
                        #secondaryName = f.split('_')[0]
                       	#entitySample = transaction.getSampleForUpdate('/%s/%s' % (space,parentCode))
                       	#sa.setPropertyValue('Q_SECONDARY_NAME', secondaryName)
        transaction.moveFile(incomingPath, dataSet)

        #sample tracking section
        wait_seconds = 1
        try_count = 3
        for i in range(try_count):
                try:
                        SAMPLE_TRACKER.updateSampleLocationToCurrentLocation(identifier)
                except Exception as e:
                        if i < try_count -1:
                                time.sleep(wait_seconds)
                                continue
                        else:
                                raise
                break
