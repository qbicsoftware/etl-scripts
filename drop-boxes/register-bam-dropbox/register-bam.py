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
        # find or register new experiment
        expType = "Q_NGS_MAPPING"

        experiments = search_service.listExperiments("/" + space + "/" + project)
        experimentIDs = []
        for exp in experiments:
                experimentIDs.append(exp.getExperimentIdentifier())

        # no existing experiment for samples of this sample preparation found
        expID = experimentIDs[0]
        i = 0
        while expID in experimentIDs:
                i += 1
                expNum = len(experiments) + i
                expID = '/' + space + '/' + project + '/' + project + 'E' + str(expNum)

        #newMappingSample = transaction.createNewSample('/' + space + '/' + 'MP'+ parentCode, "Q_NGS_MAPPING")
        #newMappingSample.setParentSampleIdentifiers([sa.getSampleIdentifier()])
        #newMappingSample.setExperiment(mapExperiment)

        sc = SearchCriteria()
        pc = SearchCriteria()
        pc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.PROJECT, project))
        sc.addSubCriteria(SearchSubCriteria.createExperimentCriteria(pc))
        allSamples = search_service.searchForSamples(sc)

        #existingSampleIDs = []

        ngsParents = []
        
        for samp in allSamples:
                #existingSampleIDs.append(samp.getSampleIdentifier())
                if samp.getSampleType()=="Q_NGS_SINGLE_SAMPLE_RUN":
                        if sa.getSampleIdentifier() in samp.getParentSampleIdentifiers():
                                ngsParents.append(samp.getSampleIdentifier())

        #replNumber = 1
        #if len(ngsParents > 1):
        mapSampleID = '/' + space + '/' + 'MP' + parentCode

        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, mapSampleID))
        foundMapSample = search_service.searchForSamples(sc)
        #while newSampleID in existingSampleIDs:
        #        vcNumber += 1
        #        newSampleID = '/' + space + '/' + 'MP' + str(vcNumber) + parentCode
        if len(foundMapSample) == 0:
                mapExperiment = transaction.createNewExperiment(expID, expType)
                mapExperiment.setPropertyValue('Q_CURRENT_STATUS', 'FINISHED')
                mappingSample = transaction.createNewSample(mapSampleID, "Q_NGS_MAPPING")
                mappingSample.setParentSampleIdentifiers(ngsParents)
                mappingSample.setExperiment(mapExperiment)
        else:
                mappingSample = transaction.getSampleForUpdate(foundMapSample[0].getSampleIdentifier())
        # create new dataset
        dataSet = transaction.createNewDataSet("Q_NGS_MAPPING_DATA")
        dataSet.setMeasuredData(False)
        dataSet.setSample(mappingSample)

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
