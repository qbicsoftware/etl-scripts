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
    if len(foundSamples) > 0:
        parentSampleIdentifier = foundSamples[0].getSampleIdentifier()
        space = foundSamples[0].getSpace()
    else:
        search_service = transaction.getSearchService()
        sc = SearchCriteria()
        pc = SearchCriteria()
        pc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.PROJECT, project));
        sc.addSubCriteria(SearchSubCriteria.createExperimentCriteria(pc))
        foundSamples = search_service.searchForSamples(sc)
        if len(foundSamples) > 0:
            space = foundSamples[0].getSpace()
            parentSampleIdentifier = "/"+space+"/"+parentCode
        else:
            # no sample found in this project, they are probably not indexed yet. try parsing space from file name instead
            space = name.split("_"+parentCode)[0]
            parentSampleIdentifier = "/"+space+"/"+parentCode
    sa = transaction.getSampleForUpdate(parentSampleIdentifier)

    # register new experiment and sample
    existingExperimentIDs = []
    existingExperiments = search_service.listExperiments("/" + space + "/" + project)
    
    numberOfExperiments = len(search_service.listExperiments("/" + space + "/" + project)) + 1

    for eexp in existingExperiments:
        existingExperimentIDs.append(eexp.getExperimentIdentifier())

    newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(numberOfExperiments)

    while newExpID in existingExperimentIDs:
        numberOfExperiments += 1 
        newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(numberOfExperiments)

    newHLATypingExperiment = transaction.createNewExperiment(newExpID, "Q_NGS_HLATYPING")
    newHLATypingExperiment.setPropertyValue('Q_CURRENT_STATUS', 'FINISHED')

    if os.path.isdir(incomingPath):
        for root, subFolders, files in os.walk(incomingPath):
            if subFolders:
                subFolder = subFolders[0]
            for f in files:
                if f.endswith('.alleles') or f.endswith('alleles.txt'):
                    resultPath = os.path.join(root, f)
                    resultFile = open(resultPath, 'r')
    else:
        resultPath = incomingPath
        resultFile = open(resultPath, 'r')
    resultContent = resultFile.read()

    mhcClass = "MHC_CLASS_II"
    mhcSuffix = "2"
    # check for MHC class
    if 'A*' in resultContent:
        mhcClass = "MHC_CLASS_I"
        mhcSuffix = "1"
    # does HLA sample of this class already exist?
    hlaCode = 'HLA' + mhcSuffix + parentCode
    sc = SearchCriteria()
    sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(
        SearchCriteria.MatchClauseAttribute.CODE, hlaCode))
    foundSamples = search_service.searchForSamples(sc)
    if len(foundSamples) < 1:
        newHLATypingSample = transaction.createNewSample('/' + space + '/' + hlaCode, "Q_NGS_HLATYPING")
        newHLATypingSample.setParentSampleIdentifiers([sa.getSampleIdentifier()])
        newHLATypingSample.setExperiment(newHLATypingExperiment)
        newHLATypingSample.setPropertyValue("Q_HLA_CLASS", mhcClass)
    else:
        newHLATypingSample = transaction.getSampleForUpdate(foundSamples[0].getSampleIdentifier())

    newHLATypingSample.setPropertyValue("Q_HLA_TYPING", resultContent)

    # create new dataset 
    dataSet = transaction.createNewDataSet("Q_NGS_HLATYPING_DATA")
    dataSet.setMeasuredData(False)
    dataSet.setSample(newHLATypingSample)

    transaction.moveFile(resultPath, dataSet)

    #sample tracking section
    SAMPLE_TRACKER.updateSampleLocationToCurrentLocation(parentCode)
