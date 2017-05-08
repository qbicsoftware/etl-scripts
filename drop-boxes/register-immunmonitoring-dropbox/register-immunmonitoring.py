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
