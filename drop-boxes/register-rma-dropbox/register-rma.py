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

# ETL script for registration of VCF files
# expected:
# *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
RE_PATTERN = 'Q\w{4}[0-9]{3}[A-Z]\w'
pattern = re.compile(RE_PATTERN)
EXP_TYPE = "Q_NGS_READ_MATCH_ALIGNMENT"
SAMPLE_TYPE = "Q_NGS_READ_MATCH_ALIGNMENT_RUN"
DS_TYPE = "Q_NGS_READ_MATCH_ARCHIVE"
FILE_TYPE_PROPERTY = "Q_READ_MATCH_ARCHIVE_FORMAT" # can be RMA2, RMA3, RMA6

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
        print "The identifier "+identifier+" did not match the pattern "+RE_PATTERN+" or checksum"
 
    # create new dataset 
    dataSet = transaction.createNewDataSet(DS_TYPE)
    dataSet.setMeasuredData(False)
    stem, ext = os.path.splitext(name)
    dataSet.setPropertyValue(FILE_TYPE_PROPERTY,ext[1:].upper())

    search_service = transaction.getSearchService()
    sc = SearchCriteria()
    sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, "RMA"+parentCode))
    foundSamples = search_service.searchForSamples(sc)
    if len(foundSamples) > 0:
        rmaSample = transaction.getSampleForUpdate(foundSamples[0].getSampleIdentifier())
    else:
        # rma sample needs to be created        
        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, "NGS"+parentCode))
        foundSamples = search_service.searchForSamples(sc)

        parentSampleIdentifier = foundSamples[0].getSampleIdentifier()
        space = foundSamples[0].getSpace()
        sa = transaction.getSampleForUpdate(parentSampleIdentifier)

        # register new experiment and sample
        existingExperimentIDs = []
        existingExperiments = search_service.listExperiments("/" + space + "/" + project)
    
        numberOfExperiments = len(existingExperiments) + 1

        for eexp in existingExperiments:
            existingExperimentIDs.append(eexp.getExperimentIdentifier())

        newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(numberOfExperiments)

        while newExpID in existingExperimentIDs:
            numberOfExperiments += 1 
            newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(numberOfExperiments)

        newRMAExperiment = transaction.createNewExperiment(newExpID, EXP_TYPE)

        rmaSample = transaction.createNewSample('/' + space + '/' + 'RMA' + parentCode, SAMPLE_TYPE)
        rmaSample.setParentSampleIdentifiers([sa.getSampleIdentifier()])

        rmaSample.setExperiment(newRMAExperiment) 
    dataSet.setSample(rmaSample)
    transaction.moveFile(incomingPath, dataSet)
