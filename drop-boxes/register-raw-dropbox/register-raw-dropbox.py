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
pattern = re.compile('Q\w{4}[0-9]{3}[a-zA-Z]\w')

def isExpected(identifier):
        try:
                id = identifier[0:9]
                #also checks for old checksums with lower case letters
                return checksum.checksum(id)==identifier[9]
        except:
                return False


def isCurrentMSRun(tr, parentExpID, msExpID):
    search_service = tr.getSearchService()
    sc = SearchCriteria()
    sc.addMatchClause(
        SearchCriteria.MatchClause.createAttributeMatch(
            SearchCriteria.MatchClauseAttribute.TYPE, "Q_MS_RUN"
        )
    )
    foundSamples = search_service.searchForSamples(sc)
    for samp in foundSamples:
        currentMSExp = samp.getExperiment()
        if currentMSExp.getExperimentIdentifier() == msExpID:
            for parID in samp.getParentSampleIdentifiers():
                parExp = (tr.getSampleForUpdate(parID)
                            .getExperiment()
                            .getExperimentIdentifier())
                if parExp == parentExpID:
                    return True
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
        
        code = identifier
        search_service = transaction.getSearchService()
        sc = SearchCriteria()    # Find the test sample
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(
        SearchCriteria.MatchClauseAttribute.CODE, code))
        foundSamples = search_service.searchForSamples(sc)

        sampleIdentifier = foundSamples[0].getSampleIdentifier()
        space = foundSamples[0].getSpace()
        sa = transaction.getSampleForUpdate(sampleIdentifier)

        # get or create MS-specific experiment/sample and
        # attach to the test sample
        expType = "Q_MS_MEASUREMENT"
        MSRawExperiment = None
        experiments = search_service.listExperiments("/" + space + "/" + project)
        experimentIDs = []
        for exp in experiments:
            experimentIDs.append(exp.getExperimentIdentifier())
            if exp.getExperimentType() == expType:
                if isCurrentMSRun(
                    transaction,
                    sa.getExperiment().getExperimentIdentifier(),
                    exp.getExperimentIdentifier()
                ):
                    MSRawExperiment = exp
        # no existing experiment for samples of this sample preparation found
        if not MSRawExperiment:
            expID = experimentIDs[0]
            i = 0
            while expID in experimentIDs:
                i += 1
                expNum = len(experiments) + i
                expID = '/' + space + '/' + project + \
                    '/' + project + 'E' + str(expNum)
            MSRawExperiment = transaction.createNewExperiment(expID, expType)
        # does MS sample already exist?
        msCode = 'MS' + code
        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(
            SearchCriteria.MatchClauseAttribute.CODE, msCode))
        foundSamples = search_service.searchForSamples(sc)
        if len(foundSamples) < 1:
            msSample = transaction.createNewSample('/' + space + '/' + msCode, "Q_MS_RUN")
            msSample.setParentSampleIdentifiers([sa.getSampleIdentifier()])
            msSample.setExperiment(MSRawExperiment)
        else:
            msSample = transaction.getSampleForUpdate(foundSamples[0].getSampleIdentifier())

        # create new dataset
        rawDataSet = transaction.createNewDataSet("Q_MS_RAW_DATA")
        rawDataSet.setMeasuredData(False)
        rawDataSet.setSample(msSample)

       	#cegat = False
        f = "source_dropbox.txt"
        sourceLabFile = open(os.path.join(incomingPath,f))
       	sourceLab = sourceLabFile.readline().strip() 
        sourceLabFile.close()
        os.remove(os.path.realpath(os.path.join(incomingPath,f)))

        for f in os.listdir(incomingPath):
            if ".origlabfilename" in f:
                os.remove(os.path.realpath(os.path.join(incomingPath,f)))
        transaction.moveFile(incomingPath, rawDataSet)
