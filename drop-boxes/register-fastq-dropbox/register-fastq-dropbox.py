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
        #identifier = name
        if isExpected(identifier):
                project = identifier[:5]
        else:
                print "The identifier "+identifier+" did not match the pattern Q[A-Z]{4}\d{3}\w{2} or checksum"
        
        search_service = transaction.getSearchService()
        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, identifier))
        foundSamples = search_service.searchForSamples(sc)

        sampleIdentifier = foundSamples[0].getSampleIdentifier()
        space = foundSamples[0].getSpace()
        sa = transaction.getSampleForUpdate(sampleIdentifier)

        sampleType = "Q_NGS_SINGLE_SAMPLE_RUN"
        if sa.getSampleType() != sampleType:
            sc = SearchCriteria()
            sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, "NGS"+identifier))
            foundSamples = search_service.searchForSamples(sc)
            if len(foundSamples) > 0:
                sampleIdentifier = foundSamples[0].getSampleIdentifier()
            else:
                search_service = transaction.getSearchService()
                sc = SearchCriteria()
                pc = SearchCriteria()
                pc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.PROJECT, project));
                sc.addSubCriteria(SearchSubCriteria.createExperimentCriteria(pc))
                foundSamples = search_service.searchForSamples(sc)
                space = foundSamples[0].getSpace()
                sampleIdentifier = "/"+space+"/"+parentCode
            if transaction.getSampleForUpdate(sampleIdentifier):
                sa = transaction.getSampleForUpdate(sampleIdentifier)
            else:
                # create NGS-specific experiment/sample and
                # attach to the test sample
                expType = "Q_NGS_MEASUREMENT"
                ngsExperiment = None
                experiments = search_service.listExperiments("/" + space + "/" + project)
                experimentIDs = []
                for exp in experiments:
                    experimentIDs.append(exp.getExperimentIdentifier())
                expID = experimentIDs[0]
                i = 0
                while expID in experimentIDs:
                    i += 1
                    expNum = len(experiments) + i
                    expID = '/' + space + '/' + project + \
                        '/' + project + 'E' + str(expNum)
                ngsExperiment = transaction.createNewExperiment(expID, expType)
                ngsExperiment.setPropertyValue('Q_SEQUENCER_DEVICE',"UNSPECIFIED_ILLUMINA_HISEQ_2500") #change this
                newID = 'NGS'+identifier
                ngsSample = transaction.createNewSample('/' + space + '/' + newID, sampleType)
                ngsSample.setParentSampleIdentifiers([sa.getSampleIdentifier()])
                ngsSample.setExperiment(ngsExperiment)
                sa = ngsSample
        # create new dataset
        dataSet = transaction.createNewDataSet("Q_NGS_RAW_DATA")
        dataSet.setMeasuredData(False)
        dataSet.setSample(sa)

        for f in os.listdir(incomingPath):
            if ".testorig" in f:
                os.remove(os.path.realpath(os.path.join(incomingPath,f)))
            if ".origlabfilename" in f:
                nameFile = open(os.path.join(incomingPath,f))
                origName = nameFile.readline().strip()
                nameFile.close()
        transaction.moveFile(incomingPath, dataSet)
