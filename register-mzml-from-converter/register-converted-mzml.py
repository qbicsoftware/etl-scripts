'''

Note:
print statements go to: ~openbis/servers/datastore_server/log/startup_log.txt
'''
import sys
sys.path.append('/home-link/qeana10/bin/')

import checksum
import re
import os
import time
import ch.systemsx.cisd.etlserver.registrator.api.v2
from java.io import File
from org.apache.commons.io import FileUtils
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria

# *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
pattern = re.compile('Q[A-Z]{4}\d{3}\w{2}')

def process(transaction):
        context = transaction.getRegistrationContext().getPersistentMap()

        # Get the incoming path of the transaction
        incomingPath = transaction.getIncoming().getAbsolutePath()

        key = context.get("RETRY_COUNT")
        if (key == None):
                key = 1


        # Get the name of the incoming file
        name = transaction.getIncoming().getName()
        
        code = pattern.findall(name)[0]

        #such checks should not be needed here as it's not raw data
        #if isExpected(code):
        #        project = code[2:7]
        #        #parentCode = code[2:]
        #else:
        #        print "The identifier "+code+" did not match the pattern MSQ[A-Z]{4}\d{3}\w{2}"
        
        search_service = transaction.getSearchService()
        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, "MS"+code))
        
        # wait for registration of ms sample to finish (done in raw dropbox after mzml marker is created)
        timeout = 10
        while timeout > 0:
            timeout = timeout - 1
            time.sleep(5)
            foundSamples = search_service.searchForSamples(sc)
            if foundSamples > 0:
                break

        sampleIdentifier = foundSamples[0].getSampleIdentifier()
        #space = foundSamples[0].getSpace()
        sa = transaction.getSampleForUpdate(sampleIdentifier)

        # create new dataset 
        dataSet = transaction.createNewDataSet("MS_RAW_DATA")
        dataSet.setMeasuredData(False)
        dataSet.setSample(sa)

        transaction.moveFile(incomingPath, dataSet)
