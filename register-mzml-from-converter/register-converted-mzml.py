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
pattern = re.compile('MSQ[A-Z]{4}\d{3}\w{2}')

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
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, code))
        foundSamples = search_service.searchForSamples(sc)

        sampleIdentifier = foundSamples[0].getSampleIdentifier()
        #space = foundSamples[0].getSpace()
        sa = transaction.getSampleForUpdate(sampleIdentifier)

        # create new dataset 
        dataSet = transaction.createNewDataSet("MZML")
        dataSet.setMeasuredData(False)
        dataSet.setSample(sa)

        # do something with this?
        f = "source_dropbox.txt"
        #sourceLabFile = open(os.path.join(incomingPath,f))
       	#sourceLab = sourceLabFile.readline().strip() 
        #sourceLabFile.close()
        os.remove(os.path.realpath(os.path.join(incomingPath,f)))

        f = name+".origlabfilename"
       	#nameFile = open(os.path.join(incomingPath,f))
        #origName = nameFile.readline().strip()
        #nameFile.close()

        os.remove(os.path.realpath(os.path.join(incomingPath,f)))

        for f in os.listdir(incomingPath):
		if ".testorig" in f:
			os.remove(os.path.realpath(os.path.join(incomingPath,f)))
        transaction.moveFile(incomingPath, dataSet)
