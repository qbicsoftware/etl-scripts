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
pattern = re.compile('Q\w{4}[0-9]{3}[a-xA-X]\w')
alt_pattern = re.compile('Q\w{4}ENTITY-[1-9][0-9]*')

def isExpected(code):
        try:
                id = code[0:9]
                #also checks for old checksums with lower case letters
                return checksum.checksum(id)==code[9]
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
        
        found = pattern.findall(name)
        if len(found) > 0:
                code = found[0]
                if not isExpected(code):
                        print "The code "+code+" did not match the checksum"
        else:
                code = alt_pattern.findall(name)[0]
        project = code[:5]

        search_service = transaction.getSearchService()
        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, code))
        foundSamples = search_service.searchForSamples(sc)

        sampleID = foundSamples[0].getSampleIdentifier()
        space = foundSamples[0].getSpace()
        sample = transaction.getSampleForUpdate(sampleID)

        # create new dataset 
        dataSet = transaction.createNewDataSet("Q_DOCUMENT")
        dataSet.setMeasuredData(False)
        dataSet.setSample(sample)

        for f in os.listdir(incomingPath):
            if f.endswith('origlabfilename' or f.endswith('source_dropbox.txt') or f.endswith('sha256sum')):
                os.remove(os.path.realpath(os.path.join(incomingPath,f)))
        transaction.moveFile(os.path.join(incomingPath, name), dataSet)