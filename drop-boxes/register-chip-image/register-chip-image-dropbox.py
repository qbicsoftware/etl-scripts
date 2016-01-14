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

def process(transaction):
        context = transaction.getRegistrationContext().getPersistentMap()

        # Get the incoming path of the transaction
        incomingPath = transaction.getIncoming().getAbsolutePath()
        search_service = transaction.getSearchService()

        key = context.get("RETRY_COUNT")
        if (key == None):
                key = 1
        for name in os.listdir(incomingPath):
                identifier = None
                searchID = pattern.findall(name)
                if isExpected(searchID[0]):
                        identifier = searchID[0]
                        project = identifier[:5]
                else:
                        print "The identifier "+identifier+" did not match the pattern Q[A-Z]{4}\d{3}\w{2} or checksum"
                sc = SearchCriteria()
                sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, "MA"+identifier))
                foundSamples = search_service.searchForSamples(sc)

                sampleIdentifier = foundSamples[0].getSampleIdentifier()
                space = foundSamples[0].getSpace()
                sa = transaction.getSampleForUpdate(sampleIdentifier)

            # create new dataset 
            dataSet = transaction.createNewDataSet("CHIP_IMAGE")
            dataSet.setMeasuredData(False)
            dataSet.setSample(sa)

            iamge = os.path.realpath(os.path.join(incomingPath,name))

            transaction.moveFile(image, dataSet)
