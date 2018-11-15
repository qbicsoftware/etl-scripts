'''
ETL script for registration of peptide data files containing peptide sequences

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

# expected code: *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
pattern = re.compile('Q\w{4}[0-9]{3}[a-zA-Z]\w')

# Check barcode for integrity
def isExpected(identifier):
    try:
        id = identifier[0:9]
        return checksum.checksum(id)==identifier[9]
    except:
        return False

# Main function which will be triggered upon registration
def process(transaction):
    context = transaction.getRegistrationContext().getPersistentMap()

    # Get the incoming path of the transaction
    incomingPath = transaction.getIncoming().getAbsolutePath()

    key = context.get("RETRY_COUNT")
    if (key == None):
        key = 1

    # Get the name of the incoming file
    name = transaction.getIncoming().getName()

    # Parse experiment, project and sample code       
    identifier = pattern.findall(name)[0]
    if isExpected(identifier):
        experiment = identifier[1:5]
        project = identifier[:5]
        parentCode = identifier[:10]
    else:
        print "The identifier "+identifier+" did not match the pattern Q[A-Z]{4}\d{3}\w{2} or checksum"

    # Initialize search service and search for sample using the provided code    
    search_service = transaction.getSearchService()
    sc = SearchCriteria()
    sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, parentCode))
    foundSamples = search_service.searchForSamples(sc)

    # Get sample ID and retrieve the sample for update
    parentSampleIdentifier = foundSamples[0].getSampleIdentifier()
    space = foundSamples[0].getSpace()
    parentSample = transaction.getSampleForUpdate(parentSampleIdentifier)

    # Create new peptide dataset and attach it to the found sample
    dataSet = transaction.createNewDataSet("Q_PEPTIDE_DATA")
    dataSet.setMeasuredData(False)
    dataSet.setSample(parentSample)

    # Move the file(s) to the new dataset
    transaction.moveFile(incomingPath, dataSet)
