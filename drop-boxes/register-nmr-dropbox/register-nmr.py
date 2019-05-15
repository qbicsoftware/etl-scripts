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

        key = context.get("RETRY_COUNT")
        if (key == None):
                key = 1


        # Get the name of the incoming file
        name = transaction.getIncoming().getName()
        
        identifier = pattern.findall(name)[0]
        if isExpected(identifier):
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
	experiments = search_service.listExperiments("/" + space + "/" + project)
	exp = None
	for e in experiments:
		if e.getExperimentType() == "Q_NMR_MEASUREMENT":
			exp = e
	if not exp:
		numberOfExperiments = len(experiments) + 1
		exp = transaction.createNewExperiment('/' + space + '/' + project + '/' + project + 'E' + str(numberOfExperiments), "Q_NMR_MEASUREMENT")

        newSample = transaction.createNewSample('/' + space + '/' + 'NMR'+ parentCode, "Q_NMR_SAMPLE_RUN")
        newSample.setParentSampleIdentifiers([sa.getSampleIdentifier()])
      
	newSample.setExperiment(exp) 
        # create new dataset 
        dataSet = transaction.createNewDataSet("Q_NMR_RAW_DATA")
        dataSet.setMeasuredData(False)
        dataSet.setSample(newSample)

        f = "source_dropbox.txt"
       	sourceLabFile = open(os.path.join(incomingPath,f))
        sourceLab = sourceLabFile.readline().strip()
        sourceLabFile.close()
        os.remove(os.path.realpath(os.path.join(incomingPath,f)))

        for f in os.listdir(incomingPath):
               	if ".origlabfilename" in f:
                       	os.remove(os.path.realpath(os.path.join(incomingPath,f)))
        transaction.moveFile(incomingPath, dataSet)
