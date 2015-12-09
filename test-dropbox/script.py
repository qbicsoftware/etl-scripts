'''

Note:
print statements go to: ~openbis/servers/datastore_server/log/startup_log.txt
'''
import sys
sys.path.append('/home-link/qeana10/bin/')

import re
import os
import time
import ch.systemsx.cisd.etlserver.registrator.api.v2
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria

# This is a test dropbox for multiple purposes so we don't have to restart openbis every time we test something. Ask the developers before you change this code.
pattern = re.compile('Q\w{4}[0-9]{3}[a-zA-Z]\w')

def isExpected(identifier):
        try:
		id = identifier[0:9]
                return True
		#also checks for old checksums with lower case letters
                #return checksum.checksum(id)==identifier[9]
        except:
                return False

def isCurrentMSRun(tr, parentExpID, msExpID):
        search_service = tr.getSearchService()
        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.TYPE, "Q_MS_RUN"))
        foundSamples = search_service.searchForSamples(sc)

	for samp in foundSamples:
		currentMSExp = samp.getExperiment()
		print "ms samp: "+samp.getCode()
		print "is sample's experiment "+currentMSExp.getExperimentIdentifier()+" the same as "+msExpID
		if currentMSExp.getExperimentIdentifier() == msExpID:
			for parID in samp.getParentSampleIdentifiers():
				print "parent "+parID
				parExp = tr.getSampleForUpdate(parID).getExperiment().getExperimentIdentifier()
				print "parent experiment "+parExp+", target experiment "+parentExpID
				if parExp == parentExpID:
					return True
	return False

def process(transaction):
        context = transaction.getRegistrationContext().getPersistentMap()

        # Get the incoming path of the transaction
        incomingPath = transaction.getIncoming().getAbsolutePath()
	# Get the name of the incoming file
        name = transaction.getIncoming().getName()
        key = context.get("RETRY_COUNT")
        if (key == None):
                key = 1

        identifier = pattern.findall(name)[0]
	code = None
        code = identifier[:10]
        # Find the test sample
        search_service = transaction.getSearchService()
        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, code))
        foundSamples = search_service.searchForSamples(sc)

        sampleIdentifier = foundSamples[0].getSampleIdentifier()
        space = foundSamples[0].getSpace()
        sa = transaction.getSampleForUpdate(sampleIdentifier)

        # create new dataset
        dataSet = transaction.createNewDataSet("EXPRESSION_MATRIX")
        dataSet.setMeasuredData(False)
        dataSet.setSample(sa)

        transaction.moveFile(incomingPath, dataSet)
