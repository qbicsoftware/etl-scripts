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
        if isExpected(identifier):
                project = identifier[:5]
                code = identifier[:10]
        else:
             	print "The identifier "+identifier+" did not match the pattern Q[A-Z]{4}\d{3}\w{2} or checksum"
        # Find the test sample
        search_service = transaction.getSearchService()
        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, code))
        foundSamples = search_service.searchForSamples(sc)

        sampleIdentifier = foundSamples[0].getSampleIdentifier()
        space = foundSamples[0].getSpace()
        sa = transaction.getSampleForUpdate(sampleIdentifier)

        # get or create MS-specific experiment/sample and attach to the test sample
        expType = "Q_MS_MEASUREMENT"
        MSRawExperiment = None
        experiments = search_service.listExperiments("/" + space + "/" + project)
        experimentIDs = []
        for exp in experiments:
            experimentIDs.append(exp.getExperimentIdentifier())
            if exp.getExperimentType() == expType:
                # if we want to assume that a project can have multiple ms run experiments we need to check if other
                # samples in the ms run come from parent samples in the same experiment as the sample carrying this barcode
                if isCurrentMSRun(transaction, sa.getExperiment().getExperimentIdentifier(), exp.getExperimentIdentifier()):
	        	MSRawExperiment = exp
        # no existing experiment found        
        if not MSRawExperiment:
            expID = experimentIDs[0]
            i = 0
            while expID in experimentIDs:
                i+=1
                expNum = len(experiments)+i
                expID = '/' + space + '/' + project + '/' + project + 'E' + str(expNum)
            MSRawExperiment = transaction.createNewExperiment(expID, expType)
	    #the following are placeholders that either need to be parsed from the filename/incoming dropbox/additional files or set later when the metadata is available
	    protocol = "PTX_LABELFREE"
	    chromType = "DIRECT_INFUSION"
	    device = "PCT_THERMO_ORBITRAP_XL"
            MSRawExperiment.setPropertyValue("Q_MS_PROTOCOL", protocol)
            MSRawExperiment.setPropertyValue("Q_CHROMATOGRAPHY_TYPE", chromType)
            MSRawExperiment.setPropertyValue("Q_MS_DEVICE", device)
        newMSSample = transaction.createNewSample('/' + space + '/' + 'MS'+ code, "Q_MS_RUN")
        newMSSample.setParentSampleIdentifiers([sa.getSampleIdentifier()])
        newMSSample.setExperiment(MSRawExperiment)
        
        # create new dataset
        dataSet = transaction.createNewDataSet("Q_MS_RAW_DATA")
        dataSet.setMeasuredData(False)
        dataSet.setSample(sa)

        transaction.moveFile(incomingPath, dataSet)
