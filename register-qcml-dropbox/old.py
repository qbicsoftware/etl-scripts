'''

Note: 
print statements go to: ~openbis/servers/datastore_server/log/startup_log.txt
'''
import sys
sys.path.append('/home-link/qeana10/bin/')

import re
import os
import shutil
import subprocess
import ch.systemsx.cisd.etlserver.registrator.api.v2
from java.io import File
from org.apache.commons.io import FileUtils
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria

# Data import and registration
# expected:
# *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
pattern = re.compile('MSQCQ\w{4}[0-9]{3}[a-zA-Z]\w')
# example: QSNYD001X7.fastq and QSNYD001X7_fastqc.zip
# every file containing a sample name is attached to the respective sample

ext = 'qcml'

def isExpected(identifier, extension):
	try:
		id = identifier[3:14]
		return extension.lower() == ext
		#also checks for old checksums with lower case letters
		#return checksum.checksum(id)==identifier[9]
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
	space = "DEFAULT" #will be set later
	project = "DEFAULT"
	experiment = "DEFAULT"
	sample_id = "DEFAULT"

	extension = name.split(".")[-1]
	identifier = pattern.findall(name)[0]
	if isExpected(identifier, extension):
		print "found identifier"
		#experiment = identifier[1:5]
		project = identifier[4:9]
		sample_id = identifier[:14]
	else:
		print "The identifier "+identifier+" did not match the pattern MSQCQ[A-Z]{4}\d{3}\w{2}, the checksum was wrong or the file is not of type ."+ext

	#Register file
	dataSet = transaction.createNewDataSet('Q_WF_MS_QUALITYCONTROL_RESULTS')
	dataSet.setMeasuredData(False)
	
	search_service = transaction.getSearchService()
	sc = SearchCriteria()
	sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, sample_id))
	foundSamples = search_service.searchForSamples(sc)

	if foundSamples.size() > 0:
		sa = transaction.getSampleForUpdate(foundSamples[0].getSampleIdentifier())
		transaction.moveFile(incomingPath, dataSet)
		dataSet.setSample(sa)
	else:
		#Sample not found, something went wrong and the file is attached to an "Unknown" sample
		transaction.moveFile(incomingPath, dataSet)
		newSample = transaction.createNewSample('/' + space + '/' + sample_id, 'UNKNOWN')
		newSample.setExperiment(experiment)
		dataSet.setSample(newSample)
