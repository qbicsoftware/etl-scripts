'''

Note:
print statements go to: ~openbis/servers/datastore_server/log/startup_log.txt
'''
import sys
sys.path.append('/home-link/qeana10/bin/')

import checksum
import re
import os
import urllib
import ch.systemsx.cisd.etlserver.registrator.api.v2
from java.io import File
from org.apache.commons.io import FileUtils
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria

# ETL script for registration of arbitrary files that should be seen on the project or experiment level
# they are attached to specific samples, e.g. QBBBB000 for the project, QBBBBE1-000 for experiments
# expected:
# *Q[Project Code]^4000*.*
# *Q[Project Code]^4E[Experiment Number]-000*
# IMPORTANT: ONLY PROJECT LEVEL WORKING RIGHT NOW
ppattern = re.compile('Q\w{4}000')
#epattern = re.compile('Q\w{4}E[1-9][0-9]*')

def process(transaction):
	context = transaction.getRegistrationContext().getPersistentMap()

	# Get the incoming path of the transaction
	incomingPath = transaction.getIncoming().getAbsolutePath()

	key = context.get("RETRY_COUNT")
	if (key == None):
		key = 1

	#read in the metadata file
	for f in os.listdir(incomingPath):
		if f == "metadata.txt":
			metadata = open(os.path.join(incomingPath, f))
			fileInfo = dict(line.strip().split('=') for line in metadata)
			metadata.close()
			try:
				user = fileInfo["user"]
			except:
				user = None
			secname = fileInfo["info"]
			code = fileInfo["barcode"]
			datasetType = fileInfo["type"]
		else:
			name = f

	project = code[:5]
	type = "INFORMATION"
	if "Results" in datasetType:
		type = "RESULT"

	if user:
		transaction.setUserId(user)

	inputFile = os.path.join(incomingPath, name)
	newname = urllib.unquote(name)
	dataFile = os.path.join(incomingPath, newname)
	print "renaming "+inputFile+" to "+dataFile
	os.rename(inputFile, dataFile)

	search_service = transaction.getSearchService()
	sc = SearchCriteria()
	sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, code))
	foundSamples = search_service.searchForSamples(sc)
	sample = None
	space = None
	sa = None
	attachmentReady = True

	if len(foundSamples) == 0:
		attachmentReady = False
		sc = SearchCriteria()
		sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, project+"ENTITY-1"))
		foundSamples = search_service.searchForSamples(sc)
	sample = foundSamples[0]
	sampleID = sample.getSampleIdentifier()
	sa = transaction.getSampleForUpdate(sampleID)
	space = sa.getSpace()
	if not attachmentReady:
		infoSampleID = "/"+space+"/"+code
		sa = transaction.getSampleForUpdate(infoSampleID)
	if not sa:
		exp = transaction.createNewExperiment('/' + space + '/' + project + '/'+ project+'_INFO', "Q_PROJECT_DETAILS")
		sa = transaction.createNewSample('/' + space + '/'+ code, "Q_ATTACHMENT_SAMPLE")
		sa.setExperiment(exp)
	info = None

	#if isProject:
	#experiments = search_service.listExperiments("/" + space + "/" + project)
	#for e in experiments:
	#	if project+"_INFO" in e.getExperimentIdentifier():
	#		info = e
	#if not info:
	#	info = transaction.createNewExperiment('/' + space + '/' + project + '/'+ project+'_INFO', "Q_PROJECT_DETAILS")
	#else:
	#	info = transaction.getExperiment('/' + space + '/' + project + '/' + code)
	# register new experiment and sample
	#sa.setExperiment(info) 
	# create new dataset 
	dataSet = transaction.createNewDataSet("Q_PROJECT_DATA")
	dataSet.setMeasuredData(False)
	dataSet.setPropertyValue("Q_SECONDARY_NAME", secname)
	dataSet.setPropertyValue("Q_ATTACHMENT_TYPE", type)
	dataSet.setSample(sa)
	transaction.moveFile(dataFile, dataSet)


