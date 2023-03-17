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
#ppattern = re.compile('Q\w{4}000')
#epattern = re.compile('Q\w{4}E[1-9][0-9]*')

# contains properties used to send emails, e.g. the mail server and the "from" address to use
import email_helper_qbic as email_helper
# provides functionality used to send emails, e.g. about registration errors
import etl_mailer

class MetadataFormattingException(Exception):
	"Thrown when metadata file cannot be successfully parsed."
	pass

class NoSamplesFoundForProjectException(Exception):
	"Thrown when sample cannot be found in openBIS."
	pass

class CouldNotCreateException(Exception):
	"Thrown when necessary experiment or sample could not be created."
	pass

def getInfoExperimentIdentifier(space, project):
	experimentID = '/' + space + '/' + project + '/'+ project+'_INFO'
	return experimentID

def extractProjectCode(sampleCode):
	return sampleCode[:5]

def process(transaction):
	error = None
	originalError = None
	context = transaction.getRegistrationContext().getPersistentMap()

	# Get the incoming path of the transaction
	incomingPath = transaction.getIncoming().getAbsolutePath()

	try:
		#read in the metadata file
		for f in os.listdir(incomingPath):
			if f == "metadata.txt":
				metadata = open(os.path.join(incomingPath, f))
				fileInfo = dict()
				for line in metadata:
					try:
						pair = line.strip().split('=')
						fileInfo[pair[0]] = pair[1]
					except IndexError as exception:
						originalError = exception
						error = MetadataFormattingException("Metadata file not correctly formatted. Check for additional line breaks.")
				metadata.close()
				try:
					user = fileInfo["user"]
				except:
					user = None
				secname = fileInfo["info"]
				sampleCode = fileInfo["barcode"]
				datasetType = fileInfo["type"]
			else:
				name = f

		project = extractProjectCode(sampleCode)
		type = "INFORMATION"
		if "Results" in datasetType:
			type = "RESULT"
		if error:
			raise error
		if user:
			transaction.setUserId(user)

		inputFile = os.path.join(incomingPath, name)
		newname = urllib.unquote(name)
		dataFile = os.path.join(incomingPath, newname)
		print "renaming "+inputFile+" to "+dataFile
		os.rename(inputFile, dataFile)

		search_service = transaction.getSearchService()
		sc = SearchCriteria()
		sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, sampleCode))
		foundSamples = search_service.searchForSamples(sc)
		sample = None
		space = None
		sa = None
		attachmentSampleFound = True

		if len(foundSamples) == 0:
			attachmentSampleFound = False
			sc = SearchCriteria()
			sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, project+"ENTITY-1"))
			foundSamples = search_service.searchForSamples(sc)
		try:
			sample = foundSamples[0]
		except IndexError as exception:
			originalError = exception
			error = NoSamplesFoundForProjectException("No sample could be found for this project.")
		sampleID = sample.getSampleIdentifier()
		sa = transaction.getSampleForUpdate(sampleID)
		space = sa.getSpace()
		if not attachmentSampleFound:
			# fetch it by name
			infoSampleID = "/"+space+"/"+sampleCode
			sa = transaction.getSampleForUpdate(infoSampleID)
		if not sa:
			# create necessary objects if sample really doesn't exist
			try:
				experimentID = getInfoExperimentIdentifier(space, project)
				exp = transaction.createNewExperiment(experimentID, "Q_PROJECT_DETAILS")
			except Exception as exception:
				originalError = exception
				error = CouldNotCreateException("Experiment "+experimentID+" could not be created.")
			try:
				sa = transaction.createNewSample(infoSampleID, "Q_ATTACHMENT_SAMPLE")
			except Exception as exception:
				originalError = exception
				error = CouldNotCreateException("Sample "+infoSampleID+" was not found and could not be created.")
			sa.setExperiment(exp)

		dataSet = transaction.createNewDataSet("Q_PROJECT_DATA")
		dataSet.setMeasuredData(False)
		dataSet.setPropertyValue("Q_SECONDARY_NAME", secname)
		dataSet.setPropertyValue("Q_ATTACHMENT_TYPE", type)
		dataSet.setSample(sa)
		transaction.moveFile(dataFile, dataSet)
	# catch other, unknown exceptions
	except Exception as exception:
		originalError = exception
		if not error:
			error = Exception("Unknown exception occured: "+str(exception))
	if originalError:
		# if there is a problem sending the email, we log this and raise the original exception
		try:
			mailFactory = etl_mailer.EmailFactory()
			etlMailer = etl_mailer.ETLMailer(email_helper.get_mail_host(), email_helper.get_mail_server(), email_helper.get_mail_from())
			
			subject = "Error storing project attachment uploaded for "+project
			content = mailFactory.formatRegistrationErrorContent(str(error))
			etlMailer.send_email([user], subject, content)
		except Exception as mailException:
			print "Could not send error email: "+str(mailException)
			pass
		raise originalError
