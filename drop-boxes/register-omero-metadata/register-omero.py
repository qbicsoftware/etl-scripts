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

class Error(Exception):
	"""Base class for exceptions in this module."""
	pass

	def __init__(self, call, msg):
		self.call = call
		self.msg = msg

class BarcodeError(Error):
	def __init__(self, barcode, msg):
		self.barcode = barcode
		self.msg = msg

#class OmeroError(Error):

# ETL script for registration of Imaging files that need to end up in Omero.
#
# Expected input characteristics:
# 1. Input needs to contain imaging files that can be displayed in Omero
# a) Input may be a folder containing other metadata
# b) This is to be specified with the imaging facility and our datahandler
# 2. Input needs to be barcoded: 
# a) Since experiments and samples for the imaging run should not exist yet in openBIS, we expect the previous level's barcode
#    This should be a tissue sample (Q_BIOLOGICAL_SAMPLE).
# b) This barcode follows the pattern:
barcode_pattern = re.compile('Q[a-zA-Z0-9]{4}[0-9]{3}[A-Z][a-zA-Z0-9]')
# c) example: QW12X001AB
# d) (the checksum digit is correct)
#
# Expected behaviour:
# 1. Given the barcode, find sample in openBIS - this is a tissue sample
#
# 2. Given meta-information in filename or metadata file, create new experiment and sample for this run
# a) There can be multiple imaging runs per tissue sample.
# b) If they are of the same type, they can use the same experiment, but different RUN samples
# c) If they are not registered with the same file/folder, they will get different experiments
#
# 3. Call external script to register data in Omero. Return the relevant Omero image ID(s)
# 4. Add Omero image ID(s) or URL containing ID to openBIS imaging RUN sample object
# 5. Finish transaction

# Error handling:
# An exception needs to be thrown if anything goes wrong. This is important so that the ETL transaction does not finish
# and delete the data!
#####

def isExpected(identifier):
	try:
		id = identifier[0:9]
		#also checks for old checksums with lower case letters
		return checksum.checksum(id)==identifier[9]
	except:
		return False

def createNewImagingExperiment(tr, space, project, modality):
	IMAGING_EXP_TYPE = "Q_BMI_GENERIC_IMAGING"
	MODALITY_CODE = "Q_BMI_MODALITY"
	search_service = tr.getSearchService()

	existing_ids = []
	existing_exps = search_service.listExperiments("/" + space + "/" + project)
	for exp in existing_exps:
		existing_ids.append(exp.getExperimentIdentifier())
	exp_id = existing_ids[0]
	i = 0
	while exp_id in existing_ids:
		i += 1
		exp_num = len(existing_exps) + i
		exp_id = '/' + space + '/' + project + '/' + project + 'E' + str(exp_num)
	exp = tr.createNewExperiment(expID, IMAGING_EXP_TYPE)
	exp.setPropertyValue(MODALITY_CODE, modality)
	return exp

def createNewImagingRun(tr, base_sample, exp, omero_link, run_offset):
	IMG_RUN_PREFIX = "IMG"
	IMG_RUN_TYPE = "Q_BMI_GENERIC_IMAGING_RUN"
	IMG_RUN_OMERO_PROPERTY_CODE = "Q_ADDITIONAL_INFO"
	# TODO: can we use a prefix for imaging samples?
	# otherwise creating new samples will be more complex
	# on the other hand, replicates need to be numbered if we use IMG, e.g IMG1QABCD001AB
	# IMG2QABCD001AB etc.
	# talk to GG and LK
	run = 0
	exists = True
	new_sample_id = None
	while exists:
		run += 1
		new_sample_id = '/' + base_sample.getSpace() + '/' + IMG_RUN_PREFIX + str(run) + base_sample.getCode()
		exists = tr.getSampleForUpdate(new_sample_id)
	new_sample_id_with_offset = '/' + base_sample.getSpace() + '/' + IMG_RUN_PREFIX + str(run+run_offset) + base_sample.getCode()
	img_run = tr.createNewSample(new_sample_id_with_offset, IMG_RUN_TYPE)
	img_run.setParentSampleIdentifiers([base_sample.getSampleIdentifier])
	img_run.setExperiment(exp)
	img_run.setPropertyValue(IMG_RUN_OMERO_PROPERTY_CODE, omero_link)
	return img_run

def process(transaction):
	context = transaction.getRegistrationContext().getPersistentMap()

	# Get the incoming path of the transaction
	incomingPath = transaction.getIncoming().getAbsolutePath()

	key = context.get("RETRY_COUNT")
	if (key == None):
		key = 1

	# Get the name of the incoming file
	name = transaction.getIncoming().getName()
	found = barcode_pattern.findall(name)
	if len(found) == 0:
		raise BarcodeError(name, "barcode pattern was not found")
	code = found[0]
	if isExpected(code):
		project = code[:5]
	else:
		raise BarcodeError(code, "checksum for barcode is wrong")        

	search_service = transaction.getSearchService()
	sc = SearchCriteria()
	sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, code))
	foundSamples = search_service.searchForSamples(sc)
	sample = None
	space = None
	sa = None

	if len(foundSamples) == 0:
		raise BarcodeError(code, "sample was not found in openBIS")
	sample = foundSamples[0]
	sampleID = sample.getSampleIdentifier()
	sa = transaction.getSampleForUpdate(sampleID)
	space = sa.getSpace()

	# Get modality from metadata here
	modality = "CT-BIOPSY"
	exp = createNewImagingExperiment(transaction, space, project, modality)
	# Get Omero id(s) from Omero before creating sample(s) in openBIS
	offset = 0
	for f in omero_image:
		omero_id = "omero-test-id"
		createNewImagingRun(transaction, sa, exp, omero_id, offset)
		offset+=1
	# done?