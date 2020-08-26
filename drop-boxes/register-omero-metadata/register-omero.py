'''
Note:
print statements go to: ~openbis/servers/datastore_server/log/startup_log.txt
'''
import sys
sys.path.append('/home-link/qeana10/bin/')

from life.qbic import TrackingHelper
from life.qbic import SampleNotFoundException
import sample_tracking_helper_qbic as thelper
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

def createNewImagingExperiment(tr, space, project, properties):
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
	exp = tr.createNewExperiment(exp_ID, IMAGING_EXP_TYPE)
	for key in properties.keys():
		exp.setPropertyValue(key, properties[key])
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

#TODO Luis
def callOmeroWithFilePath(file_path, sample_barcode):
	list_of_omero_ids = ["1","2","3"]
	return list_of_omero_ids

def getFileFromLine(line):
	return line.split("\t")[0]

# dependent on metadata dictionaries of two different files (data model), decide if new openBIS experiment needs to be created
# might be replaced by specific metadata properties, once we know more
def isSameExperimentMetadata(props1, props2):
	# initilization of tsv parser, always results in new experiment
	if not props1 or not props2:
		return False
	else:
		#TODO

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

	metadataFile = None
	for root, subFolders, files in os.walk(incomingPath):
		for f in files:
			stem, ext = os.path.splitext(f)
			if ext.lower()=='.tsv':
				with open(os.path.join(root, f), 'U') as fh: metadataFile = fh.readlines()
	offset = 0
	# see below
	previousProps = None
	# go through the metadatafile containing all pre-specified imaging metadata
	header = metadataFile[0]
	for line in metadataFile[1:]:  # (Exclude header)
		# Get modality and other metadata from tsv here for one sample
		properties = {}
		tokens = line.split("\t")
		for i in range(len(header)):
			properties[header[i]] = tokens[i]
		fileName = getFileFromLine(line)# or use dictionary

		imageFile = os.path.join(incomingPath, fileName)
		print "handling file "+imageFile

		list_of_omero_ids = callOmeroWithFilePath(imageFile, code)
		# TODO decide if new experiment is needed based on some pre-defined criteria.
		# Normally, the most important criterium is collision of experiment type properties
		# between samples. E.g. two different imaging modalities need two experiments.
		fileBelongsToExistingExperiment = isSameExperimentMetadata(previousProps, properties)
		previousProps = properties
		if(not fileBelongsToExistingExperiment):
			exp = createNewImagingExperiment(transaction, space, project, properties)
		imagingSample = createNewImagingRun(transaction, sa, exp, list_of_omero_ids, offset)# maybe there are sample properties, too!
		# register the actual data
		IMAGING_DATASET_CODE = Q_BMI_GENERIC_IMAGING_DATA # I guess
		dataset = transaction.createNewDataSet(IMAGING_DATASET_CODE)
		dataset.setSample(imagingSample)
		transaction.moveFile(imageFile, dataset)
		# increment id offset for next sample in this loop - not sure anymore if this is needed
		offset+=1










