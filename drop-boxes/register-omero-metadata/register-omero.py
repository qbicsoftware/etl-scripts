'''
Note:
print statements go to: ~openbis/servers/datastore_server/log/startup_log.txt
'''
import sys
sys.path.append('/home-link/qeana10/bin/')
import image_registration_process as irp

#from life.qbic import TrackingHelper
#from life.qbic import SampleNotFoundException
#import sample_tracking_helper_qbic as thelper

import checksum
import datetime
import re
import os
import urllib
import ch.systemsx.cisd.etlserver.registrator.api.v2
from java.io import File
from org.apache.commons.io import FileUtils
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria



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

INCOMING_DATE_FORMAT = '%d.%m.%Y'
OPENBIS_DATE_FORMAT = '%Y-%m-%d'

def mapDateString(date_string):
	return datetime.datetime.strptime(date_string, INCOMING_DATE_FORMAT).strftime(OPENBIS_DATE_FORMAT)

def createNewImagingExperiment(tr, space, project, properties, existing_ids):
	IMAGING_EXP_TYPE = "Q_BMI_GENERIC_IMAGING"
	search_service = tr.getSearchService()
	experiment_property_map = {"IMAGING_MODALITY":"Q_BMI_MODALITY", "IMAGING_DATE":"Q_MEASUREMENT_FINISH_DATE", "INSTRUMENT_USER":"Q_INSTRUMENT_USER"}

	existing_exps = search_service.listExperiments("/" + space + "/" + project)
	for exp in existing_exps:
		existing_ids.append(exp.getExperimentIdentifier())
	exp_id = existing_ids[0]
	i = 0
	while exp_id in existing_ids:
		i += 1
		exp_num = len(existing_exps) + i
		exp_id = '/' + space + '/' + project + '/' + project + 'E' + str(exp_num)
	img_exp = tr.createNewExperiment(exp_id, IMAGING_EXP_TYPE)
	existing_ids.append(exp_id)
	for incoming_label in experiment_property_map:
		if incoming_label in properties:
			key = experiment_property_map[incoming_label]
			value = properties[incoming_label]
			if key == "Q_MEASUREMENT_FINISH_DATE":
				value = mapDateString(value)
			img_exp.setPropertyValue(key, value)
	return img_exp

def createNewImagingRun(tr, base_sample, exp, omero_image_ids, run_offset, properties):
	IMG_RUN_PREFIX = "IMG"
	IMG_RUN_TYPE = "Q_BMI_GENERIC_IMAGING_RUN"
	IMG_RUN_OMERO_PROPERTY_CODE = "Q_OMERO_IDS"
	sample_property_map = {}#no specific properties from the metadata file yet

	run = 0
	exists = True
	new_sample_id = None
	# respect samples already in openbis
	while exists:
		run += 1
		new_sample_id = '/' + base_sample.getSpace() + '/' + IMG_RUN_PREFIX + str(run) + base_sample.getCode()
		exists = tr.getSampleForUpdate(new_sample_id)
	# add additional offset for samples registered in this call of the ETL script, but before this sample
	new_sample_id_with_offset = '/' + base_sample.getSpace() + '/' + IMG_RUN_PREFIX + str(run+run_offset) + base_sample.getCode()
	img_run = tr.createNewSample(new_sample_id_with_offset, IMG_RUN_TYPE)
	img_run.setParentSampleIdentifiers([base_sample.getSampleIdentifier()])
	img_run.setExperiment(exp)
	img_run.setPropertyValue(IMG_RUN_OMERO_PROPERTY_CODE, '\n'.join(omero_image_ids))
	for incoming_label in sample_property_map:
		if incoming_label in properties:
			key = sample_property_map[incoming_label]
			value = properties[incoming_label]
			img_run.setPropertyValue(key, value)
	return img_run

def getFileFromLine(line):
	return line.split("\t")[0]

def isSameExperimentMetadata(props1, props2):
	"""dependent on metadata dictionaries of two different files (data model), decide if new openBIS experiment needs to be created
	"""
	relevantPropertyNames = ["IMAGING_MODALITY", "IMAGED_TISSUE", "INSTRUMENT_MANUFACTURER", "INSTRUMENT_USER", "IMAGING_DATE"]
	for label in relevantPropertyNames:
		if label in props1 and label in props2:
			if props1[label] != props2[label]:
				return False
		if label in props1 and not label in props2:
			return False
		if label in props2 and not label in props1:
			return False
	return True

def registerImageInOpenBIS(transaction):
	search_service = transaction.getSearchService()
	sc = SearchCriteria()
	sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, sample_code))
	foundSamples = search_service.searchForSamples(sc)
	sample = None
	space = None
	sa = None

	if len(foundSamples) == 0:
		raise BarcodeError(sample_code, "sample was not found in openBIS")
	sample = foundSamples[0]
	sampleID = sample.getSampleIdentifier()
	sa = transaction.getSampleForUpdate(sampleID)
	space = sa.getSpace()

def findMetaDataFile(incomingPath):
	"""Scans the incoming path for a metadata tsv file.
	Returns a list with parsed entry lines of the tsv file. 
	Is empty if no file was found.
	"""
	metadataFileContent = []
	for root, subFolders, files in os.walk(incomingPath):
		for f in files:
			stem, ext = os.path.splitext(f)
			if ext.lower()=='.tsv':
				with open(os.path.join(root, f), 'U') as fh: metadataFileContent = fh.readlines()
	return metadataFileContent

def getPropertyNames(metadataFile):
	"""Here we could add more complex behaviour later on.
	"""
	
	property_names = metadataFile[0].split("\t")
	for i in range(len(property_names)):
		property_names[i] = property_names[i].strip().upper()

	return property_names

def validatePropertyNames(property_names):
	"""Validate metadata property names.
	TODO: call the imaging metadata parser (with json schema).
	"""

	# fast validation without parser object.
	required_names = ["IMAGE_FILE_NAME", "IMAGING_MODALITY", "IMAGED_TISSUE", "INSTRUMENT_MANUFACTURER", "INSTRUMENT_USER", "IMAGING_DATE"]

	for name in required_names:
		if not name in property_names:
			return False

	return True

def getPropertyMap(line, property_names):
	"""Build the property map. Here we could add more complex behaviour later on.
	"""

	properties = {}
	property_values = line.split("\t")

	for i in range(1, len(property_names)): #exclude first col (filename)
		##remove trailing newline, and replace space with underscore
		name = property_names[i].rstrip('\n').replace(" ", "_")
		value = property_values[i].rstrip('\n').replace(" ", "_")

		properties[name] = value

	return properties

def filterOmeroPropertyMap(property_map):
	"""Filters map before ingestion into omero server
	"""

	#the blacklist, e.g. what is going to openBIS or is automatically added to omero (e.g. file name)
	filter_list = ["IMAGE_FILE_NAME", "INSTRUMENT_USER", "IMAGING_DATE"]

	new_props = {}
	for key in property_map.keys():
		if not key in filter_list:
			new_props[key] = property_map[key]

	return new_props


def printPropertyMap(property_map):
	"""Function to display metadata properties.
	"""

	print("KEY : VALUE")
	for key in property_map.keys():
		print "--> " + str(key) + " : " + str(property_map[key])


def process(transaction):
	print "start transaction"
	"""The main entry point.
	
	openBIS calls this method, when an incoming transaction is registered.
	This happens, when the data has been moved into the openBIS dropbox AND a marker file 
	was created (event trigger).
	"""
	context = transaction.getRegistrationContext().getPersistentMap()

	# Get the incoming path of the transaction
	incomingPath = transaction.getIncoming().getAbsolutePath()

	print incomingPath

	# 1. Initialize the image registration process
	registrationProcess = irp.ImageRegistrationProcess(transaction)

	print "started reg. process"
	
	# 2. We want to get the openBIS sample code from the incoming data
	# This tells us to which biological sample the image data was aquired from.
	project_code, sample_code = registrationProcess.fetchOpenBisSampleCode()

	print project_code
	print sample_code

	#find specific sample
	tissueSample = registrationProcess.searchOpenBisSample(sample_code)
	space = tissueSample.getSpace()

	print tissueSample
	print space
	# 3. We now request the associated omero dataset id for the openBIS sample code.
	# Each dataset in OMERO contains the associated openBIS biological sample id, which
	# happened during the experimental design registration with the projectwizard.

	print "calling omero..."
	#returns -1 if operation failed
	omero_dataset_id = registrationProcess.requestOmeroDatasetId(project_code=project_code, sample_code=sample_code)

	print "omero dataset id:"
	print omero_dataset_id

	omero_failed = int(omero_dataset_id) < 0
	if omero_failed:
		raise ValueError("Omero did not return expected dataset id.")

	# Find and parse metadata file content
	metadataFile = findMetaDataFile(incomingPath)

	print "metadataFile:"
	print metadataFile

	property_names = getPropertyNames(metadataFile)

	print "property names:"
	print property_names

	valid_names = validatePropertyNames(property_names)
	if not valid_names:
		raise ValueError("Invalid Property Names.")

	#keep track of number of images for openBIS ID
	image_number = 0
	#Initialize openBIS imaging experiment
	imagingExperiment = None
	previousProps = {}
	existing_experiment_ids = []

	print "start reading metadata file"
	# Iterate over the metadata entries containing all pre-specified imaging metadata
	for line in metadataFile[1:]:  # (Exclude header)
		# Get modality and other metadata from tsv here for one sample
		properties = {}
		
		# Retrieve the image file name, please no whitespace characters in filename!
		fileName = getFileFromLine(line)

		imageFile = os.path.join(incomingPath, fileName)
		print "New incoming image file for OMERO registration:\t" + imageFile

		# 4. After we have received the omero dataset id, we know where to attach the image to
		# in OMERO. We pass the omero dataset id and trigger the image registration process in OMERO.
		omero_image_ids = registrationProcess.registerImageFileInOmero(imageFile, omero_dataset_id)
		print "Created OMERO image identifiers:\t" + str(omero_image_ids)

		omero_failed = len(omero_image_ids) < 1
		if omero_failed:
			raise ValueError("Omero did not return expected image ids.")

		# 5. Additional metadata is provided in an own metadata TSV file. 
		# We extract the metadata from this file.
		#registrationProcess.extractMetadataFromTSV()

		properties = getPropertyMap(line, property_names)
		print "Metadata properties:\t"
		printPropertyMap(properties)
		
		#one file can have many images, iterate over all img ids
		for img_id in omero_image_ids:
			registrationProcess.registerKeyValuePairs(img_id, filterOmeroPropertyMap(properties))


		####
		# 6. In addition to the image registration and technical metadata storage, we want to add
		# further experimental metadata in openBIS. This metadata contains information about the 
		# imaging experiment itself, such as modality, imaged tissue and more. 
		# We also want to connect this data with the previously created, corresponding OMERO image id t
		# hat represents the result of this experiment in OMERO. 
		#registrationProcess.registerExperimentDataInOpenBIS(omero_image_ids) # I did it myyy wayyyy

		# we decide if new experiment is needed based on some pre-defined criteria.
		# Normally, the most important criterium is collision of experiment type properties
		# between samples. E.g. two different imaging modalities need two experiments.
		
		fileBelongsToExistingExperiment = isSameExperimentMetadata(previousProps, properties)
		previousProps = properties
		if(not fileBelongsToExistingExperiment):
			imagingExperiment = createNewImagingExperiment(transaction, space, project_code, properties, existing_experiment_ids)
		imagingSample = createNewImagingRun(transaction, tissueSample, imagingExperiment, omero_image_ids, image_number, properties)
		# increment id offset for next sample in this loop
		image_number += 1

		# 7. Last but not least we create the open science file format for images which is
		# OMERO-Tiff and store it in OMERO next to the proprierary vendor format.
		#registrationProcess.triggerOMETiffConversion()
