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
import time
import re
import os
import urllib
import ch.systemsx.cisd.etlserver.registrator.api.v2
from java.io import File
from org.apache.commons.io import FileUtils
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria

from life.qbic.utils import ImagingMetadataValidator


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

# For fast validation without parser object
REQUIRED_PROPPERTY_LIST = ["IMAGE_DATA_PATH", "IMAGING_MODALITY", "IMAGED_TISSUE", "INSTRUMENT_MANUFACTURER", "INSTRUMENT_USER", "IMAGING_DATE"]
# To filter property list before pushing key-value pair to OMERO server
PROPPERTY_FILTER_LIST = ["IMAGE_DATA_PATH", "INSTRUMENT_USER", "IMAGING_DATE", "SAMPLE_ID", "OMERO_TAGS", "ETL_TAG"]
# Property value placeholder, to indicate that this property has no valid value in a TSV line (for a datafolder)
PROPPERTY_PLACEHOLDER = "*"

def log_print(msg_string):

	dt_string = time.strftime('%Y-%m-%d %H:%M:%S')
	full_string = "[BIOIMAGE-ETL " + dt_string + "] " + msg_string

	print full_string

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
	
	for incoming_label in sample_property_map:
		if incoming_label in properties:
			key = sample_property_map[incoming_label]
			value = properties[incoming_label]
			img_run.setPropertyValue(key, value)
	return img_run

def getImageDataTargetPathFromLine(line):

	data_target_path = line.split("\t")[0]

	# The string "./" is set to point to the relative root folder
	if data_target_path == "./":
			data_target_path = ""

	return data_target_path

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

	# fast validation without parser object
	for name in REQUIRED_PROPPERTY_LIST:
		if not name in property_names:
			return False

	return True

def getPropertyMap(line, property_names):
	"""Build the property map. Here we could add more complex behaviour later on.
	"""

	properties = {}
	property_values = line.split("\t")

	for i in range(0, len(property_names)): #do not exclude first col (filename), the schema checks for it
		
		# remove trailing newline, and replace space with underscore. This is needed to clean the strings from the TSV.
		# TODO: Try to wrap in method with documentation, to make it more expressive and easier to understand
		name = property_names[i].rstrip('\n').replace(" ", "_")
		value = property_values[i].rstrip('\n').replace(" ", "_")

		# look for placeholder symbol to skip property ("*")
		if value == PROPPERTY_PLACEHOLDER:
			continue

		properties[name] = value

	return properties

def isFloat(value):
	try:
		float(value)
		return True
	except ValueError:
		return False

def isInt(value):
	try:
		int(value)
		return True
	except ValueError:
		return False

def getValidationMap(properties):
	"""Builds a map for property validation.
	Lowercases the keys of the property map, and checks value types.
	"""
	
	new_properties = {}
	for key in properties.keys():
		
		value = properties[key]
		if isInt(value):
			value = int(value)
		elif isFloat(value):
			value = float(value)

		new_properties[key.lower()] = value

	return new_properties

def filterOmeroPropertyMap(property_map, filter_list):
	"""Filters map before ingestion into omero server

	filter_list is a the blacklist, e.g. for what is going to openBIS or is automatically added to omero (e.g. file name)
	"""

	new_props = {}
	for key in property_map.keys():
		if not key in filter_list:
			new_props[key] = property_map[key]

	return new_props

def printPropertyMap(property_map):
	"""Function to display metadata properties.
	"""

	log_print("KEY : VALUE")
	for key in property_map.keys():
		log_print("--> " + str(key) + " : " + str(property_map[key]))


def process(transaction):
	
	log_print("##################################################")
	log_print("Starting ETL transaction")
	"""The main entry point.
	
	openBIS calls this method, when an incoming transaction is registered.
	This happens, when the data has been moved into the openBIS dropbox AND a marker file 
	was created (event trigger).
	"""
	context = transaction.getRegistrationContext().getPersistentMap()

	# Get the incoming path of the transaction
	incomingPath = transaction.getIncoming().getAbsolutePath()
	# Get the name of the incoming folder
	folderName = transaction.getIncoming().getName()

	log_print(incomingPath)

	# 1. Initialize the image registration process
	registrationProcessFactory = irp.ImageRegistrationProcessFactory()
	defaultRegistrationProcess = registrationProcessFactory.createRegistrationProcess(transaction)
	registrationProcess = defaultRegistrationProcess
	#registrationProcess = irp.ImageRegistrationProcess(transaction)
	
	# 2. We want to get the openBIS sample code from the incoming data
	# This tells us to which biological sample the image data was aquired from.
	project_code, sample_code = registrationProcess.fetchOpenBisSampleCode()
	default_project_code = project_code
	default_sample_code = sample_code

	#find specific sample
	tissueSample = registrationProcess.searchOpenBisSample(sample_code)
	default_tissueSample = tissueSample
	space = tissueSample.getSpace()
	default_space = space

	# 3. We now request the associated omero dataset id for the openBIS sample code.
	# Each dataset in OMERO contains the associated openBIS biological sample id, which
	# happened during the experimental design registration with the projectwizard.

	# Starts omero registration
	# returns -1 if fetching dataset-id operation failed

	default_omero_dataset_id = registrationProcess.requestOmeroDatasetId(project_code=project_code, sample_code=sample_code)
	omero_dataset_id = default_omero_dataset_id

	log_print("Default OMERO dataset name: " + str(sample_code))
	log_print("Default OMERO dataset id: " + str(default_omero_dataset_id))

	omero_failed = int(omero_dataset_id) < 0
	if omero_failed:
		raise ValueError("Omero did not return expected dataset id.")

	# Find and parse metadata file content
	metadataFile = findMetaDataFile(incomingPath)

	# log_print("metadataFile: " + str(metadataFile))

	property_names = getPropertyNames(metadataFile)

	log_print("property names:")
	log_print(str(property_names))

	valid_names = validatePropertyNames(property_names)
	if not valid_names:
		raise ValueError("Invalid Property Names.")

	#keep track of number of images for openBIS ID
	dataset_number = 0
	#Initialize openBIS imaging experiment
	imagingExperiment = None
	previousProps = {}
	existing_experiment_ids = []

	log_print("Starting metadata table iterations")
	# Iterate over the metadata entries containing all pre-specified imaging metadata
	for line in metadataFile[1:]:  # (Exclude header)

		log_print("++++++++++++++++++++++++++++++++++++++++++++++++++")
		log_print("Metadata table iteration: " + str(dataset_number))

		# Get modality and other metadata from tsv.
		# Additional metadata is provided in an own metadata TSV file. 
		# We extract the metadata from this file.
		properties = {}
		properties = getPropertyMap(line, property_names)

		# Look for ETL_TAG in line/property map
		if "ETL_TAG" in properties.keys():
			log_print("Using ETL_TAG: " + properties["ETL_TAG"])
			registrationProcess = registrationProcessFactory.createRegistrationProcess(transaction, etl_tag=properties["ETL_TAG"])
		else:
			registrationProcess = defaultRegistrationProcess
		

		# Retrieve the path to image data target, an image file or a data folder
		# Please NO whitespace (or special) characters in path, or filename!
		# The string "./" is set to point to the relative root folder
		relativeTargetPath = getImageDataTargetPathFromLine(line)

		log_print("Relative data target path: " + relativeTargetPath)
		# Due to the datahandler we need to add another subfolder of the same name to the path
		basePath = os.path.join(incomingPath, folderName)
		log_print("Base path: " + basePath)
		targetPath = os.path.join(basePath, relativeTargetPath)
		log_print("Incoming target path for OMERO import: " + targetPath)

		# Look for overriding SAMPLE_ID in line/property map
		if "SAMPLE_ID" in properties.keys():
			if len(properties["SAMPLE_ID"]) == 10:
				line_project_code = properties["SAMPLE_ID"][:5]
				line_sample_code = properties["SAMPLE_ID"]
				project_code = line_project_code
				sample_code = line_sample_code
				# find specific sample
				tissueSample = registrationProcess.searchOpenBisSample(sample_code)
				space = tissueSample.getSpace()
				# request OMERO dataset ID
				omero_dataset_id = registrationProcess.requestOmeroDatasetId(project_code=project_code, sample_code=sample_code)
				log_print("Overriding SAMPLE_ID ...")
			else:
				project_code = default_project_code
				sample_code = default_sample_code
				tissueSample = default_tissueSample
				space = default_space
				omero_dataset_id = default_omero_dataset_id
		else:
			project_code = default_project_code
			sample_code = default_sample_code
			tissueSample = default_tissueSample
			space = default_space
			omero_dataset_id = default_omero_dataset_id
		log_print("Iteration OMERO dataset name: " + sample_code)
		log_print("Iteration OMERO dataset id: " + str(omero_dataset_id))

		# 4. After we have received the omero dataset id, we know where to import the images in OMERO.
		# We pass the omero dataset id and trigger the image registration process in OMERO.
		# We need to find out if the target path is for an image file or a folder containing images
		omero_image_ids = []
		if os.path.isfile(targetPath):
			log_print("Found path file target ...")
			omero_image_ids = registrationProcess.registerImageFileInOmero(targetPath, omero_dataset_id)
		elif os.path.isdir(targetPath):
			log_print("Found path folder target ...")
			omero_image_ids = registrationProcess.registerImageFolder(targetPath, omero_dataset_id)
		log_print("Created OMERO image identifiers: " + str(omero_image_ids))

		omero_failed = len(omero_image_ids) < 1
		if omero_failed:
			raise ValueError("Omero did not return expected image ids.")

		# 5.1 Validate metadata for image file
		# Temporaly disabled, pending REMBI alignment and ETL logic modifications
		# ImagingMetadataValidator.validateImagingProperties(getValidationMap(properties))
		
		# Annotate with metadata, using OMERO key-value pairs and tags 
		# one file can have many images, iterate over all img ids
		for img_id in omero_image_ids:
			registrationProcess.registerOmeroKeyValuePairs(img_id, filterOmeroPropertyMap(properties, PROPPERTY_FILTER_LIST))

		if "OMERO_TAGS" in properties.keys():
			tag_list = properties["OMERO_TAGS"].split(",")
			for tag_value in tag_list:
				for img_id in omero_image_ids:
					registrationProcess.tagOmeroImage(img_id, tag_value)

		####
		# 6. In addition to the image registration and technical metadata storage, we want to add
		# further experimental metadata in openBIS. This metadata contains information about the 
		# imaging experiment itself, such as modality, imaged tissue and more. 
		# We also want to connect this data with the previously created, corresponding OMERO image id t
		# hat represents the result of this experiment in OMERO. 

		# we decide if new experiment is needed based on some pre-defined criteria.
		# Normally, the most important criterium is collision of experiment type properties
		# between samples. E.g. two different imaging modalities need two experiments.
		
		fileBelongsToExistingExperiment = isSameExperimentMetadata(previousProps, properties)
		previousProps = properties
		if(not fileBelongsToExistingExperiment):
			imagingExperiment = createNewImagingExperiment(transaction, space, project_code, properties, existing_experiment_ids)
		imagingSample = createNewImagingRun(transaction, tissueSample, imagingExperiment, omero_image_ids, dataset_number, properties)
		# increment id offset for next sample in this loop
		dataset_number += 1

	log_print("Successfully finished ETL process")
