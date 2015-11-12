'''

Note:
print statements go to: ~openbis/servers/datastore_server/log/startup_log.txt
'''
import sys
sys.path.append('/home-link/qeana10/bin/')
sys.path.append('/home-link/qeana10/bin/simplejson-3.8.0/')

import re
import os
import checksum
import time
import datetime
import shutil
import subprocess
import simplejson as json

import ch.systemsx.cisd.etlserver.registrator.api.v2
from java.io import File
from org.apache.commons.io import FileUtils
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria
# Data import and registration
# *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
pattern = re.compile('Q\w{4}[0-9]{3}[a-zA-Z]\w')
typesDict = {'dna_seq': 'DNA'}

def parse_metadata_file(filePath):
	jsonFile = open(filePath, 'r')
	data = json.load(jsonFile)
	jsonFile.close()

	return data

def isExpected(identifier):
        #try:
        id = identifier[0:9]
        #also checks for old checksums with lower case letters
	print "id: "+identifier
	print "id without checksum: "+id
	print "checksum: "+checksum.checksum(id)
        return checksum.checksum(id)==identifier[9]
        #except:
        #       return False

def find_and_register_vcf(transaction, jsonContent):
	qbicBarcodes = []
	geneticIDS = []
	sampleSource = []
	for key in jsonContent.keys():
		if key == "type" or key == "files":
			pass
		else:
			geneticIDS.append(jsonContent[key]["id_genetics"])
			qbicBarcodes.append(jsonContent[key]["id_qbic"])
			sampleSource.append(jsonContent[key]["tumor"])
			
			
        expType = jsonContent["type"]

        project = qbicBarcodes[0][:5]

	search_service = transaction.getSearchService()
        sc = SearchCriteria()
        pc = SearchCriteria()
        pc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.PROJECT, project));
        sc.addSubCriteria(SearchSubCriteria.createExperimentCriteria(pc))

	foundSamples = search_service.searchForSamples(sc)

	datasetSample = None
	sampleFound = False

	parentIdentifiers = []
        testParentIdentifiers = []
	
	for barcode, geneticID in zip(qbicBarcodes, geneticIDS):
        	for samp in foundSamples:
                	space = samp.getSpace()
			qbicBarcodeID = '/' + space + '/' + barcode
			print qbicBarcodeID
			print geneticID
                	if qbicBarcodeID in samp.getParentSampleIdentifiers():
                        	testParentID = samp.getSampleIdentifier()
				for s in foundSamples:
					sampleType = s.getSampleType()
					print sampleType
					print testParentID
					print s.getParentSampleIdentifiers()
					print s.getPropertyValue("Q_SECONDARY_NAME")
					print geneticID
					if (testParentID in s.getParentSampleIdentifiers()) and (sampleType == "Q_NGS_SINGLE_SAMPLE_RUN") and (s.getPropertyValue("Q_SECONDARY_NAME") in geneticID):
						sampleIdent = s.getSampleIdentifier()
						parentIdentifiers.append(sampleIdent)
						testParentIdentifiers.append(testParentID)

	numberOfExperiments = len(search_service.listExperiments("/" + space + "/" + project)) + 1
	newVCExp = transaction.createNewExperiment('/' + space + '/' + project + '/' + project + 'E' + str(numberOfExperiments), "Q_NGS_VARIANT_CALLING")
	newVCExp.setPropertyValue('Q_CURRENT_STATUS', 'FINISHED')

	identString = ''
	for genID in geneticIDS:
		identString += genID.split('_')[-1]

	identString2 = ''
	for tpi in testParentIdentifiers:
		identString2 += tpi.split('/')[-1]
	
	#newVCSample = transaction.createNewSample('/' + space + '/' + 'VC'+ project + qbicBarcodes[0][5:] + qbicBarcodes[1][5:] + identString, "Q_NGS_VARIANT_CALLING")
	newVCSample = transaction.createNewSample('/' + space + '/' + 'VC'+ identString2  + identString, "Q_NGS_VARIANT_CALLING")
	newVCSample.setParentSampleIdentifiers(parentIdentifiers)
	newVCSample.setExperiment(newVCExp)

	additionalInfo = '%s %s Tumor: %s \n %s %s Tumor: %s' % (qbicBarcodes[0], geneticIDS[0], sampleSource[0], qbicBarcodes[1], geneticIDS[1], sampleSource[1]) 

	newVCSample.setPropertyValue('Q_ADDITIONAL_INFO', additionalInfo)
	secName = '%s-%s' % (geneticIDS[0], geneticIDS[1])

	newVCSample.setPropertyValue('Q_SECONDARY_NAME', secName)

	datasetSample = newVCSample
	return datasetSample

	
def find_and_register_ngs(transaction, jsonContent):
	qcValues = jsonContent["sample1"]["qc"]
	genome = jsonContent["sample1"]["genome"]
	idGenetics = jsonContent["sample1"]["id_genetics"]
	qbicBarcode = jsonContent["sample1"]["id_qbic"]
	system = jsonContent["sample1"]["processing_system"]
	tumor = jsonContent["sample1"]["tumor"]
	expType = jsonContent["type"]

    	project = qbicBarcode[:5]

	search_service = transaction.getSearchService()
    	sc = SearchCriteria()
    	pc = SearchCriteria()
    	pc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.PROJECT, project));
    	sc.addSubCriteria(SearchSubCriteria.createExperimentCriteria(pc))
	foundSamples = search_service.searchForSamples(sc)

    	datasetSample = None
    	sampleFound = False
	
    	for samp in foundSamples:
		qbicBarcodeID = '/' + samp.getSpace() + '/' + qbicBarcode
		if qbicBarcodeID in samp.getParentSampleIdentifiers():
			sampleType = samp.getSampleType()
	    		if sampleType == "Q_TEST_SAMPLE":
				if (samp.getPropertyValue("Q_SAMPLE_TYPE") == typesDict[expType]) and (samp.getPropertyValue("Q_SECONDARY_NAME") == idGenetics.split('_')[0]):
	    				sampleIdent = samp.getSampleIdentifier()
	    				for s in foundSamples:
	    				# There is already a registered NGS run
	    					if (s.getSampleType() == "Q_NGS_SINGLE_SAMPLE_RUN") and (sampleIdent in s.getParentSampleIdentifiers() and (s.getPropertyValue("Q_SECONDARY_NAME") in idGenetics)):
							sa = transaction.getSampleForUpdate(s.getSampleIdentifier())
	    						sa.setPropertyValue("Q_SECONDARY_NAME", idGenetics)
					
	    						datasetSample = sa
	    						sampleFound = True

	    				if not sampleFound:
					        	# register new experiment and sample
							space = samp.getSpace()
						        experiments = search_service.listExperiments("/" + space + "/" + project)
							numberOfExperiments = len(search_service.listExperiments("/" + space + "/" + project))
							numberOfExperiments = len(search_service.listExperiments("/" + space + "/" + project)) + 1
							newNGSMeasurementExp = transaction.createNewExperiment('/' + space + '/' + project + '/' + project + 'E' + str(numberOfExperiments), "Q_NGS_MEASUREMENT")
					        	newNGSMeasurementExp.setPropertyValue('Q_CURRENT_STATUS', 'FINISHED')
					        	newNGSMeasurementExp.setPropertyValue('Q_SEQUENCING_MODE', 'PAIRED_END')
					        	newNGSMeasurementExp.setPropertyValue('Q_SEQUENCER_DEVICE', 'IMGAG_ILLUMINA_HISEQ_2500')
					        	newNGSMeasurementExp.setPropertyValue('Q_ADDITIONAL_INFO', system)
					        	newNGSMeasurementExp.setPropertyValue('Q_SEQUENCING_TYPE', typesDict[expType])

					        	newNGSrunSample = transaction.createNewSample('/' + space + '/' + 'NGS'+ idGenetics.split('_')[-1] + samp.getCode(), "Q_NGS_SINGLE_SAMPLE_RUN")
					        	newNGSrunSample.setParentSampleIdentifiers([sampleIdent])
							newNGSrunSample.setExperiment(newNGSMeasurementExp)

					        	additionalInfo = '%s: %s\n' % ("Genome", genome)

					        	for qc in qcValues:
								line = str(qc)
					        		additionalInfo += '%s\n' % line.replace('{', '').replace('}', '')

					        	newNGSrunSample.setPropertyValue('Q_ADDITIONAL_INFO', additionalInfo)
					        	newNGSrunSample.setPropertyValue('Q_SECONDARY_NAME', idGenetics)

					        	datasetSample = newNGSrunSample

	return datasetSample

def process(transaction):
        context = transaction.getRegistrationContext().getPersistentMap()

        # Get the incoming path of the transaction
        incomingPath = transaction.getIncoming().getAbsolutePath()

        key = context.get("RETRY_COUNT")
        if (key == None):
                key = 1

        # Get the name of the incoming folder
        name = transaction.getIncoming().getName()

        identifier = pattern.findall(name)[0]
	print '_ ' + identifier
        if isExpected(identifier):
		pass
                #experiment = identifier[1:5]
                #project = identifier[:5]
                #parentCode = identifier[:10]
        else:
                print "The identifier "+identifier+" did not match the pattern Q[A-Z]{4}\d{3}\w{2} or checksum"

        os.remove(os.path.realpath(os.path.join(incomingPath,'source_dropbox.txt')))
	
	dataSet = None
        for f in os.listdir(os.path.join(incomingPath,name)):
        		if f.endswith('metadata'):
				jsonContent = parse_metadata_file(os.path.realpath(os.path.join(os.path.join(incomingPath, name),f)))
				rawFiles = jsonContent["files"]
				if rawFiles[0].endswith("vcf"):
					datasetSample = find_and_register_vcf(transaction, jsonContent)
					
					dataSet = transaction.createNewDataSet("Q_NGS_VARIANT_CALLING_DATA")
					dataSet.setSample(datasetSample)

				elif rawFiles[0].endswith("fastq") or rawFiles[0].endswith("fastq.gz"):
					datasetSample = find_and_register_ngs(transaction, jsonContent)

					dataSet = transaction.createNewDataSet("Q_NGS_RAW_DATA")
        				dataSet.setSample(datasetSample)

        			os.remove(os.path.realpath(os.path.join(os.path.join(incomingPath,name),f)))
			else:
				pass
	print dataSet
	transaction.moveFile(os.path.join(incomingPath, name), dataSet)
