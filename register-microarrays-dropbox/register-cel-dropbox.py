'''

Note:
print statements go to: ~openbis/servers/datastore_server/log/startup_log.txt
'''
import sys
sys.path.append('/home-link/qeana10/bin/')

import checksum
import re
import os
import ch.systemsx.cisd.etlserver.registrator.api.v2
from java.io import File
from org.apache.commons.io import FileUtils
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria

# *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
pattern = re.compile('Q\w{4}[0-9]{3}[a-zA-Z]\w')
mftPattern = re.compile('I[0-9]{2}R[0-9]{3}[a-z][0-9]{2}')

def isExpected(identifier):
        try:
                id = identifier[0:9]
                #also checks for old checksums with lower case letters
                return checksum.checksum(id)==identifier[9]
        except:
                return False

def parseMetadata(file):
	os.system("pdftotext "+file)
        txt = ".".join(file.split(".")[:-1])+".txt"
        info = open(txt)
        orderFlag = False
        rinFlag = False
        numFlag = False
        code = None
        rinMap = {}
        date = re.compile("[A-Z][a-z]{5,9}, [0-9]{1,2}. [A-Z][a-z]{2,8} 2[0-9]{3}")#sorry, people living in the year 3000+
        order = None
	for line in info:
                line = line.strip()
                if orderFlag and line.startswith("I"):
                        auftragFlag = False
                        auftrag = line
                elif len(date.findall(line)) > 0:
                        print line
                elif rinFlag:
                        search = pattern.findall(line)
                        if len(search) > 0:
                                id = search[0]
                                code = id[:10]
                                numFlag = True
                        elif numFlag and line.replace(',','',1).isdigit():
                                numFlag = False
                                rinMap[code] = line.replace(',','.')
                elif "Auftragsnummer" in line:
                        orderFlag = True
                elif "RIN Nummer" in line:
                        rinFlag = True
        info.close()
	return [auftrag, rinMap]

def process(transaction):
        context = transaction.getRegistrationContext().getPersistentMap()

        # Get the incoming path of the transaction
        incomingPath = transaction.getIncoming().getAbsolutePath()

        key = context.get("RETRY_COUNT")
        if (key == None):
                key = 1


        # Get the name of the incoming file
        #name = transaction.getIncoming().getName()

        pdf = None
        newArrayExperiment = None
	filesForID = {}
	maps = None
        for name in os.listdir(incomingPath): #what should the folder be called? how will this work with checksum and origlabfilename etc. created by datahandler?
                searchID = pattern.findall(name)
		if len(searchID) > 0:
			identifier = searchID[0]
			if identifier in filesForID:
				filesForID[identifier] = filesForID[identifier] + [name]
			else:
				filesForID[identifier] = [name]
                if name.lower().endswith(".pdf"):
                        pdf = os.path.join(incomingPath, name)
	        if(pdf):
	                maps = parseMetadata(pdf)
        	        auftrag = maps[0]
                	rins = maps[1]

	for identifier in filesForID:
	        if isExpected(identifier):
	                project = identifier[:5]
        	        parentCode = identifier[:10]
        	else:
                	print "The identifier "+identifier+" did not match the pattern Q[A-Z]{4}\d{3}\w{2} or checksum"
	        search_service = transaction.getSearchService()
        	sc = SearchCriteria()
        	sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, parentCode))
        	foundSamples = search_service.searchForSamples(sc)

	        parentSampleIdentifier = foundSamples[0].getSampleIdentifier()
        	space = foundSamples[0].getSpace()
	        sa = transaction.getSampleForUpdate(parentSampleIdentifier)

        	# register new experiment and sample
	        if not newArrayExperiment:
        	        numberOfExperiments = len(search_service.listExperiments("/" + space + "/" + project)) + 1
                	newArrayExperiment = transaction.createNewExperiment('/' + space + '/' + project + '/' + project + 'E' + str(numberOfExperiments), "Q_MICROARRAY_MEASUREMENT")

	        newArraySample = transaction.createNewSample('/' + space + '/' + 'MA'+ parentCode, "Q_MICROARRAY_RUN")
        	if maps:
	                try:
				newArraySample.setPropertyValue('Q_RNA_INTEGRITY_NUMBER', rins[parentCode])
				newArrayExperiment.setPropertyValue("Q_EXTERNALDB_ID", auftrag)
			except:
				pass
		newArraySample.setPropertyValue("Q_PROPERTIES", sa.getPropertyValue("Q_PROPERTIES"))
        	newArraySample.setParentSampleIdentifiers([sa.getSampleIdentifier()])
        	newArraySample.setExperiment(newArrayExperiment)

	        # create new dataset
		extIDs = mftPattern.findall(filesForID[identifier][0])
        	dataSet = transaction.createNewDataSet("Q_MA_RAW_DATA")
		if extIDs:
			dataSet.setPropertyValue("Q_EXTERNALDB_ID", extIDs[0])
        	dataSet.setMeasuredData(False)
        	dataSet.setSample(newArraySample)

		dataFolder = os.path.realpath(os.path.join(incomingPath,identifier))
		os.mkdir(dataFolder)
		for f in filesForID[identifier]:
			os.rename(os.path.join(incomingPath, f), os.path.join(dataFolder, f))
	        transaction.moveFile(dataFolder, dataSet)

        for f in os.listdir(incomingPath):
                os.remove(os.path.realpath(os.path.join(incomingPath,f)))
