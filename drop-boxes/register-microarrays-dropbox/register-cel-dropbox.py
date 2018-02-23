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
FORMAT_TO_DATASET_TYPE = {'.cel':'Q_MA_RAW_DATA', '.txt':'Q_MA_AGILENT_DATA'}
ARCHIVE_FORMATS = ['.gz']
expType = "Q_MICROARRAY_MEASUREMENT"

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

        pdf = None
        arrayExperiment = None
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

        space = None
        project = None
        parents = []
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
                parents.append(transaction.getSampleForUpdate(parentSampleIdentifier))

        try:
                experiments = search_service.listExperiments("/" + space + "/" + project)
        except:
                print "space or project could not be found, because there was no known registered barcode in any file name of the input data"
        experimentIDs = []
        for exp in experiments:
                experimentIDs.append(exp.getExperimentIdentifier())
                if exp.getExperimentType() == expType:
                        arrayExperiment = exp
        # no existing experiment for samples of this sample preparation found
        if not arrayExperiment:
                expID = experimentIDs[0]
                i = 0
                while expID in experimentIDs:
                        i += 1
                        expNum = len(experiments) + i
                        expID = '/' + space + '/' + project + '/' + project + 'E' + str(expNum)
                arrayExperiment = transaction.createNewExperiment(expID, expType)
        # now that we have an experiment we go back to the samples + data
        j = -1
        for identifier in filesForID:
                j += 1
                sa = parents[j]
                parentCode = sa.getCode()

                arraySampleID = '/' + space + '/' + 'MA'+ parentCode

                arraySample = transaction.getSampleForUpdate(arraySampleID)

                if not arraySample:
                        arraySample = transaction.createNewSample(arraySampleID, "Q_MICROARRAY_RUN")
                        if maps:
                                try:
                                        arraySample.setPropertyValue('Q_RNA_INTEGRITY_NUMBER', rins[parentCode])
                                        arrayExperiment.setPropertyValue("Q_EXTERNALDB_ID", auftrag)
                                except:
                                        pass
                        arraySample.setPropertyValue("Q_PROPERTIES", sa.getPropertyValue("Q_PROPERTIES"))
                        arraySample.setParentSampleIdentifiers([sa.getSampleIdentifier()])
                        arraySample.setExperiment(arrayExperiment)

                # create new dataset
                firstFile = filesForID[identifier][0]
                extIDs = mftPattern.findall(firstFile)

                stem, ext = os.path.splitext(firstFile)
                if ext in ARCHIVE_FORMATS:
                        stem, ext = os.path.splitext(stem)
                dataSetType = FORMAT_TO_DATASET_TYPE[ext.lower()]
                dataSet = transaction.createNewDataSet(dataSetType)
                if extIDs:
                        dataSet.setPropertyValue("Q_EXTERNALDB_ID", extIDs[0])
                dataSet.setMeasuredData(False)
                dataSet.setSample(arraySample)

                dataFolder = os.path.realpath(os.path.join(incomingPath,identifier))
                os.mkdir(dataFolder)
                for f in filesForID[identifier]:
                        os.rename(os.path.join(incomingPath, f), os.path.join(dataFolder, f))
                transaction.moveFile(dataFolder, dataSet)

        for f in os.listdir(incomingPath):
                os.remove(os.path.realpath(os.path.join(incomingPath,f)))
