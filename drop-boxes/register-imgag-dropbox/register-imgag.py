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
import string

import ch.systemsx.cisd.etlserver.registrator.api.v2
from java.io import File
from org.apache.commons.io import FileUtils
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria

from shutil import copyfile
# Data import and registration
# *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
pattern = re.compile('Q\w{4}[0-9]{3}[a-zA-Z]\w')
typesDict = {'dna_seq': 'DNA', 'rna_seq': 'RNA', 'dna_seq_somatic': 'DNA'}

def parse_metadata_file(filePath):
    jsonFile = open(filePath, 'r')
    data = json.load(jsonFile)
    jsonFile.close()

    return data

def isExpected(identifier):
    #try:
    id = identifier[0:9]
    #also checks for old checksums with lower case letters
    return checksum.checksum(id)==identifier[9]
    #except:
    #	   return False

def getNextFreeBarcode(projectcode, numberOfBarcodes):
    letters = string.ascii_uppercase
    numberOfBarcodes += 1

    currentLetter = letters[numberOfBarcodes / 999]
    currentNumber = numberOfBarcodes % 999
    code = projectcode + str(currentNumber).zfill(3) + currentLetter 
    return code + checksum.checksum(code)

numberOfExperiments = 0
newTestSamples = {}
oldTestSamples = {}
newNGSSamples = {}

def find_and_register_vcf(transaction, jsonContent, varcode):#varcode example: GS130715_03-GS130717_03 (verified in startup.log)
    qbicBarcodes = []
    geneticIDS = []
    sampleSource = []

    varcodekey = ''

    for key in jsonContent.keys():
        if key == "type" or key == "files":
            pass
        else:#keys: "sample1" and "sample2"
            geneticIDS.append(jsonContent[key]["id_genetics"])#GS130715_03 and GS130717_03
            qbicBarcodes.append(jsonContent[key]["id_qbic"])
            sampleSource.append(jsonContent[key]["tumor"])
            if jsonContent[key]["id_genetics"] == varcode:
                varcodekey = key


    expType = jsonContent["type"]
    project = qbicBarcodes[0][:5]
    search_service = transaction.getSearchService()
    sc = SearchCriteria()
    pc = SearchCriteria()
    pc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.PROJECT, project));
    sc.addSubCriteria(SearchSubCriteria.createExperimentCriteria(pc))

    foundSamples = search_service.searchForSamples(sc)
    space = foundSamples[0].getSpace()

    datasetSample = None
    sampleFound = False

    parentIdentifiers = []
    testParentIdentifiers = []


    global numberOfExperiments
    additionalInfo = ''
    secName = ''

    if len(geneticIDS) >= 2:
        somaticIdent = '%s-%s' % (geneticIDS[0], geneticIDS[1]) # if there is more than one sample we have to concatenate the identifiers
        secName = somaticIdent
        if somaticIdent == varcode:
            for i, parentBarcode in enumerate(qbicBarcodes):
                additionalInfo += '%s %s Tumor: %s \n' % (qbicBarcodes[i], geneticIDS[i], sampleSource[i])
            for barcode, geneticID in zip(qbicBarcodes, geneticIDS):
                genShortID = geneticID.split('_')[0]
                if geneticID in newNGSSamples:
                    parentIdentifiers.append(newNGSSamples[geneticID])
                    testParentIdentifiers.append(oldTestSamples[geneticID])
                else:
                    for samp in foundSamples:
                        #some short variables to clean up the long if case
                        code = samp.getCode()
                        sType = samp.getSampleType()
                        qbicBarcodeID = '/' + space + '/' + barcode # qbic identifier from the metadata that came in (probably tissue sample)
                        parentIDs = samp.getParentSampleIdentifiers()
                        analyte = samp.getPropertyValue("Q_SAMPLE_TYPE")
                        curSecName = samp.getPropertyValue("Q_SECONDARY_NAME")
                        extID = samp.getPropertyValue("Q_EXTERNALDB_ID")
                        # we are looking for either the test sample with this barcode OR a test sample with parent with this barcode, the right analyte (e.g. DNA) and the short genetics ID in secondary name or external ID
                        print code
                        print qbicBarcodeID in parentIDs
                        print analyte == typesDict[expType]
                        print (curSecName != None) and (genShortID in curSecName)
                        print (extID != None) and (genShortID in extID)
                        if ((barcode == code) and (sType == "Q_TEST_SAMPLE")) or ((qbicBarcodeID in parentIDs) and (analyte == typesDict[expType]) and (((curSecName != None) and (genShortID in curSecName)) or ((extID != None) and (genShortID in extID)))):
                            testParentID = samp.getSampleIdentifier()
                            # this time we are looking for the NGS Single Sample run attached to the test sample we just found
                            for s in foundSamples:
                                sampleType = s.getSampleType()
                                curSecName = s.getPropertyValue("Q_SECONDARY_NAME")
                                extDB = s.getPropertyValue("Q_EXTERNALDB_ID")
                                if (testParentID in s.getParentSampleIdentifiers()) and (sampleType == "Q_NGS_SINGLE_SAMPLE_RUN") and (((curSecName != None) and (geneticID in curSecName)) or ((extDB != None) and (geneticID in extDB))):
                                    sampleIdent = s.getSampleIdentifier()
                                    parentIdentifiers.append(sampleIdent)
                                    testParentIdentifiers.append(testParentID) # if we found the right one, we append it for later, as every related test sample is needed for registration
    else:
        geneticID = varcode
        genShortID = geneticID.split('_')[0]
        barcode = jsonContent[varcodekey]["id_qbic"]
        additionalInfo = '%s %s Tumor: %s \n' % (barcode, geneticID, jsonContent[varcodekey]["tumor"])
        secName += '%s ' % geneticID
        if geneticID in newNGSSamples:
            parentIdentifiers.append(newNGSSamples[geneticID])
            testParentIdentifiers.append(oldTestSamples[geneticID])
        else:
            for samp in foundSamples:
                #some short variables to clean up the long if case
                code = samp.getCode()
                sType = samp.getSampleType()
                qbicBarcodeID = '/' + space + '/' + barcode # qbic identifier from the metadata that came in (probably tissue sample)
                parentIDs = samp.getParentSampleIdentifiers()
                analyte = samp.getPropertyValue("Q_SAMPLE_TYPE")
                curSecName = samp.getPropertyValue("Q_SECONDARY_NAME")
                extID = samp.getPropertyValue("Q_EXTERNALDB_ID")
                # we are looking for either the test sample with this barcode OR a test sample with parent with this barcode, the right analyte (e.g. DNA) and the short genetics ID in secondary name or external ID
                if ((barcode == code) and (sType == "Q_TEST_SAMPLE")) or ((qbicBarcodeID in parentIDs) and (analyte == typesDict[expType]) and ((genShortID in curSecName) or (genShortID in extID))):
                    testParentID = samp.getSampleIdentifier()
                    for s in foundSamples:
                        sampleType = s.getSampleType()
                        curSecName = s.getPropertyValue("Q_SECONDARY_NAME")
                        extDB = s.getPropertyValue("Q_EXTERNALDB_ID")
                        if (testParentID in s.getParentSampleIdentifiers()) and (sampleType == "Q_NGS_SINGLE_SAMPLE_RUN") and (((curSecName != None) and (geneticID in curSecName)) or ((extDB != None) and (geneticID in extDB))):
                            sampleIdent = s.getSampleIdentifier()
                            parentIdentifiers.append(sampleIdent)
                            testParentIdentifiers.append(testParentID)
    numberOfExperiments += 1
    existingExperimentIDs = []
    existingExperiments = search_service.listExperiments("/" + space + "/" + project)
    
    for eexp in existingExperiments:
        existingExperimentIDs.append(eexp.getExperimentIdentifier())

    newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(numberOfExperiments)

    while newExpID in existingExperimentIDs:
        numberOfExperiments += 1 
        newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(numberOfExperiments)
        
    newVCExp = transaction.createNewExperiment(newExpID, "Q_NGS_VARIANT_CALLING")
    identString = varcode # not used atm
    #for genID in geneticIDS:
    #	identString += genID.split('_')[-1]

    identString2 = ''
    for tpi in testParentIdentifiers:
        identString2 += '_'+tpi.split('/')[-1]

    #newVCSample = transaction.createNewSample('/' + space + '/' + 'VC'+ project + qbicBarcodes[0][5:] + qbicBarcodes[1][5:] + identString, "Q_NGS_VARIANT_CALLING")
    newVCSample = transaction.createNewSample('/' + space + '/' + 'VC'+ identString2, "Q_NGS_VARIANT_CALLING")
    newVCSample.setParentSampleIdentifiers(parentIdentifiers)
    newVCSample.setExperiment(newVCExp)

    #additionalInfo = ""
    #secName = ""
    #for i, parentBarcode in enumerate(qbicBarcodes):
#		additionalInfo += '%s %s Tumor: %s \n' % (qbicBarcodes[i], geneticIDS[i], sampleSource[i])
#		secName += '%s ' % (geneticIDS[i])

    secName = secName.strip()
    #additionalInfo = '%s %s Tumor: %s \n %s %s Tumor: %s' % (qbicBarcodes[0], geneticIDS[0], sampleSource[0], qbicBarcodes[1], geneticIDS[1], sampleSource[1])

    newVCSample.setPropertyValue('Q_ADDITIONAL_INFO', additionalInfo)
    #secName = '%s-%s' % (geneticIDS[0], geneticIDS[1])
    newVCSample.setPropertyValue('Q_SECONDARY_NAME', secName)

    datasetSample = newVCSample
    return datasetSample

def createNewBarcode(project, tr):
    search_service = tr.getSearchService()
    sc = SearchCriteria()
    pc = SearchCriteria()
    pc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.PROJECT, project));
    sc.addSubCriteria(SearchSubCriteria.createExperimentCriteria(pc))
    foundSamples = search_service.searchForSamples(sc)

    foundSamplesFilter = [s for s in foundSamples if 'ENTITY' not in s.getCode()]

    offset = 0
    exists = True
    while exists:
        # create new barcode
        newBarcode = getNextFreeBarcode(project, len(foundSamplesFilter) + len(newTestSamples) + offset)

        # check if barcode already exists in database
        pc = SearchCriteria()
        pc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, newBarcode))
        found = search_service.searchForSamples(pc)
        if len(found) == 0:
            exists = False
        else:
            offset += 1

    return newBarcode

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
    pc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.PROJECT, project))
    sc.addSubCriteria(SearchSubCriteria.createExperimentCriteria(pc))
    foundSamples = search_service.searchForSamples(sc)

    datasetSample = None
    sampleFound = False
    sampleIdent = None
    space = foundSamples[0].getSpace()
    testSampleCode = None

    knownCodes = []

    for samp in foundSamples:
        qbicBarcodeID = '/' + samp.getSpace() + '/' + qbicBarcode
        knownCodes.append(samp.getCode())
        #if qbicBarcodeID in samp.getParentSampleIdentifiers() or qbicBarcode == samp.getCode():
        sampleType = samp.getSampleType()

        code = samp.getCode()
        parentIDs = samp.getParentSampleIdentifiers()
        analyte = samp.getPropertyValue("Q_SAMPLE_TYPE")
        curSecName = samp.getPropertyValue("Q_SECONDARY_NAME")
        extID = samp.getPropertyValue("Q_EXTERNALDB_ID")

        # we are looking for either the test sample with this barcode
        isTestSampleWithBarcode = (barcode == code) and (sType != None) and (sType == "Q_TEST_SAMPLE")
        # OR a test sample with parent with this barcode
        correctParent = qbicBarcodeID in parentIDs
        # AND the right analyte (e.g. DNA)
        correctAnalyte = (analyte != None) and (analyte == typesDict[expType])
        # AND and the short genetics ID in secondary name OR external ID
        hasGeneticsID = (curSecName != None and genShortID in curSecName) or (extID != None and genShortID in extID)
        if isTestSampleWithBarcode or (correctParent and correctAnalyte and hasGeneticsID):
            sampleIdent = samp.getSampleIdentifier()
            testSampleCode = samp.getCode()
            oldTestSamples[idGenetics] = sampleIdent

    if not sampleIdent:
        if not idGenetics in newTestSamples:
            for samp in foundSamples:
                if qbicBarcode == samp.getCode():
                    testSampleCode = createNewBarcode(project, transaction)

                    sampleIdent = '/' + space + '/' + testSampleCode
                    testSample = transaction.createNewSample(sampleIdent, "Q_TEST_SAMPLE")
                    testSample.setParentSampleIdentifiers([samp.getSampleIdentifier()])
                    testSample.setPropertyValue('Q_SECONDARY_NAME', idGenetics.split('_')[0])
                    testSample.setPropertyValue('Q_SAMPLE_TYPE', typesDict[expType])
                    global numberOfExperiments
                    
                    numberOfExperiments += 1
                    existingExperimentIDs = []
                    existingExperiments = search_service.listExperiments("/" + space + "/" + project)
                    
                    for eexp in existingExperiments:
                        existingExperimentIDs.append(eexp.getExperimentIdentifier())

                    newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(numberOfExperiments)

                    while newExpID in existingExperimentIDs:
                        numberOfExperiments += 1 
                        newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(numberOfExperiments)

                    newTestSampleExperiment = transaction.createNewExperiment(newExpID, "Q_SAMPLE_PREPARATION")
                    testSample.setExperiment(newTestSampleExperiment)
                    newTestSamples[idGenetics] = sampleIdent

    for s in foundSamples:
        # There is already a registered NGS run
        if ((s.getSampleType() == "Q_NGS_SINGLE_SAMPLE_RUN") and (sampleIdent in s.getParentSampleIdentifiers()) and (s.getPropertyValue("Q_SECONDARY_NAME") == idGenetics)):
            sa = transaction.getSampleForUpdate(s.getSampleIdentifier())
            sa.setPropertyValue("Q_SECONDARY_NAME", idGenetics)

            datasetSample = sa
            sampleFound = True

    if not sampleFound:
        # register new experiment and sample
        numberOfExperiments += 1
        existingExperimentIDs = []
        existingExperiments = search_service.listExperiments("/" + space + "/" + project)
        
        for eexp in existingExperiments:
            existingExperimentIDs.append(eexp.getExperimentIdentifier())

        newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(numberOfExperiments)

        while newExpID in existingExperimentIDs:
            numberOfExperiments += 1 
            newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(numberOfExperiments)

        newNGSMeasurementExp = transaction.createNewExperiment(newExpID, "Q_NGS_MEASUREMENT")
        newNGSMeasurementExp.setPropertyValue('Q_CURRENT_STATUS', 'FINISHED')
        newNGSMeasurementExp.setPropertyValue('Q_SEQUENCING_MODE', 'PAIRED_END')
        newNGSMeasurementExp.setPropertyValue('Q_SEQUENCER_DEVICE', 'IMGAG_ILLUMINA_HISEQ_2500')
        newNGSMeasurementExp.setPropertyValue('Q_ADDITIONAL_INFO', system)
        newNGSMeasurementExp.setPropertyValue('Q_SEQUENCING_TYPE', typesDict[expType])
        newNGSID = '/' + space + '/' + 'NGS'+ idGenetics.split('_')[-1] + testSampleCode

        freeID = idGenetics.split('_')[-1]
        existingSampleIDs = []
        
        for s in foundSamples:
            existingSampleIDs.append(s.getSampleIdentifier())

        while newNGSID in existingSampleIDs:
            freeID = str(int(freeID) + 1).zfill(len(freeID))
            newNGSID = '/' + space + '/' + 'NGS'+ freeID + testSampleCode

        existingSampleIDs.append(newNGSID)
        newNGSrunSample = transaction.createNewSample(newNGSID, "Q_NGS_SINGLE_SAMPLE_RUN")
        newNGSrunSample.setParentSampleIdentifiers([sampleIdent])
        newNGSrunSample.setExperiment(newNGSMeasurementExp)

        newNGSSamples[idGenetics] = newNGSID

        additionalInfo = '%s: %s\n' % ("Genome", genome)

        for qc in qcValues:
            line = str(qc)
            additionalInfo += '%s\n' % line.replace('{', '').replace('}', '')

        newNGSrunSample.setPropertyValue('Q_ADDITIONAL_INFO', additionalInfo)
        newNGSrunSample.setPropertyValue('Q_SECONDARY_NAME', idGenetics)

        datasetSample = newNGSrunSample
    return datasetSample

def find_and_register_ngs_without_metadata(transaction):
    context = transaction.getRegistrationContext().getPersistentMap()

    # Get the incoming path of the transaction
    incomingPath = transaction.getIncoming().getAbsolutePath()

    key = context.get("RETRY_COUNT")
    if (key == None):
            key = 1
    # Get the name of the incoming file
    name = transaction.getIncoming().getName()
    identifier = pattern.findall(name)[0]
    if isExpected(identifier):
            project = identifier[:5]
    else:
        print "The identifier "+identifier+" did not match the pattern Q[A-Z]{4}\d{3}\w{2} or checksum"
    search_service = transaction.getSearchService()
    sc = SearchCriteria()
    sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, identifier))
    foundSamples = search_service.searchForSamples(sc)

    sampleIdentifier = foundSamples[0].getSampleIdentifier()
    space = foundSamples[0].getSpace()
    sa = transaction.getSampleForUpdate(sampleIdentifier)

    sampleType = "Q_NGS_SINGLE_SAMPLE_RUN"
    if sa.getSampleType() != sampleType:
        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, "NGS"+identifier))
        foundSamples = search_service.searchForSamples(sc)
        if len(foundSamples) > 0:
            sampleIdentifier = foundSamples[0].getSampleIdentifier()
            sa = transaction.getSampleForUpdate(sampleIdentifier)
        else:
            # create NGS-specific experiment/sample and
            # attach to the test sample
            expType = "Q_NGS_MEASUREMENT"
            ngsExperiment = None
            experiments = search_service.listExperiments("/" + space + "/" + project)
            experimentIDs = []
            for exp in experiments:
                experimentIDs.append(exp.getExperimentIdentifier())
            expID = experimentIDs[0]
            i = 0
            while expID in experimentIDs:
                i += 1
                expNum = len(experiments) + i
                expID = '/' + space + '/' + project + \
                    '/' + project + 'E' + str(expNum)
            ngsExperiment = transaction.createNewExperiment(expID, expType)
            ngsExperiment.setPropertyValue('Q_SEQUENCER_DEVICE',"UNSPECIFIED_ILLUMINA_HISEQ_2500") #change this
            newID = 'NGS'+identifier
            ngsSample = transaction.createNewSample('/' + space + '/' + newID, sampleType)
            ngsSample.setParentSampleIdentifiers([sa.getSampleIdentifier()])
            ngsSample.setExperiment(ngsExperiment)
            sa = ngsSample
    # create new dataset
    dataSet = transaction.createNewDataSet("Q_NGS_RAW_DATA")
    dataSet.setMeasuredData(False)
    dataSet.setSample(sa)

    datafolder = os.path.join(incomingPath,name)
    for f in os.listdir(incomingPath):
        fPath = os.path.join(incomingPath,f)
        if "source_dropbox.txt" in f:
            os.remove(os.path.realpath(fPath))
        if ".origlabfilename" in f:
            nameFile = open(fPath)
            origName = nameFile.readline().strip()
            nameFile.close()
            os.remove(os.path.realpath(fPath))
        if ".sha256sum" in f:
            os.rename(fPath, os.path.join(datafolder, f))
    transaction.moveFile(datafolder, dataSet)

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
    if isExpected(identifier):
        pass
                #experiment = identifier[1:5]
                #parentCode = identifier[:10]
    else:
         print "The identifier "+identifier+" did not match the pattern Q[A-Z]{4}\d{3}\w{2} or checksum"

    project = identifier[:5]
    search_service = transaction.getSearchService()
    sc = SearchCriteria()
    pc = SearchCriteria()
    pc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.PROJECT, project));
    sc.addSubCriteria(SearchSubCriteria.createExperimentCriteria(pc))
    foundSamples = search_service.searchForSamples(sc)
    space = foundSamples[0].getSpace()
    global numberOfExperiments
    numberOfExperiments = len(search_service.listExperiments("/" + space + "/" + project))

    src = os.path.realpath(os.path.join(incomingPath,'source_dropbox.txt'))
    if os.path.isfile(src):
        os.remove(src)

    print "imgag start registration"
    #dataSet = None
    metadataFound = False
    for f in os.listdir(os.path.join(incomingPath,name)):
        if f.endswith('metadata'):
            metadataFound = True
            metadataPath = os.path.realpath(os.path.join(os.path.join(incomingPath, name),f))
            jsonContent = parse_metadata_file(metadataPath)
            rawFiles = jsonContent["files"]
            vcfs = []
            fastqs = []
            gsvars = []
            tsvs = []
            print "metadata read"
            for rawFile in rawFiles: #example: ["GS130715_03-GS130717_03_var_annotated.vcf.gz","GS130715_03-GS130717_03.GSvar"]
                print rawFile
                if rawFile.endswith("vcf") or rawFile.endswith("vcf.gz"):
                    vcfs.append(rawFile)
                if rawFile.endswith("fastq") or rawFile.endswith("fastq.gz"):
                    fastqs.append(rawFile)
                if rawFile.endswith("GSvar") or rawFile.endswith("GSvar.gz"):
                    gsvars.append(rawFile)
                if rawFile.endswith("tsv") or rawFile.endswith("tsv.gz"):
                    tsvs.append(rawFile)


            #if rawFiles[0].endswith("vcf") or rawFiles[0].endswith("vcf.gz"):
            #	datasetSample = find_and_register_vcf(transaction, jsonContent)
            #
            #	dataSet = transaction.createNewDataSet("Q_NGS_VARIANT_CALLING_DATA")
            #	dataSet.setSample(datasetSample)

            #elif rawFiles[0].endswith("fastq") or rawFiles[0].endswith("fastq.gz"):
            #	datasetSample = find_and_register_ngs(transaction, jsonContent)

            #	dataSet = transaction.createNewDataSet("Q_NGS_RAW_DATA")
                #	dataSet.setSample(datasetSample)

                #os.remove(os.path.realpath(os.path.join(os.path.join(incomingPath,name),f)))
        else:
            pass
    folder = os.path.join(incomingPath, name)
    if(metadataFound):
        if len(fastqs) > 0:
            fastqSample = find_and_register_ngs(transaction, jsonContent)
            fastqDataSet = transaction.createNewDataSet("Q_NGS_RAW_DATA")
            fastqDataSet.setSample(fastqSample)

            fastqFolder = os.path.join(folder, name+"_fastq_files")
            os.mkdir(fastqFolder)
            for f in fastqs:
                os.rename(os.path.join(folder, f), os.path.join(fastqFolder, f))
            for t in tsvs:
                os.rename(os.path.join(folder, t), os.path.join(fastqFolder, t))

            metadatafilename = metadataPath.split('/')[-1]
            copyfile(metadataPath, os.path.join(fastqFolder,metadatafilename))

            transaction.moveFile(fastqFolder, fastqDataSet)
            #transaction.moveFile(folder, fastqDataSet)
        for vc in vcfs:
            ident = vc.split('.')[0].replace('_vc_strelka','').replace('_var','').replace('_annotated','') #example: GS130715_03-GS130717_03
            print ident
            vcfSample = find_and_register_vcf(transaction, jsonContent, ident)
            vcfDataSet = transaction.createNewDataSet("Q_NGS_VARIANT_CALLING_DATA")
            vcfDataSet.setSample(vcfSample)
            vcfFolder = os.path.join(folder, name+"_vcf_files")
            os.mkdir(vcfFolder)
            os.rename(os.path.join(folder, vc), os.path.join(vcfFolder, vc))

            for g in gsvars:
                if(ident == g.split('.')[0]):
                    os.rename(os.path.join(folder,g), os.path.join(vcfFolder, g))

            metadatafilename = metadataPath.split('/')[-1]
            copyfile(metadataPath, os.path.join(vcfFolder,metadatafilename))
            #transaction.moveFile(folder, vcfDataSet)
            transaction.moveFile(vcfFolder, vcfDataSet)
    else:
            find_and_register_ngs_without_metadata(transaction)