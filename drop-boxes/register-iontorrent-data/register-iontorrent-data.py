'''

Note:
print statements go to: ~/openbis/servers/datastore_server/log/datastore_server_log.txt
'''
import sys

sys.path.append('/home-link/qeana10/bin')

import checksum
import re
import os
from datetime import datetime
import hashlib
import glob
import zipfile
import subprocess
import ch.systemsx.cisd.etlserver.registrator.api.v2
from java.io import File
from org.apache.commons.io import FileUtils
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria


from extractPGMdata import *


# *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
pattern = re.compile('Q\w{4}[0-9]{3}[a-zA-Z]\w')

# snpEff jar
snpEffJarPath = '/mnt/DSS1/iisek01/snpEff/snpEff.jar'


class IonTorrentDropboxError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


def printInfosToStdOut(message):
    print '[' + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']', 'IonTorrentDropbox:', message


# that's a very very simple Property validator... make better ones in the
# future


def validateProperty(propStr):
    if propStr != '':
        return(True)

    return(False)

# barcode creation stuff
def create_count_string(firstFreeBarcodeId):
    return "%03d" % firstFreeBarcodeId

def map_to_char(num):
    num += 48
    if num > 57:
      num += 7
    return chr(num)
def create_checksum(code):
    i = 1
    sum = 0
    for s in code:
        sum += ord(s) * i
        i += 1
    return map_to_char(sum % 34)

def create_barcode(projectCode, firstFreeBarcodeId, classChar):
    code = projectCode + create_count_string(firstFreeBarcodeId) + classChar
    return code + create_checksum(code)


# compute sha256sum on huge files (need to do it chunk-wise)
def computeSha256Sum(fileFullPath, chunkSize = 8*4096):
    sumObject = hashlib.sha256()
    infile = open(fileFullPath, 'rb')
    fileChunk = infile.read(chunkSize)
    while fileChunk:
        sumObject.update(fileChunk)
        fileChunk = infile.read(chunkSize)
    return sumObject.hexdigest()


def isExpected(identifier):
    try:
        id = identifier[0:9]
        # also checks for old checksums with lower case letters
        return checksum.checksum(id) == identifier[9]
    except:
        return False


def buildOpenBisTimestamp(datetimestr):
    inDateFormat = '%Y%m%d'
    outDateFormat = '%Y-%m-%d'

    return datetime.datetime.strptime(datetimestr, inDateFormat).strftime(outDateFormat)

def sampleExists(searchService, sampleCode):
    sc = SearchCriteria()
    sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(
        SearchCriteria.MatchClauseAttribute.CODE, sampleCode))
    foundSamples = searchService.searchForSamples(sc)

    if len(foundSamples) > 0:
        return True

    return False

def listSamplesForExperiment(searchService, sampleType, expID):
    sc = SearchCriteria()
    sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(
        SearchCriteria.MatchClauseAttribute.TYPE, sampleType))

    ec = SearchCriteria()

    ec.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(
        SearchCriteria.MatchClauseAttribute.CODE, expID))
    sc.addSubCriteria(SearchSubCriteria.createExperimentCriteria(ec))

    existingSamples = searchService.searchForSamples(sc)

    return existingSamples

def findExperimentByID(expIdentifier, transaction):
    # expIdentifier needs to be /SPACE/PROJECT/EXPERIMENT_CODE
    search_service = transaction.getSearchService()
    experiments = search_service.listExperiments('/UKT_PATHOLOGY_PGM/QPATH')

    results = []

    for exp in experiments:
        if exp.getExperimentIdentifier() == expIdentifier:
            printInfosToStdOut(expIdentifier + ' was found!')
            results.append(exp)
            break

    return results

def process(transaction):
    context = transaction.getRegistrationContext().getPersistentMap()

    # Get the incoming path of the transaction
    incomingPath = transaction.getIncoming().getAbsolutePath()

    key = context.get("RETRY_COUNT")
    if (key == None):
        key = 1

    # Get the name of the incoming file
    name = transaction.getIncoming().getName()

    #print incomingPath
    #print name

    varCallRunFolders = glob.glob(os.path.join(incomingPath, 'plugin_out/variantCaller*'))

    if len(varCallRunFolders) == 0:
        raise IonTorrentDropboxError('No variant calling data found in dataset! Aborting...')
    # should be just one variant calling folder... if not, we take the most recent ones
    latestVarCallFolder = varCallRunFolders[-1]

    varCallVcfFile = glob.glob(os.path.join(latestVarCallFolder, 'R_*.vcf.zip'))
    varCallXlsFile = glob.glob(os.path.join(latestVarCallFolder, 'R_*.xls.zip'))

    # better check if there are really two zip files in there
    if len(varCallXlsFile) == 0 or len(varCallVcfFile) == 0:
        raise IonTorrentDropboxError('No VCF/XLS zipfiles found in variantCaller folder! Aborting...')

    # unzip the stuff in a temporary folder. Unfortunately, /tmp is too small
    # unpack it to /mnt/DSS1/iisek01/fakeTmp for now
    fakeTmpBaseDir = '/mnt/DSS1/iisek01/fakeTmp'
    unzipDir = os.path.join(fakeTmpBaseDir, name)

    if not os.path.exists(unzipDir):
        os.makedirs(unzipDir)

    # workaround for older Python/Jython versions which don't have extractall method
    # we will use the OS' unzip command

    # vcf_zip_file = zipfile.ZipFile(varCallVcfFile[-1], 'r')
    # for zFile in vcf_zip_file.namelist():
    #     if not '.vcf.gz.tbi' in zFile:
    #         zFileContent = vcf_zip_file.read(zFile)
    #         zFileOut = open(os.path.join(unzipDir, zFile), 'wb')
    #         zFileOut.write(zFileContent)
    #         gzFile = gzip.GzipFile(os.path.join(unzipDir, zFile), 'rb')
    #         gzFileContent = gzFile.read()
    #         gzFile.close()
    #         unzippedName, gzExt = os.path.splitext(zFile)
    #         gunzipFileOut = open(os.path.join(unzipDir, unzippedName), 'wb')
    #         gunzipFileOut.write(gzFileContent)
    #         gunzipFileOut.close()
    # vcf_zip_file.close()

    unzipCommand = ['unzip', '-o', varCallVcfFile[-1], '-d', unzipDir]
    p = subprocess.call(unzipCommand)
    #xtrVcfGzPaths = glob.glob(unzipDir + '/*.vcf.gz')
    gunzipCommand = ['gunzip', os.path.join(unzipDir, '*.vcf.gz')]
    p = subprocess.call(unzipCommand)

    # xls_zip_file = zipfile.ZipFile(varCallXlsFile[-1], 'r')
    # for zFile in xls_zip_file.namelist():
    #     zFileContent = xls_zip_file.read(zFile)
    #     open(os.path.join(unzipDir, zFile), 'wb').write(zFileContent)
    # xls_zip_file.close()

    unzipCommand = ['unzip', '-o', varCallXlsFile[-1], '-d', unzipDir]
    p = subprocess.call(unzipCommand)

    # let's do some sanity checks first; number of XLS/VCF should be same as BAM files
    xtrVCFPaths = glob.glob(unzipDir + '/*.vcf')
    #annVCFPaths = [f for f in xtrVCFPaths if '_ann.vcf' in f]
    xtrXLSPaths = glob.glob(unzipDir + '/*.xls')
    bamFilePaths = glob.glob(incomingPath + '/*.bam')

    if (len(xtrXLSPaths) != len(bamFilePaths)) or (len(xtrVCFPaths) != len(bamFilePaths)):
        raise IonTorrentDropboxError('Number of BAM files and VCF/XLS were diverging! Aborting...')
    else:
        printInfosToStdOut('VCF/XLS files correspond to BAM file numbers.')

    # vcfs are extracted, now it's time to tar the whole iontorrent folder
    # get parent of incomingPath
    prestagingDir = os.path.dirname(incomingPath)
    tarFileFullPath = os.path.join(fakeTmpBaseDir, name + '.tar')

    # only tar it if file is not existing yet (TODO: leave check only for testing/development)
    if not os.path.exists(tarFileFullPath):
        tarCommand = ['tar', '-cf', tarFileFullPath, '-C', prestagingDir, name]
        p = subprocess.call(tarCommand)

    # compute the sha256sum of the tar and check against openBIS
    printInfosToStdOut('computing sha256sum...')
    #tarFileSha256Sum = computeSha256Sum(tarFileFullPath)
    # TODO: computation works fine but to speed things up we disable it
    tarFileSha256Sum = '24dc899d5023675a34b4777fa4209c037d90e9c79f7d0d9a523b3f6cfd59252f'

    printInfosToStdOut('tar file sha256sum: ' + tarFileSha256Sum)

    # TODO: check if there is a Q_NGS_IONTORRENT_DATA object with this checksum
    # if yes, stop it right here


    for vcffile in xtrVCFPaths:
        printInfosToStdOut('Processing ' + vcffile)
        justFileName = os.path.basename(vcffile)
        printInfosToStdOut('filename only: ' + justFileName)
        basename, suffix = os.path.splitext(justFileName)
        annBaseDir = os.path.join(unzipDir, 'snpEff')
        annfile = os.path.join(annBaseDir, basename + '_ann' + suffix)

        # if the files are already there, don't redo it... it's a costly operation
        if not os.path.exists(annfile):
            snpEffCommand = ['java', '-Xmx4g', '-jar', snpEffJarPath, 'hg19', vcffile]
            printInfosToStdOut('Starting ' + snpEffCommand)
            annfile_out = open(annfile, 'w')
            p = subprocess.call(snpEffCommand, stdout=annfile_out)
            annfile_out.close()

    annVCFPaths = glob.glob(annBaseDir + '/*_ann.vcf')

    if len(xtrXLSPaths) != len(xtrVCFPaths):
        raise IonTorrentDropboxError('Not all VCF were annotated! Please check for snpEff errors!')


    # we create a new experiment here: one PGM run -> one experiment (container)
    # as always, check if the experiment already exists

    spaceCode = 'UKT_PATHOLOGY_PGM'
    projectCode = 'QPATH'

    projectFullIdentifier = '/' + spaceCode + '/' + projectCode




    xtrXLSPaths = sorted(xtrXLSPaths)
    annVCFPaths = sorted(annVCFPaths)

    search_service = transaction.getSearchService()
    experiments = search_service.listExperiments(projectFullIdentifier)
    #experimentIDs = []
    subjectCounter = 1
    sampleCounter = 1


    for exp in experiments:
        expID = exp.getExperimentIdentifier()
        expCode = expID.split('/')[-1]
        printInfosToStdOut('looking for expCode ' + expCode)
        if exp.getExperimentType() == 'Q_EXPERIMENTAL_DESIGN':
            foundSamples = listSamplesForExperiment(search_service, 'Q_BIOLOGICAL_ENTITY', expCode)

            print expID, ' holds ', len(foundSamples), ' patients'
            subjectCounter += len(foundSamples)

        if exp.getExperimentType() == 'Q_NGS_MEASUREMENT':
            foundSamples = listSamplesForExperiment(search_service, 'Q_NGS_SINGLE_SAMPLE_RUN', expCode)

            print expID, ' holds ', len(foundSamples), ' NGS runs'
            sampleCounter += len(foundSamples)


    newExperimentCode = 'PGM84'
    newExperimentFullIdentifier1 = '/' + spaceCode + '/' + projectCode + '/' + newExperimentCode + '-DESIGN'
    newExperimentFullIdentifier2 = '/' + spaceCode + '/' + projectCode + '/' + newExperimentCode + '-RUN'


    queryResults1 = findExperimentByID(newExperimentFullIdentifier1, transaction)
    queryResults2 = findExperimentByID(newExperimentFullIdentifier2, transaction)

    printInfosToStdOut(queryResults1)

    # expected case: there is no experiment with this PGM number yet
    # TODO: else we should quit the dropbox since we don't want to overwrite existing data
    # TODO: alternatively, we could create PGMxy-1, PGMxy-2, etc.
    freshIonPGMDesign = None
    freshIonPGMExperiment = None

    # we need to create two experiment objects: Q_EXPERIMENTAL_DESIGN and Q_NGS_MEASUREMENT
    if len(queryResults1) == 0:
        freshIonPGMDesign = transaction.createNewExperiment(newExperimentFullIdentifier1, 'Q_EXPERIMENTAL_DESIGN')
        freshIonPGMDesign.setPropertyValue('Q_SECONDARY_NAME', name)

        freshIonPGMExperiment = transaction.createNewExperiment(newExperimentFullIdentifier2, 'Q_NGS_MEASUREMENT')
        freshIonPGMExperiment.setPropertyValue('Q_SECONDARY_NAME', name)
        freshIonPGMExperiment.setPropertyValue('Q_SEQUENCER_DEVICE', 'UKT_PATHOLOGY_THERMO_IONPGM')
    else:
        # experiment exists, check if there are samples attached
        raise IonTorrentDropboxError(newExperimentCode + 'already contains samples! Aborting...')
        #freshIonPGMDesign = queryResults1[0]
        #freshIonPGMExperiment = queryResults2[0]

    patientIDprefix = 'QPATH-PAT-'

    # this is "the main loop" here: create openBIS samples for each VCF/XLS, extract the relevant variants for centraXX, ...
    for i in range(0,len(xtrXLSPaths)):
        newPatientID = patientIDprefix + str(subjectCounter).zfill(5)

        #if sampleExists(search_service, newPatientID):
        #    raise IonTorrentDropboxError('')

        print newPatientID, extractPGMdata(annVCFPaths[i], xtrXLSPaths[i])

        newPatient = transaction.createNewSample('/' + spaceCode + '/' + newPatientID, 'Q_BIOLOGICAL_ENTITY')
        newPatient.setPropertyValue('Q_NCBI_ORGANISM', '9606')
        newPatient.setExperiment(freshIonPGMDesign)

        newNGSsampleID = create_barcode('QPATH', sampleCounter, 'A')
        newNGSrun = transaction.createNewSample('/' + spaceCode + '/' + newNGSsampleID, 'Q_NGS_SINGLE_SAMPLE_RUN')
        newNGSrun.setParentSampleIdentifiers([newPatient.getSampleIdentifier()])
        newNGSrun.setExperiment(freshIonPGMExperiment)
        newVCFdataset = transaction.createNewDataSet('Q_NGS_VARIANT_CALLING_DATA')
        newVCFdataset.setMeasuredData(False)
        newVCFdataset.setSample(newNGSrun)

        # create temporary export folder to simulate file copy
        exportDir = os.path.join(fakeTmpBaseDir, 'export')

        if not os.path.exists(exportDir):
            os.makedirs(exportDir)

        copyCommand = ['cp', annVCFPaths[i], xtrXLSPaths[i], exportDir]
        p = subprocess.call(copyCommand)

        #files2export = glob.glob(os.path.join(exportDir, '*'))
        #printInfosToStdOut(files2export)

        transaction.moveFile(os.path.join(exportDir, os.path.basename(annVCFPaths[i])), newVCFdataset)
        transaction.moveFile(os.path.join(exportDir, os.path.basename(xtrXLSPaths[i])), newVCFdataset)

        subjectCounter += 1
        sampleCounter += 1
        #newNGSrun = transaction.createNewSample('/' + spaceCode + '/' + newPatientID, "Q_BIOLOGICAL_ENTITY")
        # newMSSample.setParentSampleIdentifiers([sa.getSampleIdentifier()])
        # newMSSample.setExperiment(msExperiment)
        # create new dataset
        # dataSet = transaction.createNewDataSet("Q_MS_MZML_DATA")
        # dataSet.setMeasuredData(False)
        # dataSet.setSample(newMSSample)

        #transaction.moveFile(incomingPath, dataSet)






    #raise IonTorrentDropboxError('sorry, raising an error to force a rollback')


    # identifier = pattern.findall(name)[0]
    # if isExpected(identifier):
    #         project = identifier[:5]
    #         #parentCode = identifier[:10]
    # else:
    # print "The identifier "+identifier+" did not match the pattern
    # Q[A-Z]{4}\d{3}\w{2} or checksum"
    # propertyMap = mangleFilenameForAttributes(name)
    #
    # # we'll get qbic code and patient id
    # expID = propertyMap['expID']
    # code = propertyMap['qbicID']
    # projectCode = code[:5]
    # patientID = propertyMap['patientID']
    # timepoint = propertyMap['timepoint']
    # modality = propertyMap['modality']
    # tracer = propertyMap['tracer']
    # tissue = propertyMap['tissue']
    # timestamp = propertyMap['datestr']

    # print "look for: ", code

    # all pathology iontorrent data goes to the same openBIS space

    # search_service = transaction.getSearchService()
    # sc = SearchCriteria()    # Find the patient according to code
    # sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(
    #     SearchCriteria.MatchClauseAttribute.CODE, code))
    # foundSamples = search_service.searchForSamples(sc)
    #
    # if not len(foundSamples) > 0:
    #     raise SampleNotFoundError(
    #         'openBIS query of ' + code + ' failed. Please recheck your QBiC code!')
    #
    # # produces an IndexError if sample code does not exist (will check before)
    # sampleIdentifier = foundSamples[0].getSampleIdentifier()
    #
    # space = foundSamples[0].getSpace()
    # rootSample = transaction.getSampleForUpdate(sampleIdentifier)
    #
    # # print code, "was found in space", space, "as", sampleIdentifier
    #
    # # get or create MS-specific experiment/sample and
    # # attach to the test sample
    # expType = "Q_BMI_GENERIC_IMAGING"
    #
    # # load imaging experiments to append new data
    # activeExperiment = None
    # experiments = search_service.listExperiments(
    #     "/" + space + "/" + projectCode)
    # experimentIDs = []
    # fullExpIdentifier = '/' + space + '/' + projectCode + '/' + expID
    #
    # for exp in experiments:
    #     if exp.getExperimentType() == expType and exp.getExperimentIdentifier() == fullExpIdentifier:
    #         activeExperiment = exp
    #
    # # if expID is not found...
    # if (activeExperiment == None):
    #     raise ExperimentNotFoundError(
    #         'Experiment with ID ' + expID + ' could not be found! Check the ID.')
    #
    # sc = SearchCriteria()    # Find the patient according to code
    # sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(
    #     SearchCriteria.MatchClauseAttribute.TYPE, "Q_BMI_GENERIC_IMAGING_RUN"))
    #
    # ec = SearchCriteria()
    #
    # ec.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(
    #     SearchCriteria.MatchClauseAttribute.CODE, expID))
    # sc.addSubCriteria(SearchSubCriteria.createExperimentCriteria(ec))
    #
    # existingSamples = search_service.searchForSamples(sc)
    #
    # imagingSampleCode = modality + '-' + tracer + '-' + tissue + '-' + \
    #     patientID + '-' + timepoint + '-' + \
    #     str(len(existingSamples) + 1).zfill(3)
    #
    # # let's first check if such an imaging run was registered before
    # sc = SearchCriteria()
    # sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(
    #     SearchCriteria.MatchClauseAttribute.CODE, imagingSampleCode))
    # foundSamples = search_service.searchForSamples(sc)
    #
    # if len(foundSamples) > 0:
    #     raise SampleAlreadyCreatedError(
    #         'Sample ' + imagingSampleCode + ' has been created already. Please re-check to avoid duplicates! Offending file: ' + incomingPath)
    #
    # imagingSample = transaction.createNewSample(
    #     '/' + space + '/' + imagingSampleCode, "Q_BMI_GENERIC_IMAGING_RUN")
    # imagingSample.setParentSampleIdentifiers(
    #     [rootSample.getSampleIdentifier()])
    # imagingSample.setExperiment(activeExperiment)
    #
    # sampleLabel = modality + ' imaging (' + patientID + ', ' + timepoint + ')'
    # imagingSample.setPropertyValue('Q_SECONDARY_NAME', sampleLabel)
    # imagingSample.setPropertyValue('Q_TIMEPOINT', timepoint)
    #
    # if tissue == 'liver':
    #     imagingSample.setPropertyValue('Q_IMAGED_TISSUE', 'LIVER')
    # elif tissue == 'tumor':
    #     imagingSample.setPropertyValue(
    #         'Q_IMAGED_TISSUE', 'HEPATOCELLULAR_CARCINOMA')
    #
    # openbisTimestamp = buildOpenBisTimestamp(timestamp)
    # imagingSample.setPropertyValue('Q_MSHCC_IMAGING_DATE', openbisTimestamp)
    #
    # # create new dataset
    # imagingDataset = transaction.createNewDataSet('Q_BMI_IMAGING_DATA')
    # imagingDataset.setMeasuredData(False)
    # imagingDataset.setSample(imagingSample)
    # imagingDataset.setPropertyValue(
    #     'Q_SECONDARY_NAME', modality + ' data (' + patientID + ', ' + timepoint + ')')
    #
    # # disable hash computation for now... resulted in outOfMemory errors for some bigger files
    # #incomingFileSha256Sum = hashlib.sha256(
    # #    open(incomingPath, 'rb').read()).hexdigest()
    # incomingFileSha256Sum = 'MISSING!'
    # imagingDataset.setPropertyValue(
    #     'Q_TARBALL_SHA256SUM', incomingFileSha256Sum)
    #
    # # finish the transaction
    # transaction.moveFile(incomingPath, imagingDataset)

    # raise SampleAlreadyCreatedError(
    #    'sampleQuery for Exp ' + expID + ": " + str(len(existingSamples)))
    #set([('MRPET', 'FDG'), ('MRPET', 'Cholin'), ('CTPerfusion', 'None'), ('Punktion', 'None')])
    # ('Punktion', 'None') -> QMSHS-BMI-001
    # ('CTPerfusion', 'None') -> QMSHS-BMI-002
    # ('MRPET', 'Cholin') -> QMSHS-BMI-003
    # ('MRPET', 'FDG') -> QMSHS-BMI-004

    # we assume there is no (imaging) experiment registered so far

    # CAVEAT: experiments were preregistered now! this means we need the exp ID in filenames
    # expNum = len(experiments) + 1
    # expID  = '/' + space + '/' + project + '/' + project + '-BMI' + str(expNum).zfill(3)

    # genericImagingExperiment = transaction.createNewExperiment(expID, expType)

    # since the imaging data is newly integrated here, there's no preregistered sample
    #msCode = 'MS' + code

    #sc = SearchCriteria()
    # sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(
    #    SearchCriteria.MatchClauseAttribute.CODE, msCode))

    #existingSamples = search_service.listSamplesForExperiment(fullExpIdentifier)

    #print("number existing samples for " + expID + " " + str(len(existingSamples)))
    # if len(foundSamples) < 1:
    #     msSample = transaction.createNewSample('/' + space + '/' + msCode, "Q_MS_RUN")
    #     msSample.setParentSampleIdentifiers([sa.getSampleIdentifier()])
    #     msSample.setExperiment(MSRawExperiment)
    # else:
    #     msSample = transaction.getSampleForUpdate(foundSamples[0].getSampleIdentifier())
    #
    # # create new dataset
    # rawDataSet = transaction.createNewDataSet("Q_MS_RAW_DATA")
    # rawDataSet.setMeasuredData(False)
    # rawDataSet.setSample(msSample)
    #
 #   	#cegat = False
    # f = "source_dropbox.txt"
    # sourceLabFile = open(os.path.join(incomingPath,f))
 #   	sourceLab = sourceLabFile.readline().strip()
    # sourceLabFile.close()
    # os.remove(os.path.realpath(os.path.join(incomingPath,f)))
    #
    # for f in os.listdir(incomingPath):
    #     if ".origlabfilename" in f:
    #         os.remove(os.path.realpath(os.path.join(incomingPath,f)))
    # transaction.moveFile(incomingPath, rawDataSet)
