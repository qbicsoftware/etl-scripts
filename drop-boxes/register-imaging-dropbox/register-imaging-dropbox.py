'''

Note:
print statements go to: ~/openbis/servers/datastore_server/log/datastore_server_log.txt
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

# ETL script for registration of VCF files
# expected:
# *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
pattern = re.compile('Q\w{4}[0-9]{3}[a-zA-Z]\w')


class PropertyParsingError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value


class SampleNotFoundError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

class SampleAlreadyCreatedError(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

class ExperimentNotFoundError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

# that's a very very simple Property validator... make better ones in the
# future


def validateProperty(propStr):
    if propStr != '':
        return(True)

    return(False)


def mangleFilenameForAttributes(filename):
    filename_split = filename.split('_')

    propertyMap = {}

    if len(filename_split) >= 8:
        expID = filename_split[0].strip()
        if validateProperty(expID):
            propertyMap['expID'] = expID
        else:
            raise PropertyParsingError('expID was empty')


        qbicID = filename_split[1].strip()
        if validateProperty(qbicID):
            propertyMap['qbicID'] = qbicID
        else:
            raise PropertyParsingError('qbicID was empty')

        patientID = filename_split[2].strip()
        if validateProperty(patientID):
            propertyMap['patientID'] = patientID
        else:
            raise PropertyParsingError('patientID was empty')

        timepoint = filename_split[3].strip()
        if validateProperty(timepoint):
            propertyMap['timepoint'] = timepoint
        else:
            raise PropertyParsingError('timepoint was empty')

        modality = filename_split[4].strip()
        if validateProperty(modality):
            propertyMap['modality'] = modality
        else:
            raise PropertyParsingError('modality was empty')

        tracer = filename_split[5].strip()
        if validateProperty(tracer):
            propertyMap['tracer'] = tracer
        else:
            raise PropertyParsingError('tracer was empty')

        tissue = filename_split[6].strip()
        if validateProperty(tissue):
            propertyMap['tissue'] = tissue
        else:
            raise PropertyParsingError('tracer was empty')


        # do the suffix check here
        datestr = filename_split[7].strip()
        if '.tar' in datestr:
            lastsplit = datestr.split('.')

            if validateProperty(lastsplit[0]):
                propertyMap['datestr'] = lastsplit[0]
            else:
                raise PropertyParsingError('datestr was empty')

        else:
            raise PropertyParsingError(
                'File does not have the correct suffix (*.tar)!')
    else:
        raise PropertyParsingError(
            'Filename does not seem to have the correct number of properties!')

    return propertyMap


def isExpected(identifier):
    try:
        id = identifier[0:9]
        # also checks for old checksums with lower case letters
        return checksum.checksum(id) == identifier[9]
    except:
        return False

def process(transaction):
    context = transaction.getRegistrationContext().getPersistentMap()

    # Get the incoming path of the transaction
    incomingPath = transaction.getIncoming().getAbsolutePath()

    key = context.get("RETRY_COUNT")
    if (key == None):
        key = 1

    # Get the name of the incoming file
    name = transaction.getIncoming().getName()

    # identifier = pattern.findall(name)[0]
    # if isExpected(identifier):
    #         project = identifier[:5]
    #         #parentCode = identifier[:10]
    # else:
    # print "The identifier "+identifier+" did not match the pattern
    # Q[A-Z]{4}\d{3}\w{2} or checksum"
    propertyMap = mangleFilenameForAttributes(name)

    # we'll get qbic code and patient id
    expID = propertyMap['expID']
    code = propertyMap['qbicID']
    projectCode = code[:5]
    patientID = propertyMap['patientID']
    timepoint = propertyMap['timepoint']
    modality = propertyMap['modality']
    tracer = propertyMap['tracer']
    tissue = propertyMap['tissue']
    timestamp = propertyMap['datestr']


    # print "look for: ", code

    search_service = transaction.getSearchService()
    sc = SearchCriteria()    # Find the patient according to code
    sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(
        SearchCriteria.MatchClauseAttribute.CODE, code))
    foundSamples = search_service.searchForSamples(sc)

    if not len(foundSamples) > 0:
        raise SampleNotFoundError(
            'openBIS query of ' + code + ' failed. Please recheck your QBiC code!')

    # produces an IndexError if sample code does not exist (will check before)
    sampleIdentifier = foundSamples[0].getSampleIdentifier()

    space = foundSamples[0].getSpace()
    rootSample = transaction.getSampleForUpdate(sampleIdentifier)

    #print code, "was found in space", space, "as", sampleIdentifier

    # get or create MS-specific experiment/sample and
    # attach to the test sample
    expType = "Q_BMI_GENERIC_IMAGING"

    # load imaging experiments to append new data
    activeExperiment = None
    experiments = search_service.listExperiments("/" + space + "/" + projectCode)
    experimentIDs = []
    fullExpIdentifier = '/' + space + '/' + projectCode + '/' + expID

    for exp in experiments:
        if exp.getExperimentType() == expType and exp.getExperimentIdentifier() == fullExpIdentifier:
            activeExperiment = exp

    # if expID is not found...
    if (activeExperiment == None):
        raise ExperimentNotFoundError('Experiment with ID ' + expID + ' could not be found! Check the ID.')


    existingSamples = search_service.listSamplesForExperiment(fullExpIdentifier)

    imagingSampleCode = modality + '-' + tracer + '-' + tissue + '-' + timepoint + '-' + str(len(existingSamples) + 1).zfill(3)
    imagingSample = transaction.createNewSample('/' + space + '/' + imagingSampleCode, "Q_BMI_GENERIC_IMAGING_RUN")
    imagingSample.setParentSampleIdentifiers([rootSample.getSampleIdentifier()])
    imagingSample.setExperiment(activeExperiment)


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
    #sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(
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
