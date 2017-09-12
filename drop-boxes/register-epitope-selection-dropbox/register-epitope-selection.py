import re
import os
import time
import datetime
import shutil
import subprocess
import ch.systemsx.cisd.etlserver.registrator.api.v2
from java.io import File
from org.apache.commons.io import FileUtils
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria

# Data import and registration
# expected:
# *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
ePattern = re.compile('Q\w{4}E[0-9]+')
pPattern = re.compile('Q\w{4}')
sPattern = re.compile('Q[A-Z0-9]{4}E[0-9]{2}[A-Z0-9]{2}')
barcode = re.compile('Q[A-Z0-9]{4}[0-9]{3}[A-Z0-9]{2}')

def process(transaction):
    context = transaction.getRegistrationContext().getPersistentMap()

    # Get the incoming path of the transaction
    incomingPath = transaction.getIncoming().getAbsolutePath()

    key = context.get("RETRY_COUNT")
    if (key == None):
            key = 1

    # Get the name of the incoming file
    name = transaction.getIncoming().getName()
    foundBarcode = barcode.findall(name)[0]
    wfSample = sPattern.findall(name)[0]

    project = foundBarcode[:5]
    parentCode = foundBarcode[:10]

    ss = transaction.getSearchService()
    sc = SearchCriteria()
    sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, wfSample))
    foundSamples = ss.searchForSamples(sc)
    samplehit = foundSamples[0]
    space = foundSamples[0].getSpace()
    sample = transaction.getSampleForUpdate(samplehit.getSampleIdentifier())

    newNumber = 1
    newSampleID = '/' + space + '/' + 'VAC' + str(newNumber) + wfSample
    existingSampleIDs = []

    sc = SearchCriteria()
    pc = SearchCriteria()
    pc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.PROJECT, project))
    sc.addSubCriteria(SearchSubCriteria.createExperimentCriteria(pc))
    foundSamples2 = ss.searchForSamples(sc)

    for samp in foundSamples2:
        existingSampleIDs.append(samp.getSampleIdentifier())

    # search in known ids, but also try to fetch the sample in case it wasn't indexed yet
    while newSampleID in existingSampleIDs or transaction.getSampleForUpdate(newSampleID):
        newNumber += 1
        newSampleID = '/' + space + '/' + 'VAC' + str(newNumber) + wfSample
        
    newSample = transaction.createNewSample(newSampleID, "Q_VACCINE_CONSTRUCT")
    newSample.setParentSampleIdentifiers([samplehit.getSampleIdentifier()])

    existingExperimentIDs = []
    existingExperiments = ss.listExperiments("/" + space + "/" + project)
    numberOfExperiments = len(existingExperiments) + 1

    for eexp in existingExperiments:
        existingExperimentIDs.append(eexp.getExperimentIdentifier())

    newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(numberOfExperiments)

    while newExpID in existingExperimentIDs:
        numberOfExperiments += 1 
        newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(numberOfExperiments)

    experiment = transaction.createNewExperiment(newExpID, "Q_NGS_EPITOPE_SELECTION")
    experiment.setPropertyValue('Q_CURRENT_STATUS', 'FINISHED')
    newSample.setExperiment(experiment)

    #Register files
    dataSetRes = transaction.createNewDataSet('Q_VACCINE_CONSTRUCT_DATA')
    dataSetRes.setMeasuredData(False)
    dataSetRes.setSample(newSample)

    os.remove(os.path.realpath(os.path.join(incomingPath,'source_dropbox.txt')))

    resultsname = name.replace(foundBarcode + '__' ,'').replace('.txt', '')
    new_folder = os.path.realpath(os.path.join(incomingPath, resultsname))
    os.mkdir(new_folder)

    for f in os.listdir(incomingPath):
        if f.endswith('origlabfilename'):
            os.remove(os.path.realpath(os.path.join(incomingPath,f)))
        else:
            new_name = f.replace(foundBarcode + '__', '')
            os.rename(os.path.join(incomingPath, f), os.path.join(new_folder, new_name))

    transaction.moveFile(new_folder, dataSetRes)

