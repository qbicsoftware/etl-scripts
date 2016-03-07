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
import datetime
from java.io import File
from org.apache.commons.io import FileUtils
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria

# ETL script for registration of MS data coming from immunology departmen
# expected:
# *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
pattern = re.compile('Q\w{4}[0-9]{3}[a-zA-Z]\w')

xmltemplate = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?> <qproperties> <qfactors> <qcategorical label=\"technical_replicate\" value=\"%s\"/> <qcategorical label=\"workflow_type\" value=\"%s\"/> </qfactors> </qproperties>"
def isExpected(identifier):
    try:
        id = identifier[0:9]
        #also checks for old checksums with lower case letters
        return checksum.checksum(id)==identifier[9]
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
        
    identifier = pattern.findall(name)[0]
    if isExpected(identifier):
        experiment = identifier[1:5]
        project = identifier[:5]
        parentCode = identifier[:10]
    else:
        print "The identifier "+identifier+" did not match the pattern Q[A-Z]{4}\d{3}\w{2} or checksum"


    for root, subFolders, files in os.walk(incomingPath):
        if subFolders:
            subFolder = subFolders[0]
        for f in files:
            if f.endswith('.tsv'):
                metadataFile = open(os.path.join(root, f), 'r')
    
    metadataFile.readline()
    run = 1
    for line in metadataFile:
        splitted = line.split('\t')
        fileName = splitted[0]
        instr = splitted[1] # Q_MS_DEVICE (controlled vocabulary)
        date_input = splitted[2]
        share = splitted[3]
        comment = splitted[4]
        method = splitted[5]
        repl = splitted[6]
        wf_type = splitted[7]

        date = datetime.datetime.strptime(date_input, "%y%m%d").strftime('%Y-%m-%d')

        search_service = transaction.getSearchService()
        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, parentCode))
        foundSamples = search_service.searchForSamples(sc)

        parentSampleIdentifier = foundSamples[0].getSampleIdentifier()
        space = foundSamples[0].getSpace()
        sa = transaction.getSampleForUpdate(parentSampleIdentifier)

         # register new experiment and sample
        existingExperimentIDs = []
        existingExperiments = search_service.listExperiments("/" + space + "/" + project)
        
        numberOfExperiments = len(search_service.listExperiments("/" + space + "/" + project)) + run

        for eexp in existingExperiments:
            existingExperimentIDs.append(eexp.getExperimentIdentifier())

        newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(numberOfExperiments)

        while newExpID in existingExperimentIDs:
            numberOfExperiments += 1 
            newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(numberOfExperiments)

        newMSExperiment = transaction.createNewExperiment(newExpID, "Q_MS_MEASUREMENT")
        newMSExperiment.setPropertyValue('Q_CURRENT_STATUS', 'FINISHED')
        newMSExperiment.setPropertyValue('Q_MS_DEVICE', instr)
        newMSExperiment.setPropertyValue('Q_MEASUREMENT_FINISH_DATE', date)
        newMSExperiment.setPropertyValue('Q_EXTRACT_SHARE', share)
        newMSExperiment.setPropertyValue('Q_ADDITIONAL_INFO', comment)
        newMSExperiment.setPropertyValue('Q_MS_LCMS_METHOD', method)

        newMSSample = transaction.createNewSample('/' + space + '/' + 'MS'+ str(run) + parentCode, "Q_MS_RUN")
        newMSSample.setParentSampleIdentifiers([sa.getSampleIdentifier()])
        newMSSample.setExperiment(newMSExperiment)
        properties = xmltemplate % (repl, wf_type)
        newMSSample.setPropertyValue('Q_PROPERTIES', properties)
        # conversion ?
        newDataSet = transaction.createNewDataSet("Q_MS_RAW_DATA")
        newDataSet.setSample(newMSSample)
        
        run += 1
        transaction.moveFile(os.path.join(incomingPath, fileName), newDataSet)
