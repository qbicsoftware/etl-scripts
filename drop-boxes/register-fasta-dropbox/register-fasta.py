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

# ETL script for registration of VCF files
# expected:
# *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
pattern = re.compile('Q\w{4}[0-9]{3}[a-zA-Z]\w')

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
 
    # create new dataset 
    dataSet = transaction.createNewDataSet("Q_FASTA_DATA")
    dataSet.setMeasuredData(False)

    search_service = transaction.getSearchService()

    vcf = re.compile("VCQ\w{4}[0-9]{3}[A-Z]\w[A-Z]*")
    vcfCodes = vcf.findall(name)

    if len(vcfCodes) > 0:
        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, vcfCodes[0]))
        foundSamples = search_service.searchForSamples(sc)
        vcSample = transaction.getSampleForUpdate(foundSamples[0].getSampleIdentifier())
    else:
        # vcf sample needs to be created        
        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, parentCode))
        foundSamples = search_service.searchForSamples(sc)

        parentSampleIdentifier = foundSamples[0].getSampleIdentifier()
        space = foundSamples[0].getSpace()
        sa = transaction.getSampleForUpdate(parentSampleIdentifier)
        
        # register new experiment and sample
        existingExperimentIDs = []
        existingExperiments = search_service.listExperiments("/" + space + "/" + project)
    
        numberOfExperiments = len(existingExperiments) + 1

        for eexp in existingExperiments:
            existingExperimentIDs.append(eexp.getExperimentIdentifier())

        newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(numberOfExperiments)

        while newExpID in existingExperimentIDs:
            numberOfExperiments += 1 
            newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(numberOfExperiments)

        newVariantCallingExperiment = transaction.createNewExperiment(newExpID, "Q_FASTA_INFO")
        newVariantCallingExperiment.setPropertyValue('Q_CURRENT_STATUS', 'FINISHED')

        search_service = transaction.getSearchService()
        sc = SearchCriteria()
        pc = SearchCriteria()
        pc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.PROJECT, project))
        sc.addSubCriteria(SearchSubCriteria.createExperimentCriteria(pc))
        foundSamples2 = search_service.searchForSamples(sc)

        vcNumber = 1
        newSampleID = '/' + space + '/' + 'FASTA' + str(vcNumber) + parentCode
        existingSampleIDs = []

        for samp in foundSamples2:
            existingSampleIDs.append(samp.getSampleIdentifier())

        # search in known ids, but also try to fetch the sample in case it wasn't indexed yet
        while newSampleID in existingSampleIDs or transaction.getSampleForUpdate(newSampleID):
            vcNumber += 1
            newSampleID = '/' + space + '/' + 'FASTA' + str(vcNumber) + parentCode
            
        vcSample = transaction.createNewSample(newSampleID, "Q_FASTA")
        vcSample.setParentSampleIdentifiers([sa.getSampleIdentifier()])
        vcSample.setExperiment(newVariantCallingExperiment)

        newIncoming = os.path.realpath(incomingPath).replace('.fasta', '').replace('.fsa', '')
        os.mkdir(newIncoming)

        for f in os.listdir(incomingPath):
            if f.endswith('origlabfilename'):
                origName = open(os.path.join(incomingPath,f), 'r')
                secondaryName = origName.readline().strip().split('_')[0]
                origName.close()
                sa.setPropertyValue('Q_SECONDARY_NAME', secondaryName)
                os.remove(os.path.realpath(os.path.join(incomingPath,f)))
            elif f.endswith('sha256sum') or f.endswith('fasta') or f.endswith('fsa'):
                os.rename(os.path.realpath(os.path.join(incomingPath, f)), os.path.join(newIncoming, f))
            else:
                os.remove(os.path.realpath(os.path.join(incomingPath,f)))

        dataSet.setSample(vcSample)
        transaction.moveFile(newIncoming, dataSet)
