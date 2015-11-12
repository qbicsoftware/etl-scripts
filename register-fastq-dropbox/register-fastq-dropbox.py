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
        #identifier = name
	if isExpected(identifier):
                project = identifier[:5]
                #parentCode = identifier[:10]
        else:
                print "The identifier "+identifier+" did not match the pattern Q[A-Z]{4}\d{3}\w{2} or checksum"
        
        search_service = transaction.getSearchService()
        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, identifier))
        foundSamples = search_service.searchForSamples(sc)

        sampleIdentifier = foundSamples[0].getSampleIdentifier()
        space = foundSamples[0].getSpace()
        sa = transaction.getSampleForUpdate(sampleIdentifier)
        #numberOfExperiments = len(search_service.listExperiments("/" + space + "/" + project)) + 1
        #newVariantCallingExperiment = transaction.createNewExperiment('/' + space + '/' + project + '/' + project + 'E' + str(numberOfExperiments), "Q_NGS_VARIANT_CALLING")

        #newVariantCallingSample = transaction.createNewSample('/' + space + '/' + 'VC'+ parentCode, "Q_NGS_VARIANT_CALLING")
        #newVariantCallingSample.setParentSampleIdentifiers([sa.getSampleIdentifier()])
      
	#newVariantCallingSample.setExperiment(newVariantCallingExperiment) 
        # create new dataset 
        dataSet = transaction.createNewDataSet("Q_NGS_RAW_DATA")
        dataSet.setMeasuredData(False)
        dataSet.setSample(sa)

       	cegat = False
        f = "source_dropbox.txt"
        sourceLabFile = open(os.path.join(incomingPath,f))
       	sourceLab = sourceLabFile.readline().strip() 
        sourceLabFile.close()
        if sourceLab == 'dmcegat':
                cegat = True
        os.remove(os.path.realpath(os.path.join(incomingPath,f)))

        f = name+".origlabfilename"
       	nameFile = open(os.path.join(incomingPath,f))
        origName = nameFile.readline().strip()
        nameFile.close()
        if sourceLab == 'dmcegat':
                cegat = True
                secondaryName = origName.split('_')[3]
                sa.setPropertyValue('Q_SECONDARY_NAME', secondaryName)
        os.remove(os.path.realpath(os.path.join(incomingPath,f)))

        for f in os.listdir(incomingPath):
		if ".testorig" in f:
			os.remove(os.path.realpath(os.path.join(incomingPath,f)))
        transaction.moveFile(incomingPath, dataSet)
