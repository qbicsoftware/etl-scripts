'''

Note: 
print statements go to: ~openbis/servers/datastore_server/log/startup_log.txt
'''
import sys
sys.path.append('/home-link/qeana10/bin/')

import re
import os
import time
import datetime
import shutil
import subprocess
import checksum
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

        nameSplit = name.split("-")
        space = nameSplit[0]
        project = pPattern.findall(nameSplit[1])[0]
        experiment_id = ePattern.findall(nameSplit[2])[0]

        #Register logs
        wfSampleCode = nameSplit[-1]
        if not experiment_id:
                print "The identifier matching the pattern Q\w{4}E\[0-9]+ was not found in the fileName "+name

        ss = transaction.getSearchService()

        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, wfSampleCode))
        foundSamples = ss.searchForSamples(sc)
        samplehit = foundSamples[0]
        wfSample = transaction.getSampleForUpdate(samplehit.getSampleIdentifier())
        
        experiment = transaction.getExperimentForUpdate("/"+space+"/"+project+"/"+experiment_id)

        experiment.setPropertyValue("Q_WF_STATUS", "FINISHED")
        endpoint = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        experiment.setPropertyValue("Q_WF_FINISHED_AT", endpoint)
        sample.setExperiment(experiment)

        dataSetLogs = transaction.createNewDataSet('Q_WF_MS_PEAKPICKING_LOGS')
        dataSetLogs.setMeasuredData(False)
        dataSetLogs.setSample(wfSample)

        logname = incomingPath+"/workflow_logs"
        os.rename(incomingPath+"/logs", logname)
        transaction.moveFile(logname, dataSetLogs)

        #Register Results
        results = os.path.join(incomingPath,"result")
        for mzml in os.listdir():
                mzmlPath = os.path.join(results,"centroided_"+mzml)
                os.rename(os.path.join(results,mzml),mzmlPath)
                identifier = pattern.findall(mzml)[0]
                if isExpected(identifier):
                        code = identifier[:10]
                else:
                        print "The identifier "+identifier+" did not match the pattern 'Q\w{4}[0-9]{3}[a-zA-Z]\w' or checksum"
                search_service = transaction.getSearchService()
                sc = SearchCriteria()
                sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, code))
                foundSamples = search_service.searchForSamples(sc)
                sampleID = foundSamples[0].getSampleIdentifier()
                space = foundSamples[0].getSpace()
                sa = transaction.getSampleForUpdate(sampleID)
                dataSetRes = transaction.createNewDataSet('Q_MS_MEASUREMENT')
                dataSetRes.setSample(sa)
                transaction.moveFile(mzmlPath, dataSetRes)