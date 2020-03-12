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
import ch.systemsx.cisd.etlserver.registrator.api.v2
from java.io import File
from org.apache.commons.io import FileUtils
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria
######## Sample Tracking related import
from life.qbic.sampletracking import SampleTracker
from life.qbic.sampletracking import ServiceCredentials
from java.net import URL

import sample_tracking_helper_qbic as tracking_helper
#### Setup Sample Tracking service
SERVICE_CREDENTIALS = ServiceCredentials()
SERVICE_CREDENTIALS.user = tracking_helper.get_service_user()
SERVICE_CREDENTIALS.password = tracking_helper.get_service_password()
SERVICE_REGISTRY_URL = URL(tracking_helper.get_service_reg_url())
QBIC_LOCATION = tracking_helper.get_qbic_location_json()

### We need this object to update the sample location later
SAMPLE_TRACKER = SampleTracker.createQBiCSampleTracker(SERVICE_REGISTRY_URL, SERVICE_CREDENTIALS, QBIC_LOCATION)

# Data import and registration
# expected:
# *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
ePattern = re.compile('Q\w{4}E[0-9]+')
pPattern = re.compile('Q\w{4}')

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
        #sample_id = experiment_id+'.'
        sampleCode = nameSplit[-1]
        if not experiment_id:
                print "The identifier matching the pattern Q\w{4}E\[0-9]+ was not found in the fileName "+name

        ss = transaction.getSearchService()

        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, sampleCode))
        foundSamples = ss.searchForSamples(sc)
        samplehit = foundSamples[0]
        sample = transaction.getSampleForUpdate(samplehit.getSampleIdentifier())

        experiment = transaction.getExperimentForUpdate("/"+space+"/"+project+"/"+experiment_id)
        experiment.setPropertyValue("Q_WF_STATUS", "FINISHED")

        endpoint = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
        experiment.setPropertyValue("Q_WF_FINISHED_AT", endpoint)
        sample.setExperiment(experiment)

        #Register files
        dataSetRes = transaction.createNewDataSet('Q_NGS_READ_MATCH_ARCHIVE')
        dataSetRes.setMeasuredData(False)
        dataSetLogs = transaction.createNewDataSet('Q_WF_NGS_16S_TAXONOMIC_PROFILING_LOGS')
        dataSetLogs.setMeasuredData(False)

        dataSetRes.setSample(sample)
        dataSetLogs.setSample(sample)

        resultsname = incomingPath+"/"+experiment_id+"_workflow_results"
        logname = incomingPath+"/"+experiment_id+"_workflow_logs"
        os.rename(incomingPath+"/logs", logname)
        os.rename(incomingPath+"/result", resultsname)

        transaction.moveFile(resultsname, dataSetRes)
        transaction.moveFile(logname, dataSetLogs)
