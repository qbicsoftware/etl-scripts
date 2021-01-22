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
    sampleCode = nameSplit[-1]
    sample_id = "/"+space+"/"+sampleCode
    if not experiment_id:
            print "The identifier matching the pattern Q\w{4}E\[0-9]+ was not found in the fileName "+name

    sample = transaction.getSampleForUpdate(sample_id)
 
    experiment = transaction.getExperimentForUpdate("/"+space+"/"+project+"/"+experiment_id)

    experiment.setPropertyValue("Q_WF_STATUS", "FINISHED")
    endpoint = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    experiment.setPropertyValue("Q_WF_FINISHED_AT", endpoint)
    sample.setExperiment(experiment)

    #Register files
    dataSetRes = transaction.createNewDataSet('Q_WF_EDDA_BENCHMARK_RESULTS')
    dataSetRes.setMeasuredData(False)
    dataSetLogs = transaction.createNewDataSet('Q_WF_EDDA_BENCHMARK_LOGS')
    dataSetLogs.setMeasuredData(False)

    dataSetRes.setSample(sample)
    dataSetLogs.setSample(sample)

    transaction.moveFile(incomingPath+"/result", dataSetRes)
    transaction.moveFile(incomingPath+"/logs", dataSetLogs)

