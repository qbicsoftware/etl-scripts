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

# Data import and registration
# expected:
# *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
ePattern = re.compile('Q\w{4}E[0-9]+')
pPattern = re.compile('Q\w{4}')

# Fix - in space issue
# Assumes that there are no dashes in samples/experiments/project codes

# project = pPattern.findall(name)[0]
#experiment_id = ePattern.findall(name)[0]
# ICGC-DATA-QICGC-QICGCE13-QICGCE13R1
# Assumes that there are no dashes in samples/experiments/project codes
# space - check number of '-' if more than usual combine first and second split
#

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
    space_combination = ""
    if(len(nameSplit) > 4):
        for i in range(0,len(nameSplit) - 3):
            space_combination += nameSplit[i] + '-'
        space = space_combination[:-1]
    else:
        space = nameSplit[0]
    
    #space = nameSplit[0]
    #project = pPattern.findall(nameSplit[1])[0]
    #experiment_id = ePattern.findall(nameSplit[2])[0]
    project = pPattern.findall(name)[0]
    experiment_id = ePattern.findall(name)[0]
    sampleCode = nameSplit[-1]
    sample_id = "/"+space+"/"+sampleCode
    if not experiment_id:
            print "The identifier matching the pattern Q\w{4}E\[0-9]+ was not found in the fileName "+name

    sample = transaction.getSampleForUpdate(sample_id)

    parents = sample.getParentSampleIdentifiers()
    parentcodes = []
    for parent in parents:
        parentcodes.append(parent.split("/")[-1])
    parentInfos = "_".join(parentcodes)

    experiment = transaction.getExperimentForUpdate("/"+space+"/"+project+"/"+experiment_id)

    experiment.setPropertyValue("Q_WF_STATUS", "FINISHED")
    endpoint = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')
    experiment.setPropertyValue("Q_WF_FINISHED_AT", endpoint)
    sample.setExperiment(experiment)

    #for root, subFolders, files in os.walk(os.path.join(incomingPath, name)):
    for root, subFolders, files in os.walk(incomingPath):
	if subFolders:
		subFolder = subFolders[0]
	for f in files:
                    if 'result.tsv' in f:
                            resultFile = open(os.path.join(root, f), 'r')

    resultFile.readline()
    resultLine = resultFile.readline().split('\t')[1:-2]

    formattedResult = '\n'.join(resultLine)
    sample.setPropertyValue("Q_HLA_TYPING", formattedResult)

    newResultFile = open(os.path.join(os.path.join(incomingPath, "result/" + subFolder), sampleCode + "_alleles.alleles"), 'w')
    newResultFile.write(formattedResult)

    newResultFile.close()

    #Register files
    dataSetRes = transaction.createNewDataSet('Q_WF_NGS_HLATYPING_RESULTS')
    dataSetRes.setMeasuredData(False)
    dataSetLogs = transaction.createNewDataSet('Q_WF_NGS_HLATYPING_LOGS')
    dataSetLogs.setMeasuredData(False)

    dataSetRes.setSample(sample)
    dataSetLogs.setSample(sample)

    resultsname = incomingPath+"/"+parentInfos+"_workflow_results"
    logname = incomingPath+"/"+parentInfos+"_workflow_logs"
    os.rename(incomingPath+"/logs", logname)
    os.rename(os.path.join(incomingPath, "result/" + subFolder), resultsname)

    transaction.moveFile(resultsname, dataSetRes)
    transaction.moveFile(logname, dataSetLogs)

