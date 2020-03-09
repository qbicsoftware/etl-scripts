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

######## Sample Tracking related import
from life.qbic.sampletracking import SampleTracker
from life.qbic.sampletracking import ServiceCredentials
from java.net import URL

import sample_tracking_helper_qbic as tracking_helper

#### Setup Sample Tracking service
serviceCredentials = ServiceCredentials()
serviceCredentials.user = tracking_helper.get_service_user()
serviceCredentials.password = tracking_helper.get_service_password()
serviceUrl = URL(tracking_helper.get_service_reg_url())
qbicLocation = tracking_helper.get_qbic_location_json()

sampleTracker = SampleTracker.createQBiCSampleTracker(serviceUrl, serviceCredentials, qbicLocation)

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

class TestError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

def process(transaction):
        context = transaction.getRegistrationContext().getPersistentMap()

        # Get the incoming path of the transaction
        incomingPath = transaction.getIncoming().getAbsolutePath()

        key = context.get("RETRY_COUNT")
        if (key == None):
                key = 1

        # Get the name of the incoming file 
        name = transaction.getIncoming().getName()

        sampleID = "/CHICKEN_FARM/QTEST099HH"
        # Update Sample Location
        sampleTracker.updateSampleLocationToCurrentLocation(sampleID)

        raise TestError("Test if data was registered!")
        
        sample = transaction.getSampleForUpdate(sampleID)

        if not sampleID:
                sample = transaction.createNewSample(sampleID, "Q_TEST_SAMPLE")

        data = transaction.createNewDataSet('Q_TEST')
        data.setMeasuredData(False)
        data.setSample(sample)

        transaction.moveFile(incomingPath, data)
