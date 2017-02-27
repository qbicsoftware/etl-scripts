import os
import sys

import requests
from requests.auth import HTTPBasicAuth
import configparser

authData = {}

def loadConfigFile():
    config = configparser.ConfigParser()

    homedir = os.path.expanduser("~")
    config.read(os.path.join(homedir, '.cxxrest/config.ini'))
    print config.sections()

    authData['authuser'] = config['CXXSETUP']['authuser']
    authData['password'] = config['CXXSETUP']['password']
    authData['serveraddr'] = config['CXXSETUP']['serveraddr']

def checkRESTinterface():
    response = requests.get(authData['serveraddr'] + '/centraxx/rest/info', verify=False)
    print response

def fetchPatientByMPI(mpiCode):
    restAuth = HTTPBasicAuth(authData['authuser'], authData['password'])
    restParams = {'psn': mpiCode, 'idType': 'mpi'}

    queryUrl = authData['serveraddr'] + '/centraxx/rest/export/decisiveId/patient'
    response = requests.get(queryUrl, params=restParams, auth=restAuth, verify=False)

    print response.json()
loadConfigFile()

#print authData
#checkRESTinterface()
fetchPatientByMPI()
