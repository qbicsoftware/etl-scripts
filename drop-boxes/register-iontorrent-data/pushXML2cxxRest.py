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
    return response

def fetchPatientByMPI(mpiCode):
    restAuth = HTTPBasicAuth(authData['authuser'], authData['password'])
    restParams = {'psn': mpiCode, 'idType': 'mpi'}

    queryUrl = authData['serveraddr'] + '/centraxx/rest/export/decisiveId/patient'
    response = requests.get(queryUrl, params=restParams, auth=restAuth, verify=False)

    return response


def pushXML2CxxREST(filepath):
    headers = {'Content-Type': 'application/xml'}
    importUrl = authData['serveraddr'] + '/centraxx/rest/import/queue/'
    xmlContent = open(filepath, 'rb').readlines()


    print requests.post(importUrl, data=xmlContent, headers=headers).text

loadConfigFile()


pushXML2CxxREST(sys.argv[1])

#print authData
#checkRESTinterface()
#resp = fetchPatientByMPI('')

#outfile = open('result.xml', 'w')
#outfile.write(resp.text.encode('utf-8'))
#outfile.close()
