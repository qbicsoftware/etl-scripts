import os
import sys

import requests
from requests.auth import HTTPBasicAuth
import configparser
import io


authData = {}

def loadConfigFile():
    config = configparser.ConfigParser()

    homedir = os.path.expanduser("~")
    config.read(os.path.join(homedir, '.cxxrest/config.ini'))
    #print config.sections()

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
    filename = os.path.basename(filepath.strip())
    importUrl = authData['serveraddr'] + '/centraxx/rest/import/queue/' + filename
    restAuth = HTTPBasicAuth(authData['authuser'], authData['password'])
    headers = {'Content-Type': 'application/xml'}
    #files = {'file': io.open(filepath, 'r', encoding='utf8')}
    xmlContent = open(filepath, 'rb')

    response = requests.post(importUrl, data=xmlContent, auth=restAuth, headers=headers, verify=False)

    return response

def fetchImportedXML(filepath):
    filename = os.path.basename(filepath.strip())
    importUrl = authData['serveraddr'] + '/centraxx/rest/import/queue/' + filename
    restAuth = HTTPBasicAuth(authData['authuser'], authData['password'])
    headers = {'Content-Type': 'application/xml'}

    response = requests.get(importUrl, auth=restAuth, headers=headers, verify=False)

    return response

def triggerCxxImport(filepath):
    filename = os.path.basename(filepath.strip())
    importUrl = authData['serveraddr'] + '/centraxx/rest/import/queue/' + filename + '/start'
    restAuth = HTTPBasicAuth(authData['authuser'], authData['password'])
    #headers = {'Content-Type': 'application/xml'}
    headers = {}

    response = requests.post(importUrl, headers=headers, auth=restAuth, verify=False)

    return response

def triggerAllCxxImports():
    importUrl = authData['serveraddr'] + '/centraxx/rest/import/queue/start'
    restAuth = HTTPBasicAuth(authData['authuser'], authData['password'])
    #headers = {'Content-Type': 'application/xml'}
    headers = {}
    response = requests.post(importUrl, headers=headers, auth=restAuth, verify=False)

    return response

def getSuccessfulImport(filepath):
    filename = os.path.basename(filepath.strip())
    importUrl = authData['serveraddr'] + '/centraxx/rest/import/successful/' + filename
    restAuth = HTTPBasicAuth(authData['authuser'], authData['password'])

    response = requests.get(importUrl, auth=restAuth, verify=False)

    return response

def getErroneousImport(filepath):
    filename = os.path.basename(filepath.strip())
    importUrl = authData['serveraddr'] + '/centraxx/rest/import/error/' + filename
    restAuth = HTTPBasicAuth(authData['authuser'], authData['password'])

    response = requests.get(importUrl, auth=restAuth, verify=False)

    return response

def showCxxImportQueue():
    queueUrl = authData['serveraddr'] + '/centraxx/rest/import/queue'
    restAuth = HTTPBasicAuth(authData['authuser'], authData['password'])
    headers = {}
    response = requests.get(queueUrl, auth=restAuth, headers=headers, verify=False)

    return response

def deleteSuccessfulImport(filepath):
    filename = os.path.basename(filepath.strip())
    importUrl = authData['serveraddr'] + '/centraxx/rest/import/successful/' + filename
    restAuth = HTTPBasicAuth(authData['authuser'], authData['password'])

    response = requests.delete(importUrl, auth=restAuth, verify=False)

    return response




# load the username, password, server address etc.
loadConfigFile()
filepath = sys.argv[1]
filename = os.path.basename(filepath)

resp = pushXML2CxxREST(filepath)

# first, push XML file to REST as new resource
if resp.status_code != 201:
    raise ApiError('[CxxRest]: pushXML2CxxREST failed with ' + str(resp.status_code))
else:
    print '[CxxRest]:', filename, 'successfully pushed to Cxx REST service (' + str(resp.status_code) + ')'

resp = triggerAllCxxImports()

if resp.status_code != 202:
    raise ApiError('[CxxRest]: triggerAllCxxImports failed with ' + str(resp.status_code))
else:
    print '[CxxRest]: import was triggered successfully (' + str(resp.status_code) + ')'

resp = getSuccessfulImport(filepath)

if resp.status_code != 200:
    raise ApiError('[CxxRest]: getSuccessfulImport failed with ' + str(resp.status_code))
else:
    print '[CxxRest]:', filename, 'was marked as successfully imported (' + str(resp.status_code) + ')'

if resp.status_code != 200:
    raise ApiError('[CxxRest]: deleteSuccessfulImport failed with ' + str(resp.status_code))
else:
    print '[CxxRest]:', filename, 'was deleted from successful imports (' + str(resp.status_code) + ')'

if resp.status_code != 200:
    raise ApiError('[CxxRest]: getSuccessfulImport failed with ' + str(resp.status_code))
else:
    print '[CxxRest]:', filename, 'was marked as successfully imported (' + str(resp.status_code) + ')'




#
# resp = getErroneousImport(sys.argv[1])
# print 'getError: ', resp.status_code, resp.content

#print authData
#checkRESTinterface()
#resp = fetchPatientByMPI('')

#outfile = open('result.xml', 'w')
#outfile.write(resp.text.encode('utf-8'))
#outfile.close()
