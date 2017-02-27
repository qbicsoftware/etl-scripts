import os
import sys

import requests
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
    resp = requests.get(authData['serveraddr'] + '/centraxx/rest/info', verify=False)
    print resp


loadConfigFile()

print authData
checkRESTinterface()
