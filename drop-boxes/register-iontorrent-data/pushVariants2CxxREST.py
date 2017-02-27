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




def checkRESTinterface():
    resp = requests.get('https://134.2.189.251:5088/centraxx/rest/info')
    print resp


loadConfigFile()

print authData
checkRESTinterface()
