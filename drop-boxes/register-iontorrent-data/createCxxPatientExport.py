import os,sys
import codecs

sys.path.append('/home-link/qeana10/openbis/servers/core-plugins/QBIC/1/dss/drop-boxes/register-iontorrent-data')

import vcf2xml



variantsWhitelist = vcf2xml.loadVariantsWhitelistFile(sys.argv[2])
vcfData = vcf2xml.loadGeneVariantsFromFile(sys.argv[1])

filteredGeneList = vcf2xml.matchVariantsToQBiCPanel(vcfData, variantsWhitelist)

# use qbic patient id (arg2), sample id (arg3), and target folder (arg4) here as parameters
patientID = sys.argv[3]
sampleID = sys.argv[4]
patientMPI = sys.argv[5]
pgmSampleID = sys.argv[6]
creationTimeStamp = sys.argv[7]
panelName = sys.argv[8]

xmlOutputString = vcf2xml.createPatientExport(filteredGeneList, patientID, sampleID, patientMPI, pgmSampleID, creationTimeStamp, panelName)

#cvXMLoutput = vcfxml.create

#targetDir = sys.argv[8]
#targetFilename = patientID + '-' + sampleID + '-Cxx-export.xml'
targetFilename = sys.argv[9]

# TODO: check if we need utf8 encoding here
#xmlOutputStringUnicode = unicode(xmlOutputString, 'utf8')
with codecs.open(targetFilename, 'w', encoding='utf-8') as f:
    f.write(xmlOutputString)
