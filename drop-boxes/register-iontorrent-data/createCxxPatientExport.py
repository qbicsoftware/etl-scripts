import os,sys

sys.path.append('/home-link/qeana10/openbis/servers/core-plugins/QBIC/1/dss/drop-boxes/register-iontorrent-data')

import vcf2xml



variantsWhitelist = vcf2xml.loadVariantsWhitelistFile(sys.argv[2])
vcfData = vcf2xml.loadGeneVariantsFromFile(sys.argv[1])

filteredGeneList = vcf2xml.matchVariantsToQBiCPanel(vcfData, variantsWhitelist)

# use qbic patient id (arg2), sample id (arg3), and target folder (arg4) here as parameters
patientID = sys.argv[3]
sampleID = sys.argv[4]
patientMPI = sys.argv[5]
creationTimeStamp = sys.argv[6]
panelName = sys.argv[7]

xmlOutputString = vcf2xml.createPatientExport(filteredGeneList, patientID, sampleID, patientMPI, creationTimeStamp, panelName)

#cvXMLoutput = vcfxml.create

#targetDir = sys.argv[8]
#targetFilename = patientID + '-' + sampleID + '-Cxx-export.xml'
targetFilename = sys.argv[8]
xmloutfile = open(targetFilename, 'w')
xmloutfile.write(xmlOutputString)
xmloutfile.close()
