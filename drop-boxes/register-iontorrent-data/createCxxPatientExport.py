import os,sys

sys.path.append('/home-link/qeana10/openbis/servers/core-plugins/QBIC/1/dss/drop-boxes/register-iontorrent-data')

import vcf2xml



variantsWhitelist = vcf2xml.loadVariantsWhitelistFile(sys.argv[2])
vcfData = vcf2xml.loadGeneVariantsFromFile(sys.argv[1])

filteredGeneList = vcf2xml.matchVariantsToQBiCPanel(vcfData, variantsWhitelist)

# use qbic patient id (arg2), sample id (arg3), and target folder (arg4) here as parameters
patientID = sys.argv[3]
sampleID = sys.argv[4]
creationTimeStamp = sys.argv[5]
panelName = sys.argv[6]

xmlOutputString = vcf2xml.createPatientExport(filteredGeneList, patientID, sampleID, creationTimeStamp, panelName)

#cvXMLoutput = vcfxml.create

targetDir = sys.argv[7]
targetFilename = patientID + '-' + sampleID + '-Cxx-export.xml'
xmloutfile = open(os.path.join(targetDir, targetFilename), 'w')
xmloutfile.write(xmlOutputString)
xmloutfile.close()
