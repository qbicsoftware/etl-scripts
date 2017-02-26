import os,sys
import vcf2xml



variantsWhitelist = vcf2xml.loadVariantsWhitelistFile('finalCxxPanel4000.tsv')
vcfData = vcf2xml.loadGeneVariantsFromFile(sys.argv[1])

filteredGeneList = vcf2xml.filterGeneVariantsFromPanel(vcfData, variantsWhitelist)

# use qbic patient id (arg2), sample id (arg3), and target folder (arg4) here as parameters
patientID = sys.argv[2]
sampleID = sys.argv[3]
creationTimeStamp = sys.argv[4]

xmlOutputString = vcf2xml.createPatientExport(filteredGeneList, patientID, sampleID, creationTimeStamp)

#cvXMLoutput = vcfxml.create

targetDir = sys.argv[5]
targetFilename = patientID + '-Cxx-export.xml'
xmloutfile = open(os.path.join(targetDir, targetFilename), 'w')
xmloutfile.write(xmlOutputString)
xmloutfile.close()
