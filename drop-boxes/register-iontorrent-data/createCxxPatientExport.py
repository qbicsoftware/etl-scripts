import os,sys
import vcf2xml



geneVariantPanel = vcf2xml.loadGeneVariantPanelFile('cosmic_mutations_reduced_300.csv')
vcfData = vcf2xml.loadGeneVariantsFromVCF('patient2.vcf')

filteredGeneList = vcf2xml.filterGeneVariantsFromPanel(vcfData, geneVariantPanel)

xmlOutputString = vcf2xml.createPatientExport(filteredGeneList, 'testpatient', 'testsample')

xmloutfile = open('patient_export.xml', 'w')
xmloutfile.write(xmlOutputString)
xmloutfile.close()
