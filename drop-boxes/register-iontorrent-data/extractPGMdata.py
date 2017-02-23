

import sys

sys.path.append('/home-link/qeana10/bin/PyVCF-0.6.8')

import csv
import vcf

from collections import defaultdict


histDict = defaultdict(int)
metaInfoDict = {}
#panelStatsDict = defaultdict(int)

blockingDict = {}

headerFile = open('vcfFileHeader.vcf')
headerContent = headerFile.read()
headerFile.close()

def extractXLSdata(xlsFilename):
    xlsFile = open(xlsFilename, 'r')
    csvreader = csv.DictReader(xlsFile, delimiter='\t')

    resultsDict = defaultdict(list)

    for row in csvreader:
        chrom = row['Chrom']
        position = row['VCF Position']
        refBase = row['VCF Ref']
        varBase = row['VCF Variant']
        varType = row['Type']
        alleleCall = row['Allele Call']
        alleleFreq = float(row['Frequency'])

        if varType == 'SNP' and alleleCall == 'Heterozygous' and ((alleleFreq > 5.0 and alleleFreq < 35.0) or (alleleFreq > 65.0 and alleleFreq < 85.0)):
            #print chrom, position, refBase, varBase, alleleCall, alleleFreq
            dictKey = chrom + ':' + position
            resultsDict[dictKey].append((refBase, varBase, alleleCall, alleleFreq))

    return resultsDict

def extractVCFdata(vcfFilename):
    vcfDict = {}

    vcfreader = vcf.Reader(open(vcfFilename, 'r'))

    for record in vcfreader:
        chrom = record.CHROM
        position = record.POS
        dictKey = str(chrom) + ':' + str(position)


        vcfDict[dictKey] = record

    return vcfDict

#Annotation      : T|missense_variant|MODERATE|CCT8L2|ENSG00000198445|transcript|ENST00000359963|protein_coding|1/1|c.1406G>A|p.Gly469Glu|1666/2034|1406/1674|469/557|  |
#SubField number : 1|       2        |    3   |  4   |       5       |    6     |      7        |      8       | 9 |    10   |    11     |   12    |   13    |   14  |15| 16
def mangleSnpEffAnnotationString(annstring):
    annsplit = annstring.strip().split('|')
    annotateMap = defaultdict(str)

    annotateMap['allele'] = annsplit[0]
    annotateMap['effect'] = annsplit[1]
    annotateMap['putative_impact'] = annsplit[2]
    annotateMap['gene_name'] = annsplit[3]
    annotateMap['gene_id'] = annsplit[4]
    annotateMap['feature_type'] = annsplit[5]
    annotateMap['feature_id'] = annsplit[6]
    annotateMap['transcript_biotype'] = annsplit[7]
    annotateMap['rank_vs_total'] = annsplit[8]
    annotateMap['HGVS_c'] = annsplit[9]
    annotateMap['HGVS_p'] = annsplit[10]
    annotateMap['cDNApos_vs_cDNAlen'] = annsplit[11]
    annotateMap['CDSpos_vs_CDSlen'] = annsplit[12]
    annotateMap['proteinpos_vs_proteinlen'] = annsplit[13]
    annotateMap['distance_to_feature'] = annsplit[14]
    annotateMap['errors'] = annsplit[15]

    return annotateMap



def extractPGMdata(vcfFilename, xlsFilename):

    xlsVarDict = extractXLSdata(xlsFilename)
    vcfVarDict = extractVCFdata(vcfFilename)

    #print xlsVarDict

    extractedVariants = []

    for xlsRow, xlsCoords in xlsVarDict.iteritems():
        #xlsRefBase = v[]

        for coord in xlsCoords:
            xlsRefBase = coord[0]
            xlsAltBase = coord[1]
            xlsVarCall = coord[2]
            xlsVarFreq = coord[3]

            # fetch the correct row from vcfVarDict
            vcfRecord = vcfVarDict[xlsRow]
            refBase = vcfRecord.REF
            altBase = vcfRecord.ALT
            annField = vcfRecord.INFO['ANN']
            # first check if the ref bases are identical
            if xlsRefBase != refBase or xlsAltBase not in altBase:
                continue


            blackList = []
            for ann in annField:
                annDict = mangleSnpEffAnnotationString(ann)

                annAllele = annDict['allele']


                if annAllele == xlsAltBase:
                    genename = annDict['gene_name'].strip()
                    dnaChange = annDict['HGVS_c'].strip()
                    aaChange = annDict['HGVS_p'].strip()
                    combinedChange = '|'.join([genename, dnaChange, aaChange])
                    #print combinedChange

                    if dnaChange != '' and aaChange != '' and combinedChange not in blackList:
                        #print genename, dnaChange, aaChange
                        extractedVariants.append((genename, dnaChange, aaChange, xlsVarFreq))
                        blackList.append(combinedChange)

    return(extractedVariants)



#print extractPGMdata(sys.argv[1], sys.argv[2])
