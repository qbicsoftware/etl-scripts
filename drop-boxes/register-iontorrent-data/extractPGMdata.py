import os
import sys
import glob
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

        if varType == 'SNP' and alleleCall == 'Heterozygous' and ((alleleFreq > 5.0 and alleleFreq < 35.0) or (alleleFreq > 65.0 and alleleFreq < 0.85)):
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

    print xlsVarDict


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



            for ann in annField:
                annDict = mangleSnpEffAnnotationString(ann)

                annAllele = annDict['allele']
                blackList = []

                if annAllele == xlsAltBase:
                    genename = annDict['gene_name']
                    dnaChange = annDict['HGVS_c']
                    aaChange = annDict['HGVS_p']
                    combinedChange = '|'.join([genename, dnaChange, aaChange])
                    print combinedChange

                    if dnaChange != '' and aaChange != '' and combinedChange not in blackList:
                        print xlsRow, vcfRecord.CHROM, vcfRecord.POS, refBase, altBase, genename, annAllele, dnaChange, aaChange
                        blackList.append(combinedChange)
            #for vcfRow, vcfRecord in vcfVarDict.iteritems():
            #v.INFO['ANN']
    # vcfFile = open(vcfFilename, 'r')
    # vcfLines = vcfFile.readlines()
    # vcfFile.close()



    #localDict = defaultdict(int)
    #localDict = {}

    # xlsFilename = filename + '.xls'
    # filenameTmp = filename.split('/')[1]
    # filenameTmp = filenameTmp.replace('alleles', 'TSVC_variants')

    #vcfFilename = glob.glob('results/' + filenameTmp + '*')[0]

    #print vcfFilename
    #variantsDF = read_csv(xlsFilename, index_col=[0,1,2,3], sep='\t')

    #if variantsDF.empty:
    #    return localDict

    #print variantsDF
    #vcfDF = read_csv(vcfFilename, sep='\t', comment='#', header=None, index_col=[0,1])



    #print variantsDF

    #chromosome = variantsDF['Chrom']
    #alleleCall = variantsDF['Allele Call']
    #alleleSource = variantsDF['Allele Source']
    #alleleFrequency = variantsDF['Frequency']
    #alleleCoverage = variantsDF['Allele Cov']


    #print sortedVariantsDF['HotSpot ID']

#     for ix, row in variantsDF.iterrows():
#         if blockingDict.has_key(ix):
#             continue
#         #print ix, row
#         #print "!!!" + str(row['Var Cov'])
#         #cosmicID = row['Allele Name']
#
#         #print ix, row['Frequency'], cosmicID, row['Allele Cov']
#         #print ix, '---',
#
#         refBase = ix[2].strip()
#         altBase = ix[3].strip()
#
#         try:
#             rowList = vcfDF.ix[ix[0:2]].tolist()
#
#         except:
#             #print 'Warning: Could not find ',
#             #print ix[0:2],
#             #print ' in ' + vcfFilename
#             continue
#
#
#
#         vcfRowReconstructed = '\t'.join(map(str, ix[0:2])) + '\t' + '\t'.join(map(str, rowList))
#
#         tmpFile = open('tmpFile', 'w')
#         #print headerContent
#         tmpFile.write(headerContent)
#         tmpFile.write(vcfRowReconstructed + '\n')
#         tmpFile.close()
#
#         tmpFile = open('tmpFile', 'r')
#
#
#         vcf_reader = vcf.Reader(tmpFile)
#
#         for record in vcf_reader:
#             #print record
#             #print refBase, altBase, record.REF, record.ALT
#             #print refBase == record.REF, record.ALT.index(altBase)
#             #if not refBase == record.REF:
#             #    continue
#
#             #altBaseIndex = record.ALT.index(altBase)
#             altBaseIndex = 0
#             annstr = record.INFO['ANN'][altBaseIndex]
#             annsplit = annstr.split('|')
#
#             #effect = annsplit[1].strip()
#             #impact = annsplit[2].strip()
#             genename = annsplit[3].strip()
#             #mutation = annsplit[9].strip()
#
#             #print genename, mutation
#             localDict[genename] = 1
#             blockingDict[ix] = 1
#
#
#             # if impact != 'HIGH' and impact != 'MODERATE':
#             #     continue
#             #
#             #
#             # #print record.INFO
#             #
#             # #print genename, mutation, impact, effect,
#             #
#             # lof = ''
#             # nmd = ''
#             # metasvm = ''
#             # lrt = ''
#             #
#             # if record.INFO.has_key('LOF'):
#             #     lof = record.INFO['LOF'][altBaseIndex]
#             #
#             # if record.INFO.has_key('NMD'):
#             #     nmd = record.INFO['NMD'][altBaseIndex]
#             #
#             # if record.INFO.has_key('dbNSFP_MetaSVM_pred'):
#             #     metasvm = record.INFO['dbNSFP_MetaSVM_pred'][altBaseIndex]
#             #
#             # if record.INFO.has_key('dbNSFP_MetaSVM_pred'):
#             #     lrt = record.INFO['dbNSFP_MetaSVM_pred'][altBaseIndex]
#             #
#             # if metasvm == 'D' and lrt == 'D':
#             #     key = genename + '_' + mutation
#             #     localDict[key] = 1
#         tmpFile.close()
#
#
#
#     return localDict
#
#
# infile = open(sys.argv[1], 'r')
# inlines = infile.readlines()
#
# hotspotNum = 0
# missingNumber = 0
#
# fileCounter = 0
#
# for i in inlines:
#     sys.stderr.write(str(fileCounter) + ' of ' + str(len(inlines)) + '\n')
#     fileStats = loadGeneVariantsFromVCF(i.strip())
#     fileCounter += 1
#
#     for k in fileStats.keys():
#         histDict[k] += 1

# for k in sorted(panelStatsDict, key=panelStatsDict.get, reverse=True):
#     print k, '\t', panelStatsDict[k]
#
# print
# print
#

extractPGMdata(sys.argv[1], sys.argv[2])

# print len(histDict)
#
# for key in sorted(histDict, key=histDict.get, reverse=True):
#     if histDict[key] > 1:
#         print key, '\t', histDict[key]
