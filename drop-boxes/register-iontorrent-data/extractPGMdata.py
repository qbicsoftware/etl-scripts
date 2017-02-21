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
        alleleCall = row['Allele Call']
        alleleFreq = float(row['Frequency'])

        if alleleFreq > 5.0 and alleleFreq < 35.0:
            print chrom, position, refBase, varBase, alleleCall, alleleFreq
            dictKey = chrom[3:] + ':' + position
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

def extractPGMdata(vcfFilename, xlsFilename):

    xlsVarDict = extractXLSdata(xlsFilename)
    vcfVarDict = extractVCFdata(vcfFilename)

    print xlsVarDict
    for k,v in vcfVarDict.iteritems():
        print k, v.CHROM, v.POS, v.INFO
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
