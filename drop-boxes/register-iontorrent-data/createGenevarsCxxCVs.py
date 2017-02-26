from __future__ import print_function
import os
import sys
import csv
import cxxpy as cx
import pyxb.utils.domutils
import xml.dom.minidom
from pyxb.namespace import XMLSchema_instance as xsi
from pyxb.namespace import XMLNamespaces as xmlns
#import vcf


def loadGeneVariantPanelFile(filename):
    tmpVariantPanel = {}

    with open(filename, 'rb') as panelfile:
        panelreader = csv.reader(panelfile)
        for variant in panelreader:
            if (len(variant) == 2):
                genename = variant[0].strip()
                varname = variant[1].strip()

                if not tmpVariantPanel.has_key(genename):
                    tmpVariantPanel[genename] = []

                tmpVariantPanel[genename].append(varname)

    # print(geneVariantPanel)
    return(tmpVariantPanel)

# is tailored to the finalCxxPanel4000.tsv file
def loadVariantsWhitelistFile(filename):
    tmpVariantPanel = {}

    with open(filename, 'rb') as panelfile:
        panelreader = csv.reader(panelfile, delimiter=' ')
        for variant in panelreader:
            genename = variant[0].strip()
            varname = variant[2].strip().split('.')[1]

            if not tmpVariantPanel.has_key(genename):
                tmpVariantPanel[genename] = []

            tmpVariantPanel[genename].append(varname)

    # print(geneVariantPanel)
    return(tmpVariantPanel)


# def loadGeneVariantsFromVCF(filename):
#     vcf_reader = vcf.Reader(open(filename, 'r'))
#
#     aaMapping = {'Ala': 'A', 'Arg': 'R', 'Asn': 'N',
#                  'Asp': 'D', 'Cys': 'C', 'Glu': 'E',
#                  'Gln': 'Q', 'Gly': 'G', 'His': 'H',
#                  'Ile': 'I', 'Leu': 'L', 'Lys': 'K',
#                  'Met': 'M', 'Phe': 'F', 'Pro': 'P',
#                  'Ser': 'S', 'Thr': 'T', 'Trp': 'W',
#                  'Tyr': 'Y', 'Val': 'V', '*': '*'}
#
#     #geneVarMap = {}
#     tmpLoadedGeneVars = []
#
#     for record in vcf_reader:
#         annstr = record.INFO['ANN'][0]
#         annsplit = annstr.split('|')
#
#         genename = annsplit[3].strip()
#         mutation = annsplit[10][2:].strip()
#         firstAA = mutation[0:3]
#         mutation_short = mutation.replace(firstAA, aaMapping[firstAA])
#         secAA = mutation_short[len(mutation_short) - 3:len(mutation_short)]
#         mutation_short = mutation_short.replace(secAA, aaMapping[secAA])
#         #print(genename, mutation, mutation_short)
#         # print genename, mutation_short
#
#         # if not geneVarMap.has_key(genename):
#         #     geneVarMap[genename] = []
#
#         tmpLoadedGeneVars.append((genename, mutation_short))
#
#         # print(geneVarMap)
#
#     return(tmpLoadedGeneVars)

#geneVariantPanel = loadGeneVariantPanelFile('cosmic_mutations_reduced_300.csv')

#test = loadGeneVariantsFromVCF('missense.vcf')


def writeGenePanelControlledVocabularies(geneVariantPanel):
    pyxb.utils.domutils.BindingDOMSupport.DeclareNamespace(cx.Namespace, 'cxx')

    docRoot = cx.CentraXXDataExchange()

    docRoot.Source = 'QBiC'

    # fill in the controlled CV for gene variant profiles
    catData = cx.CatalogueDataType()

    # for each gene variant create a UsageEntryCatalogueItem
    for gene, variants in geneVariantPanel.iteritems():

        # for each gene, add the general status of 'variation' or 'no variation'
        tmpCatDataItem = cx.UsageEntryType()
        tmpCatDataItem.Code = 'QBiC-' + gene + '-VARIANTPRESENT'
        tmpCatDataItem.Category = False
        tmpMultiLingua_en = cx.MultilingualEntryType(Lang = 'en', Value = 'Variants present')
        tmpCatDataItem.append(tmpMultiLingua_en)
        tmpMultiLingua_de = cx.MultilingualEntryType(Lang = 'de', Value = 'Variante(n) gefunden')
        tmpCatDataItem.append(tmpMultiLingua_de)
        catData.append(tmpCatDataItem)

        tmpCatDataItem = cx.UsageEntryType()
        tmpCatDataItem.Code = 'QBiC-' + gene + '-VARIANTABSENT'
        tmpCatDataItem.Category = False
        tmpMultiLingua_en = cx.MultilingualEntryType(Lang = 'en', Value = 'No variants present')
        tmpCatDataItem.append(tmpMultiLingua_en)
        tmpMultiLingua_de = cx.MultilingualEntryType(Lang = 'de', Value = 'Keine Variante(n) gefunden')
        tmpCatDataItem.append(tmpMultiLingua_de)
        catData.append(tmpCatDataItem)

        for v in variants:
            tmpCatDataItem = cx.UsageEntryType()
            tmpCatDataItem.Code = 'QBiC-' + gene + '-' + v
            tmpCatDataItem.Category = False
            tmpMultiLingua_en = cx.MultilingualEntryType(Lang = 'en', Value = v)
            tmpCatDataItem.append(tmpMultiLingua_en)
            tmpMultiLingua_de = cx.MultilingualEntryType(Lang = 'de', Value = v)
            tmpCatDataItem.append(tmpMultiLingua_de)

            catData.append(tmpCatDataItem)

    docRoot.CatalogueData = catData

    docRootDOM = docRoot.toDOM()
    docRootDOM.documentElement.setAttributeNS(
        xsi.uri(), 'xsi:schemaLocation', 'http://www.kairos-med.de ../CentraXXExchange.xsd')
    docRootDOM.documentElement.setAttributeNS(
        xmlns.uri(), 'xmlns:xsi', xsi.uri())

    return(docRootDOM.toprettyxml(encoding='utf-8'))


def writeMeasurementProfileDefs(geneVariantPanel):
    pyxb.utils.domutils.BindingDOMSupport.DeclareNamespace(cx.Namespace, 'cxx')

    docRoot = cx.CentraXXDataExchange()

    docRoot.Source = 'QBiC'

    # fill in the controlled CV for gene variant profiles
    catData = cx.CatalogueDataType()
    flexValues = cx.FlexibleValuesType()
    flexValues.FlexibleEnumerationValue = []
    # for each gene variant create a UsageEntryCatalogueItem
    for gene, variants in geneVariantPanel.iteritems():
        flexEnumValue = cx.FlexibleEnumerationType()
        flexEnumValue.Code = 'QBiC-' + gene
        tmpMultiLingua_en = cx.MultilingualEntryType(Lang = 'en', Value = gene)
        tmpMultiLingua_de = cx.MultilingualEntryType(Lang = 'de', Value = gene)

        flexEnumValue.NameMultilingualEntries = [tmpMultiLingua_en, tmpMultiLingua_de]


        flexEnumValue.ChoiseType = 'SELECTMANY'

        tmpVarList = []

        tmpVarList.append('QBiC-' + gene + '-VARIANTPRESENT')
        tmpVarList.append('QBiC-' + gene + '-VARIANTABSENT')

        for v in variants:
            tmpVarList.append('QBiC-' + gene + '-' + v)

        flexEnumValue.UsageEntryTypeRef = tmpVarList
        flexValues.append(flexEnumValue)
            #catData.append(tmpCatDataItem)

    catData.append(flexValues)

    geneProfile = cx.FlexibleDataSetType()
    geneProfile.Code = 'QBiCGeneProfile-v1'
    tmpMultiLingua_en = cx.MultilingualEntryType(Lang = 'en', Value = 'QBiC Gene Variant Panel v1')
    tmpMultiLingua_de = cx.MultilingualEntryType(Lang = 'de', Value = 'QBiC Gene Variant Panel v1')

    geneProfile.NameMultilingualEntries = [tmpMultiLingua_en, tmpMultiLingua_de]
    geneProfile.FlexibleValueComplexRefs = []

    #FlexibleValueComplexRefs
    for gene, variants in geneVariantPanel.iteritems():
        flexValRef = cx.FlexibleValueRefType(gene, False)

        geneProfile.FlexibleValueComplexRefs.append(flexValRef)
            #catData.append(tmpCatDataItem)

    geneProfile.FlexibleDataSetType = 'MEASUREMENT'

    try:
        catData.append(geneProfile)
    except pyxb.ValidationError as e:
        print(e.details())

    docRoot.CatalogueData = catData

    try:
        docRootDOM = docRoot.toDOM()
    except pyxb.ValidationError as e:
        print(e.details())

    docRootDOM.documentElement.setAttributeNS(
        xsi.uri(), 'xsi:schemaLocation', 'http://www.kairos-med.de ../CentraXXExchange.xsd')
    docRootDOM.documentElement.setAttributeNS(
        xmlns.uri(), 'xmlns:xsi', xsi.uri())

    return(docRootDOM.toprettyxml(encoding='utf-8'))




geneVariantPanel = loadVariantsWhitelistFile('finalCxxPanel4000.tsv')

output = writeGenePanelControlledVocabularies(geneVariantPanel)
output2 = writeMeasurementProfileDefs(geneVariantPanel)

#print(output)

xmloutfile = open('QBiCGeneProfile-profile.xml', 'w')
xmloutfile.write(output2)
xmloutfile.close()

xmloutfile = open('QBiCGeneProfile-catalog.xml', 'w')
xmloutfile.write(output)
xmloutfile.close()




#testxml = cx.CreateFromDocument(open('xml/Patient_new.xml').read())

# print(testxml.toDOM())


# try:
#     print(v.toxml())
# except pyxb.exceptions_.IncompleteElementContentError as e:
#     print(e.details())
# print(test.toxml())
