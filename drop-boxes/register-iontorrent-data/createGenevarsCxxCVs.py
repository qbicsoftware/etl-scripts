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
            tmpVarField = variant[2].strip()

            varname = 'NOVARIANT'

            if tmpVarField != 'NOVARIANT':
                varname = tmpVarField.split('.')[1]

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


def createCustomCatalogEntry(code, value_en, value_de):
    customCatalogEntry = cx.CustomCatalogEntryType()
    customCatalogEntry.Code = code
    tmpMultiLingua_en = cx.MultilingualEntryType(Lang = 'en', Value = value_en)
    tmpMultiLingua_de = cx.MultilingualEntryType(Lang = 'de', Value = value_de)
    customCatalogEntry.NameMultilingualEntries = [tmpMultiLingua_de, tmpMultiLingua_en]

    return customCatalogEntry



def writeGenePanelControlledVocabularies(geneVariantPanel):
    pyxb.utils.domutils.BindingDOMSupport.DeclareNamespace(cx.Namespace, 'cxx')

    docRoot = cx.CentraXXDataExchange()

    docRoot.Source = 'QBiC'
    qbicPrefix = 'QBIC-GENECV-'
    # fill in the controlled CV for gene variant profiles
    catData = cx.CatalogueDataType()

    # for each gene, we create an CustomCatalog/CV to specify the allowed variants
    for gene, variants in geneVariantPanel.iteritems():

        # for each gene, add the general status of 'variation' or 'no variation'
        customCatalogObject = cx.CustomCatalogType()
        customCatalogObject.Code = qbicPrefix + gene
        tmpMultiLingua_en = cx.MultilingualEntryType(Lang = 'en', Value = gene)
        tmpMultiLingua_de = cx.MultilingualEntryType(Lang = 'de', Value = gene)
        customCatalogObject.NameMultilingualEntries = [tmpMultiLingua_en, tmpMultiLingua_de]
        customCatalogObject.CatalogUsage = 'GENERAL'
        customCatalogObject.Version = 1
        customCatalogObject.EntityStatus = 'ACTIVE'

        customCatalogObject.CustomCatalogEntry = []

        customCatalogEntryObject = createCustomCatalogEntry('VARIANTPRESENT', 'Variant(s) present', 'Variante(n) gefunden')
        customCatalogObject.CustomCatalogEntry.append(customCatalogEntryObject)
        customCatalogEntryObject = createCustomCatalogEntry('VARIANTABSENT', 'No variants present', 'Keine Varianten gefunden')
        customCatalogObject.CustomCatalogEntry.append(customCatalogEntryObject)


        for v in variants:
            if v == 'NOVARIANT':
                continue

            customCatalogEntryObject = createCustomCatalogEntry(v, v, v)
            customCatalogObject.CustomCatalogEntry.append(customCatalogEntryObject)


        catData.append(customCatalogObject)

    docRoot.CatalogueData = catData

    docRootDOM = docRoot.toDOM()
    docRootDOM.documentElement.setAttributeNS(
        xsi.uri(), 'xsi:schemaLocation', 'http://www.kairos-med.de ../CentraXXExchange.xsd')
    docRootDOM.documentElement.setAttributeNS(
        xmlns.uri(), 'xmlns:xsi', xsi.uri())

    return(docRootDOM.toprettyxml(encoding='utf-8'))


def writeMeasurementParameterDefs(geneVariantPanel):
    pyxb.utils.domutils.BindingDOMSupport.DeclareNamespace(cx.Namespace, 'cxx')

    docRoot = cx.CentraXXDataExchange()

    docRoot.Source = 'QBiC'
    qbicPrefix = 'QBIC-GENEPARAM-'
    # fill in the controlled CV for gene variant profiles
    catData = cx.CatalogueDataType()

    flexValues = cx.FlexibleValuesType()
    flexValues.FlexibleCatalogValue = []
    # for each gene variant create a UsageEntryCatalogueItem
    for gene, variants in geneVariantPanel.iteritems():
        flexCatalogValue = cx.FlexibleCatalogType()
        flexCatalogValue.Code = qbicPrefix + gene
        tmpMultiLingua_en = cx.MultilingualEntryType(Lang = 'en', Value = gene + ' variants')
        tmpMultiLingua_de = cx.MultilingualEntryType(Lang = 'de', Value = gene + '-Varianten')

        flexCatalogValue.NameMultilingualEntries = [tmpMultiLingua_en, tmpMultiLingua_de]


        flexCatalogValue.ChoiseType = 'SELECTMANY'

        flexCatalogValue.UserDefinedCatalogRef = cx.UserDefinedCatalogRefType(Code = 'QBIC-GENECV-' + gene, Version = '1')
        flexValues.append(flexCatalogValue)
            #catData.append(tmpCatDataItem)

    catData.append(flexValues)



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


def writeMeasurementProfileDef(geneVariantPanel):
    pyxb.utils.domutils.BindingDOMSupport.DeclareNamespace(cx.Namespace, 'cxx')

    docRoot = cx.CentraXXDataExchange()

    docRoot.Source = 'QBiC'
    qbicPrefix = 'QBIC-GENEPARAM-'
    # fill in the controlled CV for gene variant profiles
    catData = cx.CatalogueDataType()


    geneProfile = cx.FlexibleDataSetType()
    geneProfile.Code = 'QBIC-GENEPANEL-V1'
    tmpMultiLingua_en = cx.MultilingualEntryType(Lang = 'en', Value = 'QBiC Gene Variant Panel v1')
    tmpMultiLingua_de = cx.MultilingualEntryType(Lang = 'de', Value = 'QBiC Gene Variant Panel v1')

    geneProfile.NameMultilingualEntries = [tmpMultiLingua_en, tmpMultiLingua_de]
    geneProfile.FlexibleValueComplexRefs = []

    #FlexibleValueComplexRefs
    for gene, variants in geneVariantPanel.iteritems():
        flexValRef = cx.FlexibleValueRefType(qbicPrefix + gene, False)

        geneProfile.FlexibleValueComplexRefs.append(flexValRef)
            #catData.append(tmpCatDataItem)

    geneProfile.FlexibleDataSetType = 'MEASUREMENT'
    geneProfile.Systemwide = True
    geneProfile.Category = 'LABOR'

    crfTemplateObject = cx.CrfTemplateType()
    crfTemplateObject.Name = 'QBiC Genpanel v1'
    crfTemplateSection = cx.CrfTemplateSectionType()
    crfTemplateSection.Name = 'Gene und zugehoerige Varianten'
    crfTemplateSection.CrfTemplateField = []


    rowNumber = 0
    for gene, variants in geneVariantPanel.iteritems():
        crfTemplateField = cx.CrfTemplateFieldType()
        crfTemplateField.LaborValue = qbicPrefix + gene
        crfTemplateField.LowerRow = str(rowNumber)
        crfTemplateField.UpperRow = str(rowNumber)
        crfTemplateField.LowerColumn = '0'
        crfTemplateField.UpperColumn = '0'
        crfTemplateField.Mandatory = False
        crfTemplateField.VisibleCaption = True
        crfTemplateField.FieldType = 'LABORVALUE'

        crfTemplateField.CustomCatalogEntryDefaultValueRef = ['VARIANTPRESENT', 'VARIANTABSENT']
        for v in variants:
            if v == 'NOVARIANT':
                continue

            crfTemplateField.CustomCatalogEntryDefaultValueRef.append(v)

        crfTemplateSection.CrfTemplateField.append(crfTemplateField)

        rowNumber += 1

    crfTemplateObject.CrfTemplateSection = [crfTemplateSection]

    crfTemplateObject.FlexibleDataSetRef = 'QBIC-GENEPANEL-V1'
    crfTemplateObject.TemplateType = 'LABORMETHOD'
    crfTemplateObject.Version = '0'
    crfTemplateObject.EntityStatus = 'ACTIVE'
    crfTemplateObject.Global = False
    crfTemplateObject.MultipleUse = False
    crfTemplateObject.Active = False


    try:
        catData.append(geneProfile)
    except pyxb.ValidationError as e:
        print(e.details())

    catData.append(crfTemplateObject)

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
#print (len(geneVariantPanel))
output = writeGenePanelControlledVocabularies(geneVariantPanel)
output2 = writeMeasurementParameterDefs(geneVariantPanel)
output3 = writeMeasurementProfileDef(geneVariantPanel)

#print(output)

xmloutfile = open('QBiCGeneProfile-params-v2.xml', 'w')
xmloutfile.write(output2)
xmloutfile.close()

xmloutfile = open('QBiCGeneProfile-catalog-v2.xml', 'w')
xmloutfile.write(output)
xmloutfile.close()
xmloutfile = open('QBiCGeneProfile-profile-v2.xml', 'w')
xmloutfile.write(output3)
xmloutfile.close()



#testxml = cx.CreateFromDocument(open('xml/Patient_new.xml').read())

# print(testxml.toDOM())


# try:
#     print(v.toxml())
# except pyxb.exceptions_.IncompleteElementContentError as e:
#     print(e.details())
# print(test.toxml())
