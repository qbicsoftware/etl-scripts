from __future__ import print_function
import os
import sys

sys.path.append('/home-link/qeana10/openbis/servers/core-plugins/QBIC/1/dss/drop-boxes/register-iontorrent-data')


import csv
import datetime
from dateutil import parser
import pytz
import cxxpy as cx
import pyxb.utils.domutils
import xml.dom.minidom
from xml.dom.minidom import getDOMImplementation
from pyxb.namespace import XMLSchema_instance as xsi
from pyxb.namespace import XMLNamespaces as xmlns
from pyxb import exceptions_

from collections import defaultdict

#import vcf
import re
import uuid

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


def loadGeneVariantsFromFile(filename):
    #vcf_reader = vcf.Reader(open(filename, 'r'))
    varFile = open(filename, 'r')

    aaMapping = {'Ala': 'A', 'Arg': 'R', 'Asn': 'N',
                 'Asp': 'D', 'Cys': 'C', 'Glu': 'E',
                 'Gln': 'Q', 'Gly': 'G', 'His': 'H',
                 'Ile': 'I', 'Leu': 'L', 'Lys': 'K',
                 'Met': 'M', 'Phe': 'F', 'Pro': 'P',
                 'Ser': 'S', 'Thr': 'T', 'Trp': 'W',
                 'Tyr': 'Y', 'Val': 'V', '*': '*', '?': '?'}

    #geneVarMap = {}
    #tmpLoadedGeneVars = []
    tmpLoadedGeneVars = defaultdict(list)

    regex_splitAA = re.compile("([a-zA-Z\*\?]+)([0-9]+)([a-zA-Z\*\?]+)")

    for line in varFile:
        linesplit = line.split('\t')

        # extract annotated genename
        genename = linesplit[0].strip()
        mutation = linesplit[1].strip()

        #print(genename, " ", mutation, record.FILTER)
        #firstAA = mutation[0:3]
        #mutation_short = mutation.replace(firstAA, aaMapping[firstAA])
        # print(mutation_short)

        if mutation == 'VARIANTABSENT':
            tmpLoadedGeneVars[genename].append(mutation)
        else:
            mutation_str = mutation[2:].strip()
            mutation_split = re.match(regex_splitAA, mutation_str).groups()

            # check if we have three substring, otherwise the regex split was
            # not successful
            if (len(mutation_split) < 3):
                print(
                    "[LOAD_VCF] WARNING: Could not split AA mutation string correctly.")
                continue

            firstAA = mutation_split[0]
            secAA = mutation_split[2]
            mutation_short = mutation_str.replace(firstAA, aaMapping[firstAA])
            mutation_short = mutation_short.replace(secAA, aaMapping[secAA])

            tmpLoadedGeneVars[genename].append(mutation_short)

        #print(mutation_str + ": " + mutation_short)

    return(tmpLoadedGeneVars)


# def loadGeneVariantsFromVCF(filename):
#     vcf_reader = vcf.Reader(open(filename, 'r'))
#
#     aaMapping = {'Ala': 'A', 'Arg': 'R', 'Asn': 'N',
#                  'Asp': 'D', 'Cys': 'C', 'Glu': 'E',
#                  'Gln': 'Q', 'Gly': 'G', 'His': 'H',
#                  'Ile': 'I', 'Leu': 'L', 'Lys': 'K',
#                  'Met': 'M', 'Phe': 'F', 'Pro': 'P',
#                  'Ser': 'S', 'Thr': 'T', 'Trp': 'W',
#                  'Tyr': 'Y', 'Val': 'V', '*': '*', '?': '?'}
#
#     #geneVarMap = {}
#     #tmpLoadedGeneVars = []
#     tmpLoadedGeneVars = defaultdict(list)
#
#     regex_splitAA = re.compile("([a-zA-Z\*\?]+)([0-9]+)([a-zA-Z\*\?]+)")
#
#     for record in vcf_reader:
#
#         if (record.var_type == 'snp'):
#             annstr = record.INFO['ANN'][0]
#
#             # print(annstr)
#             annsplit = annstr.split('|')
#
#             if not any("p." in s for s in annsplit) or 'synonymous_variant' in annsplit:
#                 continue
#
#             # extract annotated genename
#             genename = annsplit[3].strip()
#
#             mutation = filter(lambda x: 'p.' in x, annsplit)
#             #print(genename, " ", mutation, record.FILTER)
#             #firstAA = mutation[0:3]
#             #mutation_short = mutation.replace(firstAA, aaMapping[firstAA])
#             # print(mutation_short)
#             mutation_str = mutation[0][2:].strip()
#             mutation_split = re.match(regex_splitAA, mutation_str).groups()
#
#             # check if we have three substring, otherwise the regex split was
#             # not successful
#             if (len(mutation_split) < 3):
#                 print(
#                     "[LOAD_VCF] WARNING: Could not split AA mutation string correctly.")
#                 continue
#
#             firstAA = mutation_split[0]
#             secAA = mutation_split[2]
#             mutation_short = mutation_str.replace(firstAA, aaMapping[firstAA])
#             mutation_short = mutation_short.replace(secAA, aaMapping[secAA])
#
#             tmpLoadedGeneVars[genename].append(mutation_short)
#
#             #print(mutation_str + ": " + mutation_short)
#
#     return(tmpLoadedGeneVars)


#test = loadGeneVariantsFromVCF('missense.vcf')

# def filterGeneVariantsFromPanel(vcfData, panelData):
#     #filteredGeneVariants = defaultdict(list)
#     filteredGeneVariants = {}
#
#     for gene, variants in panelData.iteritems():
#         if gene in vcfData:
#             overlap = set(vcfData[gene]).intersection(variants)
#
#             if len(overlap) > 0:
#                 #print(gene, overlap)
#                 filteredGeneVariants[gene] = list(overlap)
#
#     return(filteredGeneVariants)

def matchVariantsToQBiCPanel(vcfData, panelData):
    #filteredGeneVariants = defaultdict(list)
    filteredGeneVariants = {}

    for gene, vcfVariants in vcfData.iteritems():
        if 'VARIANTABSENT' in vcfVariants:
            filteredGeneVariants[gene] = ['VARIANTABSENT']
        elif panelData.has_key(gene):
            overlap = set(vcfData[gene]).intersection(panelData[gene])

            if len(overlap) > 0:
                #print(gene, overlap)
                filteredGeneVariants[gene] = list(overlap)
                filteredGeneVariants[gene].append('VARIANTPRESENT')
            else:
                # variant found in vcf but not part of our variant mask
                filteredGeneVariants[gene] = ['VARIANTPRESENT']

    return(filteredGeneVariants)

# def writeGenePanelControlledVocabularies(geneVariantPanel):
#     pyxb.utils.domutils.BindingDOMSupport.DeclareNamespace(cx.Namespace, 'cxx')
#
#     docRoot = cx.CentraXXDataExchange()
#
#     docRoot.Source = 'QBiC'
#
#     # fill in the controlled CV for gene variant profiles
#     catData = cx.CatalogueDataType()
#
#     # for each gene variant create a UsageEntryCatalogueItem
#     for gene, variants in geneVariantPanel.iteritems():
#
#         for v in variants:
#             tmpCatDataItem = cx.UsageEntryType()
#             tmpCatDataItem.Code = v
#             tmpCatDataItem.Category = False
#             tmpMultiLingua_en = cx.MultilingualEntryType()
#             tmpMultiLingua_en.Lang = 'en'
#             tmpMultiLingua_en.Value = v
#             tmpCatDataItem.append(tmpMultiLingua_en)
#
#             tmpMultiLingua_de = cx.MultilingualEntryType()
#             tmpMultiLingua_de.Lang = 'de'
#             tmpMultiLingua_de.Value = v
#             tmpCatDataItem.append(tmpMultiLingua_de)
#
#             catData.append(tmpCatDataItem)
#
#     docRoot.CatalogueData = catData
#
#     docRootDOM = docRoot.toDOM()
#     docRootDOM.documentElement.setAttributeNS(
#         xsi.uri(), 'xsi:schemaLocation', 'http://www.kairos-med.de ../CentraXXExchange.xsd')
#     docRootDOM.documentElement.setAttributeNS(
#         xmlns.uri(), 'xmlns:xsi', xsi.uri())
#
#     return(docRootDOM.toprettyxml(encoding='utf-8'))


def createPatientExport(vcfPanel, qPatientID, qSampleID, patientMPI, pgmSampleID, creationTimeStamp = '1970-01-01T11:59:59', panelName = 'Unknown gene panel'):
    pyxb.utils.domutils.BindingDOMSupport.DeclareNamespace(cx.Namespace, 'cxx')

    docRoot = cx.CentraXXDataExchange()

    docRoot.Source = 'XMLIMPORT'

    effData = cx.EffectDataType()

    # fill in the controlled CV for gene variant profiles
    #catData = cx.CatalogueDataType()

    # for each gene variant create a UsageEntryCatalogueItem

    #docRoot.CatalogueData = catData

    patientData = cx.PatientDataSetType()
    patientData.Source = 'XMLIMPORT'

    patientIDcontainer = cx.IDContainerType()
    patientFlexID = cx.FlexibleIDType('MPI', patientMPI)
    patientIDcontainer.append(patientFlexID)

    patientFlexID = cx.FlexibleIDType('QBIC_PAT_ID', qPatientID)
    patientIDcontainer.append(patientFlexID)

    patientData.IDContainer = patientIDcontainer

    masterData = cx.PatientMasterdataType()
    patientData.Masterdata = masterData

    # container for samples (MasterSample)
    sampleData = cx.SampleDataType()
    newMasterSample = cx.MasterSampleType()

    newMasterSample.Source = 'XMLIMPORT'

    sampleIDcontainer = cx.SampleIDContainerType()

    sampleFlexID = cx.FlexibleIDType('SAMPLEID', pgmSampleID)
    sampleIDcontainer.append(sampleFlexID)

    sampleFlexID = cx.FlexibleIDType('QBIC_SAMPLE_ID', qSampleID)
    sampleIDcontainer.append(sampleFlexID)

    newMasterSample.SampleIDContainer = sampleIDcontainer

    newMasterSample.SampleTypeCatalogueTypeRef = 'UNKN'
    newMasterSample.OrganisationUnitTypeRef = 'QBIC'
    newMasterSample.SampleReceptacleTypeRef = 'KRYO'
    newMasterSample.HasChildren = False

    newMasterSample.AmountRest = cx.VolumeType(1.0, cx.AmountUnitEnumType.PC)
    newMasterSample.InitialAmount = cx.VolumeType(
        1.0, cx.AmountUnitEnumType.PC)
    newMasterSample.SampleKind = cx.SampleKindEnumType.TISSUE
    newMasterSample.SampleLocationRef = 'QBIC_STORAGE'
    newMasterSample.UseSPREC = False
    newMasterSample.VirtualPatient = False
    newMasterSample.XPosition = 0
    newMasterSample.YPosition = 0

    # todo was fixed... we have now the timestamp of the PGM vcf in here
    samplingDate = cx.DateType()
    # pytz.timezone('Europe/Berlin')
    #currDateTime = datetime.datetime.now()
    #print(creationTimeStamp)
    dateTimeObject = parser.parse(creationTimeStamp)
    #timeFormat = '%Y-%m-%dT%H:%M:%S%z'
    samplingDate.Date = dateTimeObject.isoformat()
    samplingDate.Precision = cx.DatePrecision.EXACT
    newMasterSample.SamplingDate = samplingDate
    newMasterSample.RepositionDate = samplingDate
    newMasterSample.FirstRepositionDate = samplingDate

    newMasterSample.SopDeviation = False

    # here we generate a unique id for cross-linking flexible datasets
    sampleCrossLink = qSampleID + '-' + str(uuid.uuid4())
    newMasterSample.FlexibleDataSetRef = [sampleCrossLink]

    # Sample aus welchem Tissue/Organ?
    #newMasterSample.OrganSampleRef = 'Organ'


    patientData.OrganisationUnitRefs.append('QBIC')

    try:
        sampleData.append(newMasterSample)
    except pyxb.ValidationError as e:
        print(e.details())

    try:
        patientData.SampleData = sampleData
    except pyxb.ValidationError as e:
        print(e.details())

    try:
        effData.append(patientData)
    except pyxb.ValidationError as e:
        print(e.details())


    # Here, we write the actual variant data
    variantDataSet = cx.FlexibleDataSetInstanceType()
    #variantDataSet.Source = 'QBIC'
    variantDataSet.FlexibleDataSetTypeRef = 'QBIC-GENEPANEL-V1'
    variantDataSet.InstanceName = panelName


    variantDataSet.Date = samplingDate
    #variantDataSet.Date.Date = dateTimeObject.isoformat()
    #variantDataSet.Date.Precision = cx.DatePrecision.EXACT
    variantDataSet.Code = 'QBIC-GENEPANEL-V1-INSTANCE-' + qSampleID
    variantDataSet.FlexibleDataSetInstanceRef = sampleCrossLink
    # loop over gene variants in vcfPanel and write the corresponding XML elements
    enumValues = []

    for gene, muts in vcfPanel.iteritems():
        flexValue = cx.FlexibleEnumerationDataType()
        flexValue.FlexibleValueTypeRef = 'QBIC-GENEPARAM-' + gene
        # TODO: need to check here if referenced by code or value
        flexValue.UserDefinedCatalogEntryRef = []

        for mut in muts:
            flexValue.UserDefinedCatalogEntryRef.append(mut)

        enumValues.append(flexValue)



    variantDataSet.EnumerationValue = enumValues
    # try:
    #     variantDataSet.append(flexValues)
    # except pyxb.ValidationError as e:
    #     print("APPEND\n" + e.details())

    try:
        effData.append(variantDataSet)
    except pyxb.ValidationError as e:
        print(e.details())

    try:
        docRoot.EffectData = effData
    except pyxb.IncompleteElementContentError as e:
        print(e.details())


    # initialize docRootDOM with an empty document
    domimpl = getDOMImplementation()
    docRootDOM = domimpl.createDocument(None, "init", None)

    try:
        docRootDOM = docRoot.toDOM()
    except pyxb.IncompleteElementContentError as e:
        print(e.details())

    docRootDOM.documentElement.setAttributeNS(
        xsi.uri(), 'xsi:schemaLocation', 'http://www.kairos-med.de ../CentraXXExchange.xsd')
    docRootDOM.documentElement.setAttributeNS(
        xmlns.uri(), 'xmlns:xsi', xsi.uri())

    return(docRootDOM.toprettyxml(encoding='utf-8'))
