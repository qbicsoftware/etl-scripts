from __future__ import print_function
import sys
sys.path.append('/home-link/qeana10/bin/')
"""
@author: Sven Fillinger

Dropbox for data concerning the Molecular Tumor Board in Tübingen.

Basically handles to types of incoming data:
    1.) Raw data: Fastq files for tumor sample and blood
    2.) MTB archive: A ZIP archive containing 6 TSV. These contain information
        about SNVs, CNVs and metadata. The specification can be found on the mtbparser
        GitHub repository:
        https://github.com/qbicsoftware/qbic.mtbparser
        
1. Raw data
The sequencing facilities CeGaT and the human genetics department at UKT only see the QBiC barcode
of the tumor sample. However, as part of the whole MTB process, during patient registration in openBIS
an additional sample for PBMC is generated and attached as sample to the patient. It carries
an unique barcode, that is different from the tumor's one.

So in this dropbox we query the patient to whome the tumor sample barcode belongs and can access the
PBMC barcode from there. 

The incoming FASTQ file specification for CeGaT and human genetics for the MTB project (only!) is:

<QBiC-Barcode>_normal.1.fastq.gz
<QBiC-Barcode>_normal.2.fastq.gz
<QBiC-Barcode>_tumor.1.fastq.gz
<QBiC-Barcode>_tumor.2.fastq.gz


2. MTB Archive (zip)
Will be processed by qbicsoftware/qbic.mtbconverter (https://github.com/qbicsoftware/qbic.mtbconverter).
Please check the README on the GitHub repo.

Step 1 - 'convert' command: Takes the ZIP Archive and the patient ID and creates a XML file
    that is valid for the CentraXX XML Scheme.

Step 2 - 'push' command: Takes the XML and submits it to CentraXX

Note:
print statements go to: ~openbis/servers/datastore_server/log/startup_log.txt
"""
import checksum
import os
import re
import string
import sys

import ConfigParser
import logging
import tarfile

import ch.systemsx.cisd.etlserver.registrator.api.v2
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchSubCriteria
from ch.ethz.sis.openbis.generic.asapi.v3.dto.common.search import SearchResult
import ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.Sample
from ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.fetchoptions import SampleFetchOptions
from ch.ethz.sis.openbis.generic.asapi.v3 import IApplicationServerApi
from ch.systemsx.cisd.common.spring import HttpInvokerUtils
from ch.ethz.sis.openbis.generic.asapi.v3.dto.sample.search import SampleSearchCriteria

import mtbutils
from mtbutils import Counter

#############################################################################
#
# The ETL environment setup.
#
#############################################################################
# Path to checksum.py
sys.path.append('/home-link/qeana10/bin')

# String template for the CentraXX XML naming specification
CENTRAXX_XML_NAME = '{patient_id}_{sample_id}_patient_centraxx.xml'

# Regex matching the QBiC barcode naming specification
QCODE_REG = re.compile('Q\w{4}[0-9]{3}[a-zA-Z]\w')

# Regex matching the RNAseq sample file naming specification
RNASEQ_REG = re.compile(r'.*tumor_rna.[1,2]{1}.fastq.gz')

# Path to the openBIS properties file
PROPERTIES = '/etc/openbis.properties'

# Some openBIS type definitions
NGS_SAMPLE_TYPE = 'Q_NGS_SINGLE_SAMPLE_RUN'
NGS_EXP_TYPE = 'Q_NGS_MEASUREMENT'
NGS_RAW_DATA = 'Q_NGS_RAW_DATA'
MTB_SAMPLE_TYPE = 'Q_NGS_MTB_DIAGNOSIS_RUN'
MTB_EXP_TYPE = 'Q_NGS_MTB_DIAGNOSIS'
MTB_RAW_DATA = 'Q_NGS_MTB_DATA'
NGS_VARIANT_CALL = 'Q_NGS_VARIANT_CALLING'

# Experiment ID counter
EXPERIMENT_ID = 0

# Check if the mtbconverter executable is found in the system path
cmd_status = mtbutils.mtbconverter(['-h'])

if cmd_status != 0:
    raise mtbutils.MTBdropboxerror("Mtbconverter could not be loaded: " + cmd_status)

print(mtbutils.log_stardate("Mtbconverter executable found."))

config = ConfigParser.ConfigParser()
config.read(PROPERTIES)

api = HttpInvokerUtils.createServiceStub(IApplicationServerApi, config.get('openbis','url') + IApplicationServerApi.SERVICE_URL, 5000)
 
sessionToken = api.login(config.get('openbis','user'), config.get('openbis','password'))

if sessionToken:
    mtbutils.log_stardate("Successfully authenticated against the openBIS API.")
else:
    raise mtbutils.MTBdropboxerror("Could not authenticate against openBIS.")

COUNTER = Counter()

#############################################################################
#
# The ETL logic starts here.
#
#############################################################################


def process(transaction):
    """The main dropbox funtion.
    openBIS executes this function during an incoming file event.
    """
    incoming_path = transaction.getIncoming().getAbsolutePath()
    file_name = transaction.getIncoming().getName()
    print(mtbutils.log_stardate('Incoming file event: {}'.format(file_name)))
    # Iterate through the incoming path and get all files
    file_list = getfiles(incoming_path)
    tar_balls = []
    for f in file_list:
        if f.endswith(".tar"): tar_balls.append(f)
    
    for ball in tar_balls:
        print(mtbutils.log_stardate('Putative tar-archive detected: {}'.format(ball)))
        tar = tarfile.open(ball)
        tar.extractall(path=incoming_path)
        print(mtbutils.log_stardate('tar-archive extracted.'))
        tar.close()

    # Scan incoming dir again
    file_list = getfiles(incoming_path)
    
    # Determine the types of incoming files and route the process
    unknown_file_types = []
    fastqs_tumor = []
    fastqs_normal = []
    rna_seq_files = []
    vcf_files = []
    for in_file in file_list:
        if in_file.endswith('origlabfilename') or in_file.endswith('sha256sum') or 'source_dropbox.txt' in in_file:
            continue
        if RNASEQ_REG.findall(in_file):
            rna_seq_files.append(in_file)
        elif 'fastq' in in_file:
            if 'normal' in in_file:
                fastqs_normal.append(find_pbmc(in_file, transaction))
            elif 'tumor' in in_file:
                fastqs_tumor.append(in_file)
            else:
                unknown_file_types.append(in_file)
        elif in_file.endswith('vcf.gz'):
            vcf_files.append(in_file)
        elif in_file.endswith('.zip'):
            proc_mtb(in_file, transaction)
        else:
            unknown_file_types.append(in_file)

    for vcf in vcf_files:
        register_vcf(vcf, transaction)
    
    if fastqs_normal and fastqs_tumor:
        proc_fastq(fastqs_tumor, transaction)
        proc_fastq(fastqs_normal, transaction)

    if rna_seq_files:
        register_rnaseq(rna_seq_files, transaction)

    # Check, if there are files of unknown type left
    if unknown_file_types:
        for file_name in unknown_file_types:
            print(mtbutils.log_stardate('Unknown file type: {}'.format(file_name)))
       # raise mtbutils.MTBdropboxerror('We have found files that could not be processed!'
       #     'Manual intervention needed.')

    print(mtbutils.log_stardate('Processing finished.'))


def get_last_exp_id(experiments):
    """Fetches the highest experiment number from a list of experiments and returns its number."""
    exp_ids = [int(re.search(r'[E](\d*)$', oid.getExperimentIdentifier()).group(1)) for oid in experiments if re.search(r'[E](\d*)$', oid.getExperimentIdentifier())]
    exp_ids.sort()
    return exp_ids[-1]


def getNextFreeBarcode(projectcode, numberOfBarcodes):
    letters = string.ascii_uppercase
    numberOfBarcodes += 1

    currentLetter = letters[numberOfBarcodes / 999]
    currentNumber = numberOfBarcodes % 999
    code = projectcode + str(currentNumber).zfill(3) + currentLetter
    return code + checksum.checksum(code)


def register_rnaseq(rna_seq_files, transaction):
    """Registers RNAseq experiment raw data in openBIS.

    The list must contain two elements, following the naming convention ``r'.*tumor_rna[1,2]{1}.fastq.gz'``.
    Both files must additionally contain the same QBiC sample code in order to get registered.

    Args:
        rna_seq_files (list): A list with fastq files from an RNAseq experiment
        transaction (:class:DataSetRegistrationTransaction): An openBIS data set registration object

    Raises:
        MTBdropboxerror: If some of the conditions have not been fullfilled, with a text string explaining
                        the reason for the failure.
    """
    print(mtbutils.log_stardate('Registering incoming MTB RNAseq data {}'.format(rna_seq_files)))
    assert len(rna_seq_files) == 2
    file1 = os.path.basename(rna_seq_files[0])
    file2 = os.path.basename(rna_seq_files[1])
    assert len(set(QCODE_REG.findall(file1))) == 1
    assert len(set(QCODE_REG.findall(file2))) == 1
    assert QCODE_REG.findall(file1)[0] == QCODE_REG.findall(file2)[0]

    # This is the tumor dna sample barcode (type: TEST_SAMPLE)
    dna_barcode = QCODE_REG.findall(file1)[0]
    # Find the corresponding space and project
    space, project = space_and_project(dna_barcode)

    search_service = transaction.getSearchService()
    sc = SearchCriteria()
    pc = SearchCriteria()
    pc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.PROJECT, project));
    sc.addSubCriteria(SearchSubCriteria.createExperimentCriteria(pc))
    result = search_service.searchForSamples(sc)
    print("Found {} samples for project {} in space {}.".format(len(result), project, space))
    new_rna_sample_barcode = getNextFreeBarcode(project, numberOfBarcodes=len(result))

    # Now get the parent sample id (tumor sample, type: BIOLOGICAL_SAMPLE)
    tumor_dna_sample = getsample(dna_barcode, transaction)
    parent_ids = tumor_dna_sample.getParentSampleIdentifiers()
    assert len(parent_ids) == 1
    print(parent_ids)
    tumor_tissue_sample = getsample(parent_ids[0], transaction)

    # Now we have to create a new TEST_SAMPLE with sample type RNA and attach it
    # to the tumor tissue sample
    new_rna_sample = transaction.createNewSample("/{space}/{barcode}".format(
        space=space,
        barcode=new_rna_sample_barcode), "Q_TEST_SAMPLE")
    new_rna_sample.setExperiment(transaction.getSearchService().getExperiment(
        "/{space}/{project}/{project}E{number}".format(
            space=space,
            project=project,
            number=3
        )
    ))
    parent_sample_id = tumor_tissue_sample.getSampleIdentifier()
    new_rna_sample.setParentSampleIdentifiers([parent_sample_id])
    new_rna_sample.setPropertyValue('Q_SAMPLE_TYPE', 'RNA')

    # We design a new experiment and sample identifier
    experiments = transaction.getSearchService().listExperiments('/{}/{}'.format(space, project))
    last_exp_id = get_last_exp_id(experiments)
    new_exp_id = '/{space}/{project}/{project}E{number}'.format(
        space=space, project=project, number=last_exp_id + COUNTER.newId())
    new_sample_id = '/{space}/NGS{barcode}'.format(
        space=space, project=project, barcode=new_rna_sample_barcode)
    new_ngs_experiment = transaction.createNewExperiment(new_exp_id, "Q_NGS_MEASUREMENT")
    new_ngs_experiment.setPropertyValue('Q_CURRENT_STATUS', 'FINISHED')
    new_ngs_sample = transaction.createNewSample(new_sample_id, "Q_NGS_SINGLE_SAMPLE_RUN")
    new_ngs_sample.setParentSampleIdentifiers(["/{space}/{barcode}".format(space=space, barcode=new_rna_sample_barcode)])
    new_ngs_sample.setExperiment(new_ngs_experiment)

    # Create a data-set attached to the VARIANT CALL sample
    data_set = transaction.createNewDataSet("Q_NGS_RAW_DATA")
    data_set.setMeasuredData(False)
    data_set.setSample(new_ngs_sample)
    
    # Put the files in one directory
    base_path = os.path.dirname(transaction.getIncoming().getAbsolutePath())
    registration_dir = os.path.join(base_path, '{}_pairend_end_sequencing_reads'.format(new_rna_sample_barcode))
    os.mkdir(registration_dir)
    
    for raw_data in rna_seq_files:
        # replace tumor dna barcode with tumor rna barcode
        old_base = os.path.basename(raw_data)
        new_base = old_base.replace(dna_barcode, new_rna_sample_barcode)
        os.rename(raw_data, os.path.join(registration_dir, os.path.basename(new_base)))
    # Attach the directory to the dataset
    transaction.moveFile(registration_dir, data_set)


def register_vcf(in_file, transaction):
    print(mtbutils.log_stardate('Registering VCF {}'.format(in_file)))
    basename = os.path.basename(in_file)
    parent_dir = os.path.dirname(in_file)
    barcode = QCODE_REG.findall(basename)
    if not barcode:
        barcode = QCODE_REG.findall(parent_dir)
    if not barcode:
        raise mtbutils.MTBdropboxerror('No QBiC Barcode found in {}'.format(basename))
    if len(set(barcode)) > 1:
        raise mtbutils.MTBdropboxerror('More than one QBiC Barcode found in {}'.format(basename))
    space, project = space_and_project(barcode[0])
    search_service = transaction.getSearchService()

    # We design a new experiment and sample identifier
    experiments = transaction.getSearchService().listExperiments('/{}/{}'.format(space, project))
    last_exp_id = get_last_exp_id(experiments)
    new_exp_id = '/{space}/{project}/{project}E{number}'.format(
        space=space, project=project, number=last_exp_id + COUNTER.newId())
    new_sample_id = '/{space}/VC{barcode}'.format(
        space=space, project=project, barcode=barcode[0])
    print(mtbutils.log_stardate('Preparing sample and experiment creation for {sample} and {experiment}'
        .format(sample=new_sample_id, experiment=new_exp_id)))
    new_ngs_experiment = transaction.createNewExperiment(new_exp_id, NGS_VARIANT_CALL)
    new_ngs_experiment.setPropertyValue('Q_CURRENT_STATUS', 'FINISHED')
    new_ngs_sample = transaction.createNewSample(new_sample_id, NGS_VARIANT_CALL)
    new_ngs_sample.setParentSampleIdentifiers([barcode[0]])
    new_ngs_sample.setExperiment(new_ngs_experiment)

    if not barcode[0] in basename:
        parent_dir_path = os.path.dirname(in_file)
        print(parent_dir_path)
        new_path = os.path.join(parent_dir_path, '{}_{}'.format(barcode[0], basename))
        print(new_path)
        os.rename(in_file, new_path)
        in_file = new_path

    # Create a data-set attached to the VARIANT CALL sample
    data_set = transaction.createNewDataSet("Q_NGS_VARIANT_CALLING_DATA")
    data_set.setMeasuredData(False)
    data_set.setSample(new_ngs_sample)

    # Attach the directory to the dataset
    transaction.moveFile(in_file, data_set)


def find_pbmc(in_file, transaction):
    basename = os.path.basename(in_file)
    parent_dir = os.path.dirname(in_file)
    barcode = QCODE_REG.findall(basename)
    if not barcode:
        raise mtbutils.MTBdropboxerror('No QBiC Barcode found in {}'.format(in_file))
    if len(set(barcode)) > 1:
        raise mtbutils.MTBdropboxerror('More than one QBiC Barcode found in {}'.format(in_file))
    _, _, pbmc = getentityandpbmc(barcode[0], transaction)
    
    new_name = basename.replace(barcode[0], pbmc)
    new_path = os.path.join(parent_dir, new_name)
    os.rename(in_file, new_path)

    return new_path


def proc_fastq(fastq_file, transaction):
    """Register fastq as dataset in openBIS"""

    # Check, if there are file pairs present (paired-end data!)
    if len(fastq_file) != 2:
        raise mtbutils.MTBdropboxerror('Expecting paired end reads files, found only {}'
            .format(len(fastq_file)))
    qbiccode_f1 = QCODE_REG.findall(os.path.basename(fastq_file[0]))
    qbiccode_f2 = QCODE_REG.findall(os.path.basename(fastq_file[1]))
    if not qbiccode_f1 or not qbiccode_f2:
        raise mtbutils.MTBdropboxerror('No QBiC Barcode found in {}'.format(fastq_file))
    if len(qbiccode_f1) > 1 or len(qbiccode_f2) > 1:
        raise mtbutils.MTBdropboxerror('More than one QBiC Barcode found in {}'.format(fastq_file))
    if qbiccode_f1[0] != qbiccode_f2[0]:
        raise mtbutils.MTBdropboxerror('Found two different barcodes for read pair: {}<->{}'
            .format(qbiccode_f1[0], qbiccode_f2[0]))

    # Get space and project ids
    space, project = space_and_project(qbiccode_f1[0])

    # Create new experiment id
    experiments = transaction.getSearchService().listExperiments('/{}/{}'.format(space, project))
    last_exp_id = get_last_exp_id(experiments)
    new_exp_id = '/{space}/{project}/{project}E{number}'.format(
        space=space, project=project, number=last_exp_id + COUNTER.newId())
    new_sample_id = '/{space}/NGS{barcode}'.format(
        space=space, project=project, barcode=qbiccode_f1[0])
    print(mtbutils.log_stardate('Preparing sample and experiment creation for {sample} and {experiment}'
        .format(sample=new_sample_id, experiment=new_exp_id)))
    new_ngs_experiment = transaction.createNewExperiment(new_exp_id, NGS_EXP_TYPE)
    new_ngs_experiment.setPropertyValue('Q_SEQUENCER_DEVICE', 'UNSPECIFIED_ILLUMINA_HISEQ_2500')
    new_ngs_sample = transaction.createNewSample(new_sample_id, NGS_SAMPLE_TYPE)
    new_ngs_sample.setParentSampleIdentifiers([qbiccode_f1[0]])
    new_ngs_sample.setExperiment(new_ngs_experiment)

    # Create a data-set attached to the NGS sample
    data_set = transaction.createNewDataSet(NGS_RAW_DATA)
    data_set.setMeasuredData(False)
    data_set.setSample(new_ngs_sample)

    # Put the files in one directory
    base_path = os.path.dirname(transaction.getIncoming().getAbsolutePath())
    registration_dir = os.path.join(base_path, '{}_pairend_end_sequencing_reads'.format(qbiccode_f1[0]))
    os.mkdir(registration_dir)
    
    for raw_data in fastq_file:
        os.rename(raw_data, os.path.join(registration_dir, os.path.basename(raw_data)))

    # Attach the directory to the dataset
    transaction.moveFile(registration_dir, data_set)


def space_and_project(qbiccode):
    """Determines the space and project of a given
    sample id.
    
    Returns: Tuple (space, project)
    """
    sample = getsamplev3(qbiccode)
    space = sample.getSpace().getCode()
    project = qbiccode[:5]

    return space, project


def proc_mtb(zip_archive, transaction):
    """Register archive and submit to CentraXX"""
    # CentraXX submition
    submit(zip_archive, transaction)
    # openBIS registration
    registermtb(zip_archive, transaction)


def registermtb(archive, transaction):
    """Register the MTB zipfile as own experiment
    in openBIS"""
    qbiccode_found = QCODE_REG.findall(os.path.basename(archive))
    if not qbiccode_found:
        raise mtbutils.MTBdropboxerror('Could not find a barcode in {}.'.format(archive))
    if len(qbiccode_found) > 1:
        raise mtbutils.MTBdropboxerror('More than one barcode found barcode in {}.'.format(archive))
    qcode = qbiccode_found[0]

    # Get space and project ids
    space, project = space_and_project(qcode)
    search_service = transaction.getSearchService()

    # We design a new experiment and sample identifier
    experiments = transaction.getSearchService().listExperiments('/{}/{}'.format(space, project))
    last_exp_id = get_last_exp_id(experiments)
    new_exp_id = '/{space}/{project}/{project}E{number}'.format(
        space=space, project=project, number=last_exp_id + COUNTER.newId())
    new_sample_id = '/{space}/MTB{barcode}'.format(
        space=space, project=project, barcode=qcode)
    print(mtbutils.log_stardate('Preparing MTB sample and experiment creation for {sample} and {experiment}'
        .format(sample=new_sample_id, experiment=new_exp_id)))
    new_ngs_experiment = transaction.createNewExperiment(new_exp_id, MTB_EXP_TYPE)
    new_ngs_sample = transaction.createNewSample(new_sample_id, MTB_SAMPLE_TYPE)
    new_ngs_sample.setParentSampleIdentifiers([qcode])
    new_ngs_sample.setExperiment(new_ngs_experiment)

    # Create a data-set attached to the NGS sample
    data_set = transaction.createNewDataSet(MTB_RAW_DATA)
    data_set.setMeasuredData(False)
    data_set.setSample(new_ngs_sample)

    # Attach the directory to the dataset
    transaction.moveFile(archive, data_set)


def submit(archive, transaction):
    """Handles the archive parsing and submition
    to CentraXX"""
    print(mtbutils.log_stardate('Preparing CentraXX export of {}...'.format(os.path.basename(archive))))
    qbiccode_found = QCODE_REG.findall(os.path.basename(archive))
    if not qbiccode_found:
        raise mtbutils.MTBdropboxerror('Could not find a barcode in {}.'.format(archive))
    if len(qbiccode_found) > 1:
        raise mtbutils.MTBdropboxerror('More than one barcode found barcode in {}.'.format(archive))
    qcode = qbiccode_found[0]
    patient = getentity(qcode, transaction)
    
    # Arguments for mtbconverter: archive.zip patientID
    args = ['convert', archive, patient]
   
    exit_code = mtbutils.mtbconverter(args)

    if exit_code > 0:
        raise mtbutils.MTBdropboxerror('Could not create patient xml for Centraxx export. '
            'Process quit with exit code {}'.format(exit_code))
    
    # Format the patient export xml filename
    export_fname = CENTRAXX_XML_NAME.format(patient_id=patient, sample_id=qcode)
    export_path = os.path.join(os.getcwd(), export_fname)

    # Create arguments for mtbconverter
    args = ['push', '-t', export_path]
    exit_status = mtbutils.mtbconverter(args)
    if exit_status > 0:
        raise mtbutils.MTBdropboxerror('Did not transfer xml to CentraXX successfully.'
            'Process quit with exit code {}'.format(exit_status))

    print(mtbutils.log_stardate('Successfully exported {} to CentraXX'.format(os.path.basename(archive))))


def getfiles(path):
    """Retrieve all the absolute paths recursively from
    a given directory.

    Returns: list of abs file paths
    """
    if not os.path.isdir(path):
        raise mtbutils.MTBdropboxerror('The incoming data is not a directory.')
    file_list = []
    for path, subdirs, files in os.walk(path):
        for name in files:
            file_list.append(os.path.join(path, name))
    return file_list


def getentityandpbmc(path, transcation):
    """Parses the QBiC barcode from the incoming file path
    and requests the corresponding QBiC patient id and
    PBMC sample id for further processing the registration 
    of tumor and blood sequencing data.

    Returns: Tuple of (path, entity_id, pbmc_id)
    """
    qcode_findings = QCODE_REG.findall(path)
    if not qcode_findings:
        raise mtbutils.MTBdropboxerror('Could not find a barcode in {}.'.format(path))
    if len(qcode_findings) > 1:
        raise mtbutils.MTBdropboxerror('More than one barcode found barcode in {}.'.format(path))
    qcode = qcode_findings[0]

    entity_id = getentity(qcode, transcation)
    pbmc_id = getpbmc(entity_id, transcation)

    print(mtbutils.log_stardate('Found parent with id {}'.format(entity_id)))
    print(mtbutils.log_stardate('Found corresponding PBMC id {}'.format(pbmc_id)))

    return (path, entity_id, pbmc_id)


def getentity(qcode, transaction):
    """Find the corresponding patient id (Q_BIOLOGICAL_ENTITY)
    for a given tumor sample DNA (Q_TEST_SAMPLE)
    
    Returns: The qbic barcode for the patient
    """
    tumor_sample = getsample(qcode, transaction)
    parent_ids = tumor_sample.getParentSampleIdentifiers()
    
    grandparents_found = []
    for parent in parent_ids:
        grandparents_found.append(getsample(parent, transaction).getParentSampleIdentifiers())

    if not grandparents_found:
        raise mtbutils.MTBdropboxerror('No corresponding patient for tumor sample found.')
    if len(grandparents_found) > 1:
        raise mtbutils.MTBdropboxerror("More than one patient "
            "id found for tumor sample: {}".format(grandparents_found))  
    
    grandparent = grandparents_found[0][0]
    
    return(grandparent.split('/')[-1])


def getpbmc(qcode_entity, transaction):
    """Searches for corresponding blood samples (type PBMC)
    for a given patient ID. It is expected, that a given patient
    has only one DNA sample extracted from PBMC as reference for
    somatic variant calling.

    Returns: The QBiC barcode of the corresponding PBMC Q_TEST_SAMPLE
    """
    pbmc_samples = []
    descendand_samples = getallchildren(qcode_entity)
    for sample in descendand_samples:
        if sample.getProperty('Q_PRIMARY_TISSUE') == 'PBMC':
            pbmc_samples.append(sample)
    if not pbmc_samples:
        raise mtbutils.MTBdropboxerror("Could not find any PBMC sample.")
    if len(pbmc_samples) > 1:
        raise mtbutils.MTBdropboxerror("More than 1 PBMC sample found for entity {}"
            .format(qcode_entity))
    
    # Get the id of the attached Q_TEST_SAMPLE
    pbmc_id = ""
    try:
        children = pbmc_samples[0].getChildren()
        pbmc_id = children[0].getCode()
    except Exception as exc:
        mtbutils.MTBdropboxerror("Could not access Q_TEST_SAMPLE code for PBMC in {}"
            .format(qcode_entity))    
    return pbmc_id


def getallchildren(qcode):
    """Fetch all children samples of a given
    sample code and return them as a list

    Returns: List of sample objects
    """ 
    fetch_opt = SampleFetchOptions()
    fetch_opt.withChildrenUsing(fetch_opt)
    fetch_opt.withProperties()

    scrit = SampleSearchCriteria()
    scrit.withCode().thatEquals(qcode)
   
    children_samples = []

    result = api.searchSamples(sessionToken, scrit, fetch_opt)
    
    for sample in result.getObjects():
        # Q_BIOLOGICAL_SAMPLE level
        for kid in sample.getChildren():
            children_samples.append(kid)
            # Q_TEST_SAMPLE level
            for grandkid in kid.getChildren():
                children_samples.append(grandkid)

    return children_samples


def getsample(qcode, transaction):
    """Resturns a immutable sample object for a given
    QBIC barcode."""
    sserv = transaction.getSearchService()
    scrit = SearchCriteria()
    scrit.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(
        SearchCriteria.MatchClauseAttribute.CODE, qcode))
    result = sserv.searchForSamples(scrit)
    
    if not result:
        raise mtbutils.MTBdropboxerror('No matching sample found in openBIS for code {}.'.format(qcode))
    if len(result) > 1:
        raise mtbutils.MTBdropboxerror('More than one sample found in openBIS for code {}.'.format(qcode))
    
    return result[0]


def getsamplev3(qcode):
    """Get a sample object of a given identifier
    in API V3 style

    Returns: A sample (v3) object
    """
    scrit = SampleSearchCriteria()
    scrit.withCode().thatEquals(qcode)

    fetch_opt = SampleFetchOptions()
    fetch_opt.withProperties()
    fetch_opt.withSpace()

    result = api.searchSamples(sessionToken, scrit, fetch_opt)
    samples = []
    for sample in result.getObjects():
        samples.append(sample)
    if len(samples) > 1:
        raise mtbutils.MTBdropboxerror('More than one sample found with identifier {}'.format(qcode))
    return samples[0]
    

