from __future__ import print_function

import os
import re
import sys

# Java openBIS imports
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import SearchCriteria

#############################################################################
#
# The ETL environment setup.
#
#############################################################################
# Path to checksum.py
sys.path.append('/home-link/qeana10/bin')

# Regex matching the QBiC barcode naming specification
QCODE_REG = re.compile('Q\w{4}[0-9]{3}[a-zA-Z]\w')

# Regex matching the WIFF files naming specification
WIFF_FILE = re.compile(r'(\w*\.wiff$)\b')
WIFF_SCAN_FILE = re.compile(r'(\w*\.wiff.scan)\b')

# Path to the openBIS properties file
PROPERTIES = '/etc/openbis.properties'

#############################################################################
#
# The ETL logic starts here.
#
#############################################################################


def process(transaction):
    incoming_path = transaction.getIncoming().getAbsolutePath()
    file_name = transaction.getIncoming().getName()
    file_list = getfiles(incoming_path)

    # Create wiff file pairs
    wiff_pairs = wiffpairs(file_list)
    assert wiff_pairs

    qbic_id = re.findall(QCODE_REG, file_name)  # Fetch QBiC id from dir name
    assert qbic_id

    register_wiff_pairs(transaction, wiff_pairs, qbic_id[0])


def register_wiff_pairs(transaction, wiff_pairs, qbic_id):
    """Registers wiff file pairs in openBIS.

    :param transaction: The current transaction
    :param wiff_pairs: A list of wiff file and scan tuples
    :param qbic_id: The QBiC id
    :return: Nothing
    """
    space, project = space_and_project(transaction, qbic_id)
    experiments = transaction.getSearchService().listExperiments('/{}/{}'.format(space, project))
    exp_ids = [int(re.search(r'\d.*', oid.getExperimentIdentifier()).group(0)) for oid in experiments if re.search(r'\d', oid.getExperimentIdentifier())]
    exp_ids.sort()
    last_exp_id = exp_ids[-1]

    new_exp_id = '/{space}/{project}/{project}E{number}'.format(
        space=space, project=project, number=last_exp_id + 1)
    new_sample_id = '/{space}/MS{barcode}'.format(
        space=space, project=project, barcode=qbic_id)
    new_ms_experiment = transaction.createNewExperiment(new_exp_id, "Q_MS_MEASUREMENT")
    new_ms_experiment.setPropertyValue('Q_CURRENT_STATUS', 'FINISHED')
    new_ms_sample = transaction.createNewSample(new_sample_id, "Q_MS_RUN")
    new_ms_sample.setParentSampleIdentifiers(["/{space}/{barcode}".format(space=space, barcode=qbic_id)])
    new_ms_sample.setExperiment(new_ms_experiment)

    # Create a data-set attached to the VARIANT CALL sample
    data_set = transaction.createNewDataSet("Q_MS_RAW_DATA")
    data_set.setMeasuredData(False)
    data_set.setSample(new_ms_sample)

    # Put the files in one directory
    base_path = os.path.dirname(transaction.getIncoming().getAbsolutePath())
    registration_dir = os.path.join(base_path, '{}_wiff_scan'.format(qbic_id))
    os.mkdir(registration_dir)

    for pair in wiff_pairs:
        wiff, scan = pair
        os.rename(wiff, os.path.join(registration_dir, os.path.basename(wiff)))
        os.rename(scan, os.path.join(registration_dir, os.path.basename(scan)))

    # Attach the directory to the dataset
    transaction.moveFile(registration_dir, data_set)


def space_and_project(transaction, qbiccode):
    """Determines the space and project of a given
    sample id.

    Returns: Tuple (space, project)
    """
    search_service = transaction.getSearchService()
    sc = SearchCriteria()
    sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, qbiccode))
    found_samples = search_service.searchForSamples(sc)
    space = found_samples[0].getSpace()

    return space, qbiccode[0:5]


def wiffpairs(files):
    """Creates tuples of corresponding wiff file and wiff scan files.

    Throws an exception if a pair can not be found.

    :param files: A list of files
    :return: A list of pairs of wiff files and wiff scan files
    """
    pairs = []
    wiff_files = [wiff for wiff in files if re.findall(WIFF_FILE, wiff)]
    assert wiff_files  # should not be empty
    wiff_scans = [scan for scan in files if re.findall(WIFF_SCAN_FILE, scan)]
    assert wiff_scans  # should not be empty
    assert len(wiff_scans) == len(wiff_files)
    # Find the corresponding wiff scan file
    for wiff_file in wiff_files:
        scan = [scan for scan in wiff_scans if wiff_file in scan]
        assert scan
        pairs.append((wiff_file, scan[0]))
        wiff_scans.remove(scan[0])
    assert not wiff_scans  # means we have not left any scan files
    return pairs


def getfiles(path):
    """Retrieve all the absolute paths recursively from
    a given directory.

    Returns: list of abs file paths
    """
    if not os.path.isdir(path):
        raise IOError('The incoming data is not a directory.')
    file_list = []
    for path, subdirs, files in os.walk(path):
        for name in files:
            file_list.append(os.path.join(path, name))
    return file_list

