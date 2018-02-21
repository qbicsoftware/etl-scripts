from __future__ import print_function

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

import sys
import mtbutils
import logging

# Path to checksum.py
sys.path.append('/home-link/qeana10/bin')

CONDA_ENV = 'centraxx_mtb'

cmd_status = mtbutils.conda_activate(CONDA_ENV)

if cmd_status != 0:
    raise mtbutils.MTBdropboxerror("Conda environment could not be loaded. Exit status was: " + cmd_status)

print(mtbutils.log_stardate("Conda environment loaded successfully"))

def process(transaction):
    """The main dropbox funtion.
    openBIS executes this function during an incoming file event.
    """
    incoming_path = transaction.getIncoming().getAbsolutePath()
    file_name = transaction.getIncoming().getName()
    print(mtbutils.log_stardate('Incoming file event: {}'.format(file_name)))
    raise mtbutils.MTBdropboxerror('Diese Datei entfernst du nicht, openBIS')
