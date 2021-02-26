![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/qbicsoftware/etl-scripts)
![Python Language](https://img.shields.io/badge/language-python-blue.svg)
![License](https://img.shields.io/github/license/qbicsoftware/etl-scripts)
[![DOI](https://zenodo.org/badge/45912621.svg)](https://zenodo.org/badge/latestdoi/45912621)


# ETL openBIS dropboxes

This repository holds a collection of Jython ETL (extract-transform-load) scripts that are used at QBiC that define the behaviour of openBIS dropboxes.
The ETL processes combine some quality control measures for incoming data and data transformation to facilitate the registration in openBIS.

## Environment setup

**1. Conda environment for the register-omero-metadata dropbox**

To provide the dependencies for the register-omero-metadata dropbox to work properly, you can build a conda environment based on the provided [`environment.yaml`](./environment.yaml):

```bash
conda env create -f environment.yaml
```
Make sure that the path to the executables provided in the environment are referenced properly in the register-omero-metadata Python script.

**2. Dependencies for sample tracking functionality**

OpenBIS loads Java libararies on startup, if they are provided in a `lib` folder of an openBIS dropbox. For the sample-tracking to work, you need to provide the 
[sample-tracking-helper](https://github.com/qbicsoftware/sample-tracking-helper-lib) library and deploy it in one of the lib folders.


**3. Dependencies for data transfer objects and parsers**

We decoupled some shared functionality in the [data-model-lib](https://github.com/qbicsoftware/data-model-lib) and the [core-utils-lib](https://github.com/qbicsoftware/core-utils-lib). Please make sure to deploy them as well in of the lib folders, such that the classes are loaded by the etlserver class loader and available during runtime.


## Data format guidelines

These guidelines describe the necessary file structure for different
data types to be met in order to ingest and register them correctly in
openBIS.

Formats:

- [NGS single-end / paired-end data](#ngs-single-end--paired-end-data)

### NGS single-end / paired-end data

**Responsible dropbox:**
[QBiC-register-fastq-dropbox](drop-boxes/register-fastq-dropbox)

**Resulting data model in openBIS**  
Q_TEST_SAMPLE -> Q_NGS_RAW_DATA (with sample code) -> DataSet (directory
with files contained)

**Description**  
For paired-end sequencing reads in FASTQ format, the file structure
needs to look like this

```
<QBIC sample code>.fastq // Directory
    |-- <QBIC sample code>_R1.fastq
    |-- <QBIC sample code>_R1.fastq.sha256sum
    |-- <QBIC sample code>_R2.fastq
    |-- <QBIC sample code>_R2.fastq.sha256sum
```

or in the case of gzipped FASTQ files:

```
<QBIC sample code>.fastq.gz // Directory
    |-- <QBIC sample code>_R1.fastq.gz
    |-- <QBIC sample code>_R1.fastq.gz.sha256sum
    |-- <QBIC sample code>_R2.fastq.gz
    |-- <QBIC sample code>_R2.fastq.gz.sha256sum
```

In the case of single-end sequencing data, the file structure needs to
look like this:

```
<QBIC sample code>.fastq.gz // Directory
    |-- <QBIC sample code>.fastq.gz
    |-- <QBIC sample code>.fastq.gz.sha256sum
```


### Attachment Data

**Responsible dropbox:**
[QBiC-register-exp-proj-attachment](drop-boxes/register-attachments-dropbox)

**openBIS structure:**

Attachments are attached to the Q_PROJECT_DETAILS experiment type and its sample type Q_ATTACHMENT_SAMPLE.

**Expected data structure**
The data structure needs to be a root folder, containing a file `metadata.txt`.

Incoming structure overview:

```
|-<anything> (top level folder name, normally a time stamp of upload time)
    |
    |- metadata.txt
```

**Expected metadata**
Metadata is expected to be denoted in line-separated key-value pairs, where key and value are separated by a '='. The following structure/pairs are expected:

```
user=<the (optional) uploading user name>
info=<short info about the file>
barcode=<the sample code of the attachment sample>
type=<the type of attachment: information or results>
```
The code of the attachment sample is built from the project code followed by three zeroes, conforming to the regular expression "Q[A-Z0-9]{4}000", e.g. QABCD000.

See code examples:
https://github.com/qbicsoftware/attachi-cli/blob/master/attachi/attachi.py#L63
https://github.com/qbicsoftware/projectwizard-portlet/blob/9c86f500b26af4cf2613cfae32e470bf5d50bf78/src/main/java/life/qbic/projectwizard/io/AttachmentMover.java#L145

