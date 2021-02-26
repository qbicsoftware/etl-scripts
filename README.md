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
- [NGS single-end / paired-end data with metadata (deprecated)](#ngs-single-end--paired-end-data-with-metadata)
- [Attachment Data](#attachment-data)
- [Mass Spectrometry mzML conversion and registration](#mass-spectrometry-mzml-conversion-and-registration)

### NGS single-end / paired-end data

**Responsible dropbox:**
[QBiC-register-fastq-dropbox](drop-boxes/register-fastq-dropbox)

**Resulting data model in openBIS**  
Q_TEST_SAMPLE -> Q_NGS_SINGLE_SAMPLE_RUN (with sample code) -> DataSet
of type Q_NGS_RAW_DATA (directory with files contained)

Example sample ids are:

QABCD001AE (Analyte, Q_TEST_SAMPLE)  
NGSQABCD001AE (Sequencing result, Q_SINGLE_SAMPLE_RUN)

If several runs are submitted with the same analyte id, then no new id
for the run is generated, but a new dataset attached to the existing
sequencing result id.

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


### NGS single-end / paired-end data with metadata (deprecated)

**Disclaimer!**  
This data format is targeted for a single use case and should not be
used for general data registration purposes. Please use the
[NGS single-end / paired-end data](#ngs-single-end--paired-end-data)
format for now.

**Responsible dropbox:**
[QBiC-register-imgag-dropbox](drop-boxes/register-imgag-dropbox)

**Resulting data model in openBIS**  
Q_TEST_SAMPLE -> Q_NGS_SINGLE_SAMPLE_RUN (with sample code) -> DataSet
of type Q_NGS_RAW_DATA (directory with raw sequencing files contained)

Example sample ids:

QABCD001AE (Analyte, Q_TEST_SAMPLE)  
NGS[0-9]{2}QABCS001AE (Sequencing Result, Q_NGS_SINGLE_SAMPLE_RUN) where
the running two-digit number is taken from the identifier suffix from
the `genetics_id` in the metadata file.

**Description**  
For paired-end sequencing reads in FASTQ format, the file structure
needs to look like this

```
<QBIC sample code> // Directory
    |-- file1.fastq.gz
    |-- file2.fastq.gz
    |-- metadata
    |- ...
```

**Expected metadata**  
Additional metadata is required in this format case and expected to be
noted in JSON in a file called `metadata` and following the
[upload metadata schema](drop-boxes/register-imgag-dropbox/upload-metadata.schema.json).
A valid JSON object can look like this:

```
{
    "files": [
        "reads.1.fastq.gz",
        "reads.2.fastq.gz"
    ],
    "type": "dna_seq",
    "sample1": {
        "genome": "GRCh37",
        "id_genetics": "GS000000_01",
        "id_qbic": "QTEST002AE",
        "processing_system": "Test system",
        "tumor": "no"
    }
}
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


### Mass Spectrometry mzML conversion and registration

**Responsible dropbox:**
[QBiC-convert-register-ms-vendor-format](drop-boxes/register-convert-ms-vendor-format)

**Resulting data model in openBIS**  
...Q_TEST_SAMPLE (-> Q_MHC_LIGAND_EXTRACT (Immunomics case)) -> Q_MS_RUN per data file --> 2 DataSets per data file, one for raw data, one converted to mzML

**Expected data structure**
In every use case, the data structure needs to contain a top folder around the respective data in order to accommodate metadata files.

The sample code found in the top folder can be of type `Q_TEST_SAMPLE` or `Q_MS_RUN`. In the former case, a new sample of type `Q_MS_RUN` is created and attached as child to the test sample.

**Valid folder/file types**:
- Thermo Fisher Raw file format
- Waters Raw folder
- Bruker .d folder

**Incoming structure overview for standard case without additional metadata file:**
```
QABCD102A5_20201229145526_20201014_CO_0976StSi_R05_.raw
|-- QABCD102A5_20201229145526_20201014_CO_0976StSi_R05_.raw
|-- QABCD102A5_20201229145526_20201014_CO_0976StSi_R05_.raw.sha256sum
```
In this case, existing mass spectrometry metadata is expected to be already stored and the dataset will be attached.


**Incoming structure overview for the use case of Immunomics data with metadata file:**
```
QABCD090B7
|-- QABCD090B7
|   |-- file1.raw
|   |-- file2.raw
|   |-- file3.raw
|   `-- metadata.tsv
|-- QABCD090B7.sha256sum
`-- source_dropbox.txt
```
The source_dropbox.txt currently has to indicate the source as one of the Immunomics data sources.

The `metadata.tsv` columns for the Immunomics case are tab-separated:
```
Filename	Q_MS_DEVICE	Q_MEASUREMENT_FINISH_DATE	Q_EXTRACT_SHARE	Q_ADDITIONAL_INFO	Q_MS_LCMS_METHODS	technical_replicate	workflow_type
file1.raw	THERMO_QEXACTIVE	171010	10		QEX_TOP07_470MIN	DDA_Rep1	DDA
```

Filename - one of the (e.g. raw) file names found in the incoming structure
Q_MS_DEVICE - openBIS code from the vocabulary of Mass Spectrometry devices
Q_MEASUREMENT_FINISH_DATE - Date in YYMMDD format (ISO 8601:2000)
Q_EXTRACT_SHARE - the extract share
Q_ADDITIONAL_INFO - any optional comments
Q_MS_LCMS_METHODS - openBIS code from the vocabulary of LCMS methods
technical_replicate - free text to denote replicates
workflow_type - DDA or DIA
