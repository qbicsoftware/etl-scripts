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
- [NGS single-end / paired-end data with metadata (deprecated)](#ngs-single-end--paired-end-data-with-metadata-(deprecated))

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