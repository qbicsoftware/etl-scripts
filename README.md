# ETL openBIS dropboxes

This repository holds a collection of Jython ETL (extract-transform-load) scripts that are used at QBiC that define the behaviour of openBIS dropboxes.
The ETL processes combine some quality control measures for incoming data and data transformation to facilitate the registration in openBIS.

## Data format guidelines

These guidelines describe the necessary file structure for different
data types to be met in order to ingest and register them correctly in
openBIS.

Formats:

- [NGS single-end / paired-end data](#ngs-single-end--paired-end-data)
<<<<<<< HEAD
- [HLA Typing data](#hla-typing-data)

=======
>>>>>>> master

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

<<<<<<< HEAD
### HLA Typing data
**Responsible dropbox:**
[QBiC-register-hlatyping-dropbox](drop-boxes/register-hlatyping-dropbox)

**Resulting data model in openBIS**  
Q_TEST_SAMPLE -> Q_NGS_HLATYPING (with sample code) -> DataSet (directory
with files contained)

or

Q_TEST_SAMPLE -> Q_NGS_SINGLE_SAMPLE_RUN (provided sample code) -> Q_NGS_HLATYPING -> DataSet (directory
with files contained)

Example sample ids are:
QABCD001AE (Analyte, Q_TEST_SAMPLE)  
HLA1QABCD001AE (HLA-Typing result, Q_NGS_HLATYPING) for HLA MHC class I
or
HLA2QABCD001AE (HLA-Typing result, Q_NGS_HLATYPING) for HLA MHC class II


**Description**  
For HLA typing data in VCF format, the file structure
needs to look like this:

```
<QBIC sample code> // Directory
    |-- <QBIC sample code>.txt
    |-- <QBIC sample code>.txt.sha256sum
```

=======
>>>>>>> master

