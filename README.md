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

##4. Dependencies for the example dropbox written in pure Java/Groovy

Just deploy the compiled JAR of the [Java openBIS dropbox](https://github
.com/qbicsoftware/java-openbis-dropboxes) in the `lib` folder of the dropbox (`
./register-example-java-dropbox/lib`).

## Data format guidelines

These guidelines describe the necessary file structure for different
data types to be met in order to ingest and register them correctly in
openBIS.

Formats:

- [Illumina NGS single-end / paired-end data](#illumina-ngs-single-end--paired-end-data)
- [PacBio NGS data](#pacbio-ngs-data)
- [HLA Typing data](#hla-typing-data)
- [NGS single-end / paired-end data with metadata (deprecated)](#ngs-single-end--paired-end-data-with-metadata)
- [Attachment Data](#attachment-data)
- [Mass Spectrometry mzML conversion and registration](#mass-spectrometry-mzml-conversion-and-registration)
- [Mass Spectrometry with preconverted mzML](#mass-spectrometry-with-preconverted-mzml)
- [BioImage data with OMERO server](#bioimage-data-with-omero-server)

### Illumina NGS single-end / paired-end data

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

### PacBio NGS data

**Responsible dropbox:**
[QBiC-register-pacbio](drop-boxes/register-pacbio-dropbox)

**Resulting data model in openBIS**  
Q_TEST_SAMPLE -> Q_NGS_PACBIO_RUN" (with sample code) -> DataSet
of type Q_NGS_PACBIO_DATA (directory with files contained)

Example sample ids are:

QABCD001AE (Analyte, Q_TEST_SAMPLE)  
NGSQABCD001AE (Sequencing result, Q_NGS_PACBIO_RUN)

If several runs are submitted with the same analyte id, then no new id
for the run is generated, but a new dataset attached to the existing
sequencing result id.

**Description**  
To be recognized as PacBio sequencing data by the script, at least one bam,
the pacbio bam index (.bam.pbi) and the pacbio xml file needs to be contained:

```
<QBIC sample code>_<anything>_pacbio // Directory
    |-- <anyname1>.xml
    |-- <anyname2>.bam.pbi
    |-- <anybam1>.bam
    |-- <anybam2>.bam
    ...
```

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

### NGS single-end / paired-end data with metadata
(deprecated)

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
user=<the (optional) uploading user name.>
info=<short info about the file>
barcode=<the sample code of the attachment sample>
type=<the type of attachment: information or results>
```
If a university user name is provided and the registration of data fails, the user will receive an email containing the error.

The info field must not contain line breaks, as each line in the metadata file must contain a key-value pair.

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
Filename    Q_MS_DEVICE Q_MEASUREMENT_FINISH_DATE   Q_EXTRACT_SHARE Q_ADDITIONAL_INFO   Q_MS_LCMS_METHODS   technical_replicate workflow_type
file1.raw   THERMO_QEXACTIVE    171010  10      QEX_TOP07_470MIN    DDA_Rep1    DDA
```

Filename - one of the (e.g. raw) file names found in the incoming structure

Q_MS_DEVICE - openBIS code from the vocabulary of Mass Spectrometry devices

Q_MEASUREMENT_FINISH_DATE - Date in YYMMDD format (ISO 8601:2000)

Q_EXTRACT_SHARE - the extract share

Q_ADDITIONAL_INFO - any optional comments

Q_MS_LCMS_METHODS - openBIS code from the vocabulary of LCMS methods

technical_replicate - free text to denote replicates

workflow_type - DDA or DIA


### Mass Spectrometry with preconverted mzML

**Responsible dropbox:**
[register-raw-and-mzml.py](drop-boxes/register-convert-ms-vendor-format)

**Resulting data model in openBIS**  
...Q_TEST_SAMPLE -> Q_MS_RUN per data file --> 2 DataSets per data file, one for raw data, one for the provided mzML

**Expected data structure**
The data structure needs to contain a top folder around the respective files in order to accommodate both the raw and the converted file.

The sample code found in the top folder can be of type `Q_TEST_SAMPLE` or `Q_MS_RUN`. In the former case, a new sample of type `Q_MS_RUN` is created and attached as child to the test sample.

**Valid folder/file types**:
- Thermo Fisher Raw file format
- Waters Raw folder
- Bruker .d folder

**Incoming structure overview:**
```
QABCD102A5_20201229145526_20201014_CO_0976StSi_R05
|-- QABCD102A5_20201229145526_20201014_CO_0976StSi_R05.raw
|-- QABCD102A5_20201229145526_20201014_CO_0976StSi_R05.mzML
```
In this case, existing mass spectrometry metadata is expected to be already stored and the dataset will be attached.


### BioImage data with OMERO server

**Responsible dropbox:**
[QBiC-register-omero-metadata](drop-boxes/register-omero-metadata)

**Resulting data model in openBIS:**  
For each biological sample, multiple images (the data files) can be created, so multiple Q_BMI_GENERIC_IMAGING_RUN samples are created and attached to that biological sample, `Q_BIOLOGICAL_SAMPLE` -> one `Q_BMI_GENERIC_IMAGING_RUN` per data target (e.g. an image file, or folder containing image files).

**Expected data structure:**
The structure of the input data needs to contain a top (root) folder, named after the corresponding biological sample code, this code (ID) is of type `Q_BIOLOGICAL_SAMPLE`. This data folder must contain a `metadata_table.tsv` file to specify image-level metadata, by defining a table where each row indicates an image data target, i.e. a path to an image file, or a sub-folder containing image files (`IMAGE_DATA_PATH`). The columns of the table are used to specify names (keys) of metadata properties, or ETL parameters (e.g. `IMAGE_DATA_PATH`, `SAMPLE_ID`, `ETL_TAG`).

**Valid file types:**
Valid files in the folder are any bioimage files that can be handled by Bio-Formats and an OMERO server. This ETL process uses the Python package [`omero-bifrost`](https://github.com/qbicsoftware/omero-bifrost) to register the input data into an OMERO server, this package depends on the [`omero-py`](https://github.com/ome/omero-py) CLI and remote API for image data and metadata transfer.

**Incoming structure overview:**

```
QABCD002A8
|-- QABCD002A8
|   |-- Est-B1b.lif
|   |-- Image_1.czi
|   |-- dataset_1
|   |   |-- Est-B2a.lif
|   |   |-- Image_2.czi
|   |   |-- sub_tomo_1.mrc
|   |-- dataset_2
|   |   |-- Est-B3c.lif
|   |   |-- Image_3.czi
|   |   |-- sub_tomo_1.mrc
|   |-- tissue_x_marker_1.ome.tiff
|   `-- metadata_table.tsv
|-- QABCD002A8.sha256sum
`-- source_dropbox.txt
```

The metadata annotations are specified in the TSV file `metadata_table.tsv`. This file ends in `.tsv`, it has tab-separated columns that create the following table structure:

```
IMAGE_DATA_PATH  IMAGING_MODALITY    IMAGED_TISSUE   SAMPLE_ID      OMERO_TAGS      ETL_TAG      INSTRUMENT_MANUFACTURER    INSTRUMENT_USER    IMAGING_DATE
./                  NCIT_C18113         cell            *              tag-x,tag-y     *            FEI                        Dr. Horrible       01.03.2021
dataset_1/          NCIT_C18113         cell            *              tag-y           *            FEI                        Max Mustermann     01.04.2021
dataset_2/          NCIT_C18216         leaf            QABCD002F5     *               dicom-vol    Zeiss                      Max Mustermann     23.02.2021
```

The `SAMPLE_ID` field is used to override the target sample ID (`Q_BIOLOGICAL_SAMPLE`) for a specific data sub-folder (row in the metadata table). The `OMERO_TAGS` field is used to specify OMERO tags, this will annotate all images in the data folder with the specified tags in the OMERO server (tag values separated by the character `,`). The `ETL_TAG` field is used to specify a modality-specific subprocess within the ETL process for a specific data sub-folder. Modality-specific subprocesses aim to provide additional support for specialized data processing (e.g. transform DICOM fileset into NIfTI file) in a range of bioimaging modalities (e.g. MRI/DICOM, CODEX/MACSima, light-sheet microscopy). The placeholder value `*` for a property (table column) is used to indicate that the property has no valid value for the data folder specified in the table row (line in the TSV file). If the value `./` is provided for `IMAGE_DATA_PATH`, the relative root directory will be asumed (targets the root folder).

The following use-case exemplifies a data structure and metadata table with both image file and sub-folder targets:

```
QABCD002A8
|-- QABCD002A8
|   |-- Est-B1a.lif
|   |-- Image_1.czi
|   |-- dataset_1
|   |   |-- Image_2.czi
|   |   |-- sub_tomo_1.mrc
|   |-- dataset_2
|   |   |-- Est-B2c.lif
|   |   |-- Image_3.czi
|   |   |-- sub_tomo_2.mrc
|   `-- metadata_table.tsv
|-- QABCD002A8.sha256sum
`-- source_dropbox.txt
```

```
IMAGE_DATA_PATH             IMAGING_MODALITY    IMAGED_TISSUE   INSTRUMENT_MANUFACTURER    INSTRUMENT_USER    IMAGING_DATE
Est-B1a.lif                    NCIT_C18113         leaf            FEI                        Dr. Horrible       01.03.2021
Image_1.czi                    NCIT_C18113         leaf            Zeiss                      Dr. Horrible       01.03.2021
dataset_1/Image_2.czi          NCIT_C18113         cell            Zeiss                      Max Mustermann     01.04.2021
dataset_1/sub_tomo_1.mrc       NCIT_C18113         cell            FEI                        Max Mustermann     01.04.2021
dataset_2/                     NCIT_C18216         leaf            Zeiss                      Max Mustermann     23.02.2021
```

**Description of image-level metadata table:**

column name | description
--------------|----------------
`IMAGE_DATA_PATH`| The relative path to one of the image files, or data sub-folders found in the incoming root folder (one data target per line).
`IMAGING_MODALITY`| Ontology Identifier for the imaging modality, currently from the [NCI Thesaurus](https://ncit.nci.nih.gov/ncitbrowser/pages/home.jsf?version=21.02d). The following are valid examples: <br> `NCIT_C16855` (Scanning Electron Microscopy) <br> `NCIT_C18216` (Transmission Electron Microscopy) <br> `NCIT_C18113` (Cryo-Electron Microscopy) <br> `NCIT_C17995` (Light Microscopy) <br> `NCIT_C16856` (Fluorescence Microscopy) <br> `NCIT_C17753` (Confocal Microscopy) <br> `NCIT_C23011` (Hematoxylin and Eosin Staining Method) <br> `NCIT_C23020` (Immunohistochemistry Staining Method) <br> `NCIT_C181928` (Multiplexed Immunofluorescence) <br> `NCIT_C16809` (Magnetic Resonance Imaging) <br> `NCIT_C17204` (Computed Tomography) 
`IMAGED_TISSUE` | The imaged tissue or cell type
`INSTRUMENT_MANUFACTURER` | The imaging instrument manufacturer
`INSTRUMENT_USER` | The person who measured the data file using the imaging instrument
`IMAGING_DATE` | The date of the measurement in **dd.mm.yyyy** format (days and months with leading zeroes)
`SAMPLE_ID` | Overrides the sample ID for a specific data folder **(Optional)**
`OMERO_TAGS` | Used to specify OMERO tags, this will annotate all images in the data folder **(Optional)**
`ETL_TAG` | Used to specify a modality-specific subprocess (e.g. for DICOM data, CODEX/MACSima, or light-sheet microscopy) **(Optional)**
