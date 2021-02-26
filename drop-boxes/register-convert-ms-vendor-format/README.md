# Mass Spectrometry Dropbox

## Expected data structure
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

metadata.tsv columns for the Immunomics case, tab-separated with one example row:
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

**Resulting data model in openBIS**  
...Q_TEST_SAMPLE (-> Q_MHC_LIGAND_EXTRACT (Immunomics case)) -> Q_MS_RUN per data file --> 2 DataSets per data file, one for raw data, one converted to mzML
