# IMGAG dropbox

## Expected data structure
The data structure needs to be a root folder, containing a file `metadata` following the [upload metadata schema](upload-metadata.schema.json). In addition, the folder shall contain files of type `fastq/fastq.gz` and/or `vcf/vcf.gz` and/or `GSvar/GSvar.gz`. 

Incoming structure overview:

```
|-QTEST001AE (top level folder name)
    |
    |- file1.fastq.gz
    |- file2.fastq.gz
    |- metadata
    |- ...

```

openBIS structure overview:

TODO: ER model.

## Expected metadata
Metadata is expected to be noted in JSON and following the [upload metadata schema](upload-metadata.schema.json). An example JSON entry can look like this:

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

The sample code for `id_qbic` can be of type `Q_TEST_SAMPLE` or `Q_BIOLOGICAL_SAMPLE`. In the latter case, a new sample of type `Q_TEST_SAMPLE` is created and attached as child to the biological sample. The data-set will be registered under this test sample then.