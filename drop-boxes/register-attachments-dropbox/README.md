# Attachments Dropbox

## Expected data structure
The data structure needs to be a root folder, containing a file `metadata.txt`.

Incoming structure overview:

```
|-<anything> (top level folder name, normally a time stamp of upload time)
    |
    |- metadata.txt
```

openBIS structure overview:

Attachments are attached to the Q_PROJECT_DETAILS experiment type and its sample type Q_ATTACHMENT_SAMPLE.

TODO: ER model.

## Expected metadata
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
