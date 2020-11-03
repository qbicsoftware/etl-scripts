# Changelog


## 1.5.0 2020-11-03

* New maintenance task: update missing checksum once, after dss starts.

## 1.4.1 2020-11-03

* Imgag dropbox: raise an exception, if files of unknown type are part of the transaction

## 1.4.0

* Provide first imaging registration support with OMERO server
* Provide `environment.yaml` that can be used to configure a conda
  environment for the proper setup for the register-omero-metadata
  dropbox
* Register unclassified pooling data of Nanopore experiments directly at the experiment level (no copies are added to sample-based datasets)

## 1.3.1

* Avoid sample registration for existing mass spectrometry data

## 1.3

* Provide metadata schema in JSON for the IMGAG dropbox
* Register checksums for Oxford Nanopore datasets
* Register unclassified read data for Oxford Nanopore datasets

## 1.2

* Provide ETL routine for Oxford Nanopore NGS data
* New dropbox `register-nanopore-dropbox`
