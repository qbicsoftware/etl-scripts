# Changelog

# 2.0.0

* Provides new ETL for MTB project data that are not supposed to be stored in QUK17 [(#89)](https://github.com/qbicsoftware/etl-scripts/pull/89)
* Allow multiple sequencing lanes for MTB data

# 1.9.0 2021-06-28

* Provides new ETL routine written in Java, that will replace all Jython scripts at some point [(#85)](https://github.com/qbicsoftware/etl-scripts/pull/85)
* Support for nf-core pipeline result registration [(#85)](https://github.com/qbicsoftware/etl-scripts/pull/85)
* Provides metadata validation for imaging data (OMERO etl). [(#85)](https://github.com/qbicsoftware/etl-scripts/pull/83)

## 1.8.0 2021-05-11

* Add example Java dropbox

## 1.7.0 2021-03-19

* Provides fully tested functionality to register generic imaging data, with OMERO server support (v5.4.10). [Link to PR](https://github.com/qbicsoftware/etl-scripts/pull/78)
* Uses an omero-importer-cli (with Bio-formats) for image file registration into an OMERO server instance
* Uses an initial version of the openBIS-OMERO metadata model

## 1.6.0 2021-01-22

* Fix for workflow result registration: fetch sample by identifier instead of search for robustness against indexing problems
* Retry sample tracking updates twice and log failures that occur

## 1.5.0 2020-11-03

* New maintenance task: update missing checksum once, after dss starts.
* Fix for nanopore registration: rename folders for pooling case
* Fix for experiment update: force identifier into a string to support v3 API objects

## 1.4.1 2020-11-03

* Imgag dropbox: raise an exception, if files of unknown type are part of the transaction

## 1.4.0

* Provide first imaging registration support with OMERO server
* Provide `environment.yaml` that can be used to configure a conda
  environment for the proper setup for the register-omero-metadata
  dropbox
* Register unclassified pooling data of Nanopore experiments directly at the experiment level (no copies are added to sample-based datasets)
* Add description for data of register-hlatyping-dropbox

## 1.3.1

* Avoid sample registration for existing mass spectrometry data

## 1.3

* Provide metadata schema in JSON for the IMGAG dropbox
* Register checksums for Oxford Nanopore datasets
* Register unclassified read data for Oxford Nanopore datasets

## 1.2

* Provide ETL routine for Oxford Nanopore NGS data
* New dropbox `register-nanopore-dropbox`
