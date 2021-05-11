# Example openBIS dropbox written in Java

Please find the source code of the ETL routine that this article is referring to in the
[Java openBIS dropboxes](https://github.com/qbicsoftware/java-openbis-dropboxes) Github repository.

## Installation

Please provide the Java binaries as JAR from the [Java openBIS dropbox](https://github.com/qbicsoftware/java-openbis-dropboxes) in this directories
folder `./lib`.

The DSS needs to be restarted in order to activate this dropbox.

## ETL routine

This dropbox expects a file of any type and creates a new openBIS dataset from it. This dataset
is then attached to a fixed sample with id `/TEST28/QXEGD018AW` for demonstration purposes.