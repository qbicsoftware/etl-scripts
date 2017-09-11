# ETL script for UKT diagnostics in PM

## To Do

- [ ] Reroute incoming files with barcode tags of type `QUK17*` to this dropbox (datahandler rule)
- [ ] Determine data type
- [ ] Check if sample already has dataset of this type
- [ ] Register data sets
- [ ] Enable snpEff annotation of VCFs
- [ ] Provide proper tracking of snpEff version used (put it in a Singularity container for instance? With version tag!)
- [ ] Register annVCF
- [ ] Activate gene whitelist filtering
- [ ] Build XML in CentraXX scheme
- [ ] Export XML to CentraXX

## Overview schematic of the controlled process

<img src="https://github.com/qbicsoftware/etl-scripts/blob/development/drop-boxes/register-ukt-diagnostics/SOP_new_incoming_dropbox.png" alt="Data Process Workflow">


