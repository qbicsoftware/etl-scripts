# How to psudo-anonymize those dicoms


I have a conda virtual environment running for this project with python v3.6.1 so as not to muddy the pure waters of my system's python installation. I have provided an environment.yml file, which you could use to duplicate the environment using the following command:

```bash
conda env create -f environment.yml
``` 

Otherwise, the only things you would need to install are the *pydicom* package for parsing the dicom files, and *numpy* for making some random numbers.

Depending on your system, you can install *pydicom* with *pip* or *conda*.

```bash 
pip install pydicom 
```
alternatively

```bash
conda install -c conda-forge pydicom=0.9.9
```
If the script still gives you problems, here are my import statemets:

```python
try:
    import pydicom as dicom
except ImportError:
    import dicom
import os
import numpy as np
from argparse import ArgumentParser
import pickle
from glob import glob
import tarfile
import re
```

I usually like to run one job for each folder in the dropbox: *Punktion*, *PETMR*, *CT-Perfusion*. Actually, the script is really set up to run this way.

First, make sure that the appropriate folder structure is intact in these folders. Otherwise, it will be made:

```bash
python HCC_dicom_anonymize.py --clean-up-multiple /full/path/to/Punktion/PETMR/or/CT-Perfusion

``` 
Once this script has run, then do the pseudo-anonymization in a similar fashion.

```bash
python HCC_dicom_anonymize.py --root-directory /full/path/to/Punktion/PETMR/or/CT-Perfusion 

```

Optionally, you can create a directory where you would like things to be saved, otherwise, by default, a folder called *anonymize_to_qbic* is generated in the current working directory.

```bash
python HCC_dicom_anonymize.py --root-directory /full/path/to/Punktion/PETMR/or/CT-Perfusion --save_dir /full/path/to/output/directory
```