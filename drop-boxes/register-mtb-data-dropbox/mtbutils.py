""" A utility class, holding helper functions
for the main dropbox """

import datetime
import subprocess as sp

def conda_activate(conda_env):
    """Tries to activate a given conda environment"""
    
    command = ['/home/qeana10/bin/miniconda/bin/mtbconverter']
    ret_code = sp.call(command)
    return ret_code

def log_stardate(msg):
    """Prints a message nicely with current stardate"""
    stardate = datetime.datetime.now()
    return '{} [{}]: {}'.format(stardate.isoformat(), 'mtbconverter', msg)

class MTBdropboxerror(Exception):
    """A generic Exception class for this dropbox."""
