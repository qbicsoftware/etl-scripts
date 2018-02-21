""" A utility class, holding helper functions
for the main dropbox """

import datetime
import subprocess as sp

MTB_CONVERTER_PATH = '/home/qeana10/bin/miniconda/bin/mtbconverter'

def mtbconverter(*cmds):
    """Tries to activate a given conda environment"""
    command = [MTB_CONVERTER_PATH] + list(cmds)
    ret_code = sp.call(command)
    return ret_code

def log_stardate(msg):
    """Prints a message nicely with current stardate"""
    stardate = datetime.datetime.now()
    return '{} [{}]: {}'.format(stardate.isoformat(), 'mtbconverter', msg)

class MTBdropboxerror(Exception):
    """A generic Exception class for this dropbox."""
