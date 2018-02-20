""" A utility class, holding helper functions
for the main dropbox """

import subprocess as sp

def conda_activate(conda_env):
    """Tries to activate a given conda environment"""
    command = ['source', 'activate', conda_env]
    ret_code = sp.call(command)
    return ret_code

class MTBdropboxerrer(Exception):
    """A generic Exception class for this dropbox."""