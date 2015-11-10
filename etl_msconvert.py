"""
This etl script converts proteomics raw files to mzML.

Incoming raw files are converted and the new mzML is copied
to an mzML dropbox. After that the raw file is registered at
openbis.

To convert the raw files a virtual windows machine
`qconvert.am10.uni-tuebingen.de` is used. The file is copied
to a temporary directory on that machine with rsync, and
`msconvert` is executed via ssh. The result is copied back
with rsync.

The stdout of this file is redirected to
`~openbis/servers/datastore_server/log/startup_log.txt`
TODO why there??
"""

import sys
import os
import time
import re
import shutil
import subprocess
import signal
from functools import partial
import logging
import ch.systemsx.cisd.etlserver.registrator.api.v2
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import (
    SearchCriteria, SearchSubCriteria
)
try:
    import shlex as pipe
except ImportError:
    import pipe

# *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
barcode_pattern = re.compile('Q[a-zA-Z0-9]{4}[0-9]{3}[A-Z][a-zA-Z0-9]')
MARKER = '.MARKER_is_finished_'
MZML_DROPBOX = "/i-dont-know-yet"
MSCONVERT_HOST = "qmsconvert.am10.uni-tuebingen.de"
MSCONVERT_USER = "qbic"
REMOTE_BASE = "/cydrive/d/etl-convert"
CONVERSION_TIMEOUT = 7200


if not hasattr(__builtins__, 'TimeoutError'):
    class TimeoutError(Exception):
        pass


class ConvertionError(RuntimeError):
    pass


def check_output(cmd, timeout=None, **kwargs):
    """Run a program and raise an error on error exit code.

    This is basically just `subprocess.check_output`, but the version
    in jython does not support timeouts."""
    PIPE = subprocess.PIPE
    popen = subprocess.Popen(cmd, stdout=PIPE, stderr=PIPE, **kwargs)

    class _Alarm(Exception):
        pass

    def alarm_handler(signum, frame):
        raise _Alarm()

    if timeout:
        old_handler = signal.signal(signal.SIGALRM, alarm_handler)
        old_alarm = signal.alarm(timeout)
    try:
        out, err = popen.communicate()
        retcode = popen.returncode
        if retcode:
            logging.debug("Command %s failed with error code %s",
                          " ".join(cmd), retcode)
            logging.debug("stdout: %s", out)
            logging.debug("stderr: %s", err)
            raise subprocess.CalledProcessError(retcode, " ".join(cmd))
        return out, err
    except _Alarm:
        popen.kill()
        _ = popen.communicate()
        raise TimeoutError()
    finally:
        if timeout:
            signal.alarm(old_alarm)
            signal.signal(signal.SIGALRM, old_handler)


def rsync(source, dest, source_host=None, dest_host=None, source_user=None,
          dest_user=None, timeout=None, extra_options=None):
    """Use rsync to copy a file from one host to another."""
    if source_host:
        source = "%s:%s" % (source_host, source)

    if dest_host:
        dest = "%s:%s" % (dest_host, dest)

    cmd = ['rsync', '--', source, dest]
    if extra_options:
        cmd = cmd[0:1] + extra_options + cmd[1:]
    return check_output(cmd, timeout=timeout)


def call_ssh(cmd, host, user=None, timeout=None):
    """Execute cmd on host via ssh.

    Return stdout and stderr of the remote command. The command must
    be a list containing the name of the program and the arguments.
    """
    if user:
        host = "%s@%s" % (user, host)
    full_cmd = ['ssh', host, '-oBatchMode=yes', '--']
    full_cmd.extend(pipe.quote(i) for i in cmd)
    return check_output(full_cmd, timeout=timeout)


def convert_raw(raw_path, dest, remote_base, host, timeout, user=None,
                msconvert_options=None, dryrun=False):
    """Convert a raw file to mzml on a remote machine.

    Uses ssh to run remote commands. Creates a temporary directory on
    the remote host in `remote_base` and use rsync to copy the raw file
    into that directory. Execute msconvert via ssh. If this finishes
    before the timeout runs out, the result is copied to `dest`. If
    msconvert times out, this function will try to remove the remote
    temporary directory and raise a `ConvertionError`.
    """
    ssh = partial(call_ssh, host=host, user=user, timeout=timeout)
    rsync_base = partial(rsync, timeout=timeout, extra_options=['-q'])
    rsync_to = partial(rsync_base, dest_user=user, dest_host=host)
    rsync_from = partial(rsync_base, source_host=host, source_user=user)

    remote_dir, _ = ssh(['mktemp', '-dq', '-p', remote_base])
    try:
        remote_dir = remote_dir.decode().strip()
    except AttributeError:
        remote_dir = remote_dir.strip()
    try:
        remote_file = os.path.join(remote_dir, os.path.basename(raw_path))
        rsync_to(source=raw_path, dest=remote_file)

        remote_mzml = os.path.splitext(remote_file)[0] + '.mzML'
        if dryrun:
            ssh(['cp', remote_file, remote_mzml])
        else:
            ssh(['msconvert', remote_file])
        rsync_from(source=remote_mzml, dest=dest)
    finally:
        try:
            ssh(["rm", "-rf", remote_dir])
        except Exception:
            logging.exception("Could not remove remote dir.")
        if os.path.exists(dest):
            os.unlink(dest)


def extract_barcode(filename):
    """Extract valid barcodes from the filename.

    Return project_id, experiment_id and the whole barcode.
    If no barcode was found, raise a ValueError. If the file
    contains a qbic barcode with an invalid checksum, raise
    a RuntimeError.

    TODO rewrite
    """
    try:
        code = barcode[0:-1]
        return checksum.checksum(code) == barcode[-1]
    except:
        return False


class DropboxhandlerFile(object):
    """Represent a new file coming from dropboxhandler.

    For each new input file the dropboxhandler will write a directory
    containing the actual datafile and some metadata:

        name_of_new_incoming_file
            name_of_new_incoming_file.RAW
            name_of_new_incoming_file.RAW.sha256sum
            name_of_new_incoming_file.RAW.origlabfilename
            meta.json

    If the name of the file contained a qbic barcode, that will be
    written to the beginning of the new file name.

    Attributes
    ----------
    barcode: str or None
        If there was a barcode found in the file name, this will
        search the openbis db for a sample with this barcode and
        return the openbis object.
    project: str or None
        The part of the barcode representing the project (including
        the leading Q).
    datapath: str
        The path to the actual data file.
    dataname: str
        The name of the data file with extension.
    meta: dict
        A dictionary containing metadata about the dataset. Taken
        from `meta.json`.
    checksum_path: str
        The path to the checksum file.
    """

    def __init__(path, require_barcode=True):
        self.path = path
        self.name = os.path.basename(path)
        if not os.path.isdir(self.path):
            raise ValueError("Invalid path. Not a directory: %s" % self.path)

        self.datapath = os.path.join(self.path, self.name)
        if not os.path.exists(self.datapath):
            raise ValueError("Could not find data file %s" % self.datapath)
        self.dataname = os.path.basename(self.datapath)
        self.checksum_path = self.datapath + '.sha256sum'
        if not os.path.exists(self.checksum_path):
            raise ValueError(
                "Could not find checksum file %s" % self.checksum_path
            )

        try:
            self.barcode = extract_barcode(path)
            self.project = self.barcode[:5]
        except ValueError:
            if require_barcode:
                raise
            self.barcode, self.project = None, None

    def write_metadata(self, measurement=None, run=None, dataset=None):
        """Write metadata to openbis experiments / samples as appropriate."""
        raise NotImplementedError()

    @property
    def meta(self):
        """Metadata about the dataset."""
        if not hasattr(self, '_meta'):
            self._meta = {}
            meta_fn = os.path.join(self.path, 'meta.json')
            if os.path.exists(meta_fn):
                with open(meta_fn) as meta_file:
                    self._meta.update(json.load(meta_file))
        return self._meta


class QBisTransaction(object):
    """QBiC specific api for registering data at openbis."""
    def __init__(self, transaction):
        self._transaction = transaction

    def bio_sample(self, barcode):
        """Find a sample in openbis by it's barcode.

        This will raise a ValueError if it could not find a sample.
        Since we use the barcode an unique identifier, There should never
        be more than one sample with a given barcode.
        """
        search = self._transaction.getSearchService()
        criteria = SearchCriteria()
        barcode_match = SearchCriteria.MatchClause.createAttributeMatch(
            SearchCriteria.MatchClauseAttribute.CODE, barcode
        )
        criteria.addMatchClause(barcode_match)
        samples = search.searchForSamples(criteria)
        if len(samples) > 1:
            raise RuntimeError(
                "Found more than one sample for barcode %s." % barcode
            )
        if not samples:
            raise ValueError(
                "Could not find a sample for barcode %s" % barcode
            )
        return samples[0]

    def measurement(self, force_create=False, allow_create=False):
        if force_create and allow_create:
            raise ValueError("Only one of force_create and allow_create can "
                             "be specified at once.")
        raise NotImplemented

    def run(self, force_create=False, allow_create=False):
        pass


def process(transaction):
    convert = partial(convert_raw,
                      remote_base=REMOTE_BASE,
                      host=MSCONVERT_HOST,
                      timeout=CONVERSION_TIMEOUT,
                      remote_user=MSCONVERT_USER)

    incoming_path = transaction.getIncoming().getAbsolutePath()
    incoming = DropboxhandlerFile(incoming_path, require_barcode=True)

    name_root, ext = os.path.splitext(incoming.filename)
    if not ext.lower() == '.raw':
        raise ValueError("Not a raw file: %s" % incoming.datapath)

    qtransaction = QBisTransaction(transaction)

    tmpdir = tempfile.mkdtemp()
    try:
        mzml_path = os.path.join(tmpdir, incoming.stem + '.mzML')
        convert(incoming.datapath, mzml_path)

        measurement = incoming.measurement(allow_create=True)
        run = create_run(transaction, space, barcode,
                         measurement=measurement,
                         parent=sample_id)
        dataset_raw = create_dataset(transaction, sample, "Q_MS_RAW_DATA")

        incoming.write_metadata(
            dataset=dataset_raw,
            measurement=measurement,
            run=run
        )

        converted_name = os.path.basename(converted_path)
        mzml_dest = os.path.join(MZML_DROPBOX, converted_name)
        mzml_marker = os.path.join(MZML_DROPBOX, MARKER + converted_name)
        os.rename(mzml_path, mzml_dest)
        open(mzml_marker, "w").close()

        transaction.moveFile(incoming_path, dataset_raw)
    finally:
        shutil.rmtree(tmpdir)
