"""
This etl script converts proteomics raw files to mzML and registers
both to the same openBIS sample.

Incoming raw files (of different vendor formats) are converted and the
new mzML is copied to a temporary adress. After that both files are registered at
openbis.

To convert the raw files a virtual windows machine
`qconvert.am10.uni-tuebingen.de` is used. The file is copied
to a temporary directory on that machine with rsync, and
`msconvert` is executed via ssh. The result is copied back
with rsync.

The stdout of this file is redirected to
`~openbis/servers/datastore_server/log/startup_log.txt`
TODO why there?? - it's tradition!
"""

import tempfile
import sys
import os
import time
import re
import shutil
import subprocess
import signal
import datetime
import xml.etree.ElementTree
from functools import partial
import logging
import ch.systemsx.cisd.etlserver.registrator.api.v2
from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import (
    SearchCriteria, SearchSubCriteria
)
try:
    import shlex
    quote = shlex.quote
except AttributeError:
    import pipes
    quote = pipes.quote

logging.basicConfig(level=logging.DEBUG)

# *Q[Project Code]^4[Sample No.]^3[Sample Type][Checksum]*.*
barcode_pattern = re.compile('Q[a-zA-Z0-9]{4}[0-9]{3}[A-Z][a-zA-Z0-9]')
ms_pattern = re.compile('MS[0-9]*Q[A-Z0-9]{4}[0-9]{3}[A-Z][A-Z0-9]')
ms_prefix_pattern = re.compile('MS[0-9]*')

MARKER = '.MARKER_is_finished_'
MZML_TMP = "/mnt/DSS1/dropboxes/ms_convert_tmp/"
DROPBOX_PATH = "/mnt/DSS1/openbis_dss/QBiC-convert-register-ms-vendor-format/"
VENDOR_FORMAT_EXTENSIONS = {'.raw':'RAW_THERMO', '.d':'D_BRUKER','.wiff':'WIFF_SCIEX'}
MSCONVERT_HOST = "qmsconvert.am10.uni-tuebingen.de"
MSCONVERT_USER = "qbic"
REMOTE_BASE = "/cygdrive/d/etl-convert"
CONVERSION_TIMEOUT = 7200

# Standard BSA sample and experiment
BSA_MPC_SPACE = "MFT_QC_MPC"
BSA_MPC_PROJECT = "QCMPC"

BSA_MPC_EXPERIMENT_ID = "/MFT_QC_MPC/QCMPC/QCMPCE4"
BLANK_MPC_EXPERIMENT_ID = "/MFT_QC_MPC/QCMPC/QCMPCE6"

BSA_MPC_BARCODE = "QCMPC002AO"
BLANK_MPC_BARCODE = "QCMPC003AW"

bsa_run_pattern = re.compile(BSA_MPC_BARCODE)
blank_run_pattern = re.compile(BLANK_MPC_BARCODE)

try:
    TimeoutError
except NameError:
    class TimeoutError(Exception):
        pass


class ConversionError(RuntimeError):
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
        raise _Alarm("Command timeout: %s" % cmd)

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

    if source_user:
        source = "%s@%s" %(source_user, source)

    if dest_host:
        dest = "%s:%s" % (dest_host, dest)

    if dest_user:
        dest = "%s@%s" % (dest_user, dest)

    cmd = ['rsync', '--', source, dest]
    if extra_options:
        cmd = cmd[0:1] + extra_options + cmd[1:]
    return check_output(cmd, timeout=timeout)


def call_ssh(cmd, host, user=None, timeout=None, cwd=None):
    """Execute cmd on host via ssh.

    Return stdout and stderr of the remote command. The command must
    be a list containing the name of the program and the arguments.
    """
    if user:
        host = "%s@%s" % (user, host)
    full_cmd = ['ssh', host, '-oBatchMode=yes', '--']
    if cwd:
        full_cmd.append("cd %s;" % cwd)
    full_cmd.extend(quote(i) for i in cmd)
    return check_output(full_cmd, timeout=timeout)


def convert_raw(raw_path, dest, remote_base, host, timeout, user=None,
                msconvert_options=None, dryrun=False):
    """Convert a raw file to mzml on a remote machine.

    Uses ssh to run remote commands. Creates a temporary directory on
    the remote host in `remote_base` and use rsync to copy the raw file
    into that directory. Execute msconvert via ssh. If this finishes
    before the timeout runs out, the result is copied to `dest`. If
    msconvert times out, this function will try to remove the remote
    temporary directory and raise a `ConversionError`.
    """
    ssh = partial(call_ssh, host=host, user=user, timeout=timeout)
    rsync_base = partial(rsync, timeout=timeout, extra_options=['-qr'])
    rsync_to = partial(rsync_base, dest_user=user, dest_host=host)
    rsync_from = partial(rsync_base, source_host=host, source_user=user)

    remote_dir, _ = ssh(['mktemp', '-dq', '-p', remote_base])
    try:
        try:
            remote_dir = remote_dir.decode().strip()
        except AttributeError:
            remote_dir = remote_dir.strip()
        remote_file = os.path.join(remote_dir, os.path.basename(raw_path))
        rsync_to(source=raw_path, dest=remote_dir)#changed this from remote_file to remote_dir so it works for folders (.d)

        remote_mzml = os.path.splitext(remote_file)[0] + '.mzML'
        if dryrun:
            ssh(['cp', remote_file, remote_mzml])
        else:
            raw_name = os.path.basename(raw_path)
            ssh(['msconvert', raw_name, '--outfile', remote_mzml], cwd=remote_dir)
        rsync_from(source=remote_mzml, dest=dest)
    finally:
        try:
            ssh(["rm", "-rf", remote_dir])
        except Exception:
            logging.exception("Could not remove remote dir.")


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
        return True

def parse_timestamp_easy(mzml_path):
    mzml = open(mzml_path)
    time = None
    for line in mzml:
        if "<run id=" in line:
            for token in line.split(" "):
                if "startTimeStamp=" in token:
                    xsdDateTime = token.split('"')[1]
                    time = datetime.datetime.strptime(xsdDateTime, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
            break
            mzml.close()
    return time

def parse_instrument_accession(mzml_path):
    print ""
    mzml = open(mzml_path)
    accession = None
    out = True
    for line in mzml:
        if "<instrumentConfigurationList" in line or 'id="CommonInstrumentParams">' in line:
            out = False
        if "</referenceableParamGroup>" in line or "</instrumentConfiguration>" in line:
            out = True
        if not out and '<cvParam cvRef="MS"' in line:
            print line
            line = line.split(" ")
            for token in line:
                if "accession=" in token:
                    accession = token.split('"')[1]
            mzml.close()
            break
    print "accession: "+accession
    return accession

def parse_timestamp_from_mzml(mzml_path):
    schema = '{http://psi.hupo.org/ms/mzml}'
    for event, element in xml.etree.ElementTree.iterparse(mzml_path):
        if element.tag == schema+'run':
            xsdDateTime = element.get('startTimeStamp')
            element.clear()
            break
        element.clear() # remove unused xml elements
    time = None
    try:
        time = datetime.datetime.strptime(xsdDateTime, '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
    except TypeError:
        print "no startTimeStamp found"
    return time

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

    @property
    def meta(self):
        """Metadata about the dataset."""
        if not hasattr(self, '_meta'):
            self._meta = {}
            meta_fn = os.path.join(self.path, 'meta.json')
            if os.path.exists(meta_fn):
                meta_file = open(meta_fn)
                try:
                    self._meta.update(json.load(meta_file))
                finally:
                    meta_file.close()
        return self._meta


class QBisRegistration(object):
    """QBiC specific api for registering data at openbis."""
    def __init__(self, transaction, barcode):
        self._transaction = transaction
        self.barcode = barcode
        self.space, self.project, self.bio_sample = self._bio_sample(barcode)

    def _bio_sample(self, barcode):
        """Find a sample in openbis by it's barcode.

        Since we use the barcode an unique identifier, there should never
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
        sample = samples[0]
        return sample.getSpace(), self.barcode[:5], sample

    def measurement(self, force_create=False, allow_create=False):
        for measurement in self.measurements():
            for run in self.runs():
                if measurement.getExperimentIdentifier() == run:
                    return

    def run(self, force_create=False, allow_create=False):
        pass

    def runs(self):
        """Return *all* runs in the db.

        TODO this is madness!! At least, this should only return
        runs in this project.
        """
        search = self.transaction.getSearchService()
        criteria = SearchCriteria()
        criteria.addMatchClause(
            SearchCriteria.MatchClause.createAttributeMatch(
                SearchCriteria.MatchClauseAttribute.TYPE,
                'Q_MS_RUN'
            )
        )
        return search.searchForSamples(criteria)

    def measurements(self):
        """Return all measurements in this project."""
        exp_type = 'Q_MS_MEASUREMENT'
        path = "/%s/%s" % (space, project)
        search = self.transaction.getSearchService()
        exps = search.listExperiments(path)
        return [exp for exp in exps if exp.getExperimentType() == exp_type]

def isCurrentMSRun(tr, parentExpID, msExpID):
    """Ask Andreas"""
    search_service = tr.getSearchService()
    sc = SearchCriteria()
    sc.addMatchClause(
        SearchCriteria.MatchClause.createAttributeMatch(
            SearchCriteria.MatchClauseAttribute.TYPE, "Q_MS_RUN"
        )
    )
    foundSamples = search_service.searchForSamples(sc)
    for samp in foundSamples:
        currentMSExp = samp.getExperiment()
        if currentMSExp.getExperimentIdentifier() == msExpID:
            for parID in samp.getParentSampleIdentifiers():
                parExp = (tr.getSampleForUpdate(parID)
                            .getExperiment()
                            .getExperimentIdentifier())
                if parExp == parentExpID:
                    return True
    return False

class SampleAlreadyCreatedError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

class SampleNotFoundError(Exception):

    def __init__(self, value):
        self.value = value

    def __str__(self):
        return self.value

def createRawDataSet(transaction, incomingPath, sample, format, time_stamp):
    rawDataSet = transaction.createNewDataSet("Q_MS_RAW_DATA")
    rawDataSet.setPropertyValue("Q_MS_RAW_VENDOR_TYPE", format)
    if time_stamp:
        rawDataSet.setPropertyValue("Q_MEASUREMENT_START_DATE", time_stamp)
    rawDataSet.setMeasuredData(False)
    rawDataSet.setSample(sample)
    transaction.moveFile(incomingPath, rawDataSet)

def GZipAndMoveMZMLDataSet(transaction, filepath, sample, file_exists = False):
    mzmlDataSet = transaction.createNewDataSet("Q_MS_MZML_DATA")
    #TODO more properties from mzml?
    time_stamp = parse_timestamp_easy(filepath)

    mzmlDataSet.setMeasuredData(False)
    mzmlDataSet.setSample(sample)
    if time_stamp:
        mzmlDataSet.setPropertyValue("Q_MEASUREMENT_START_DATE", time_stamp)
    if not file_exists:
        subprocess.call(["gzip", filepath])
    zipped = filepath+".gz"
    transaction.moveFile(zipped, mzmlDataSet)
    return time_stamp

'''Metadata extraction written by Chris, handles everything but conversion. Support for batch upload (no metadata here) by Andreas'''
def handleImmunoFiles(transaction):

    xmltemplate = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?> <qproperties> <qfactors> <qcategorical label=\"technical_replicate\" value=\"%s\"/> <qcategorical label=\"workflow_type\" value=\"%s\"/> </qfactors> </qproperties>"

    context = transaction.getRegistrationContext().getPersistentMap()

    # Get the incoming path of the transaction
    incomingPath = transaction.getIncoming().getAbsolutePath()

    key = context.get("RETRY_COUNT")
    if (key == None):
        key = 1

    # Get the name of the incoming file
    name = transaction.getIncoming().getName()

    code = barcode_pattern.findall(name)[0]
    if extract_barcode(code):
        project = code[:5]
        experiment = code[1:5]
        parentCode = code[:10]
    else:
        raise ValueError("Invalid barcode: %s" % code)        

    data_files = []
    metadataFile = None
    for root, subFolders, files in os.walk(incomingPath):
        if subFolders:
            subFolder = subFolders[0]
        for f in files:
            stem, ext = os.path.splitext(f)
            if ext.lower()=='.tsv':
                metadataFile = open(os.path.join(root, f), 'U')
            if ext.lower() in VENDOR_FORMAT_EXTENSIONS:
                data_files.append(os.path.join(root, f))
    # Metadata file: this was registered by hand, metadata needs to be read
    if metadataFile:
        line = metadataFile.readline()

        #info needed in the for loop
        search_service = transaction.getSearchService()
        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, parentCode))
        foundSamples = search_service.searchForSamples(sc)
        space = foundSamples[0].getSpace()
        existingExperimentIDs = []
        existingExperiments = search_service.listExperiments("/" + space + "/" + project)

        #test for existing samples before conversion step
        run = 0
        sampleExists = True
        while sampleExists:
            run += 1
            newSampleID = '/' + space + '/' + 'MS'+ str(run) + parentCode
            sampleExists = transaction.getSampleForUpdate(newSampleID)
                #raise SampleAlreadyCreatedError("Sample "+newSampleID+" already exists.")

        # start at first ms run id not yet found. datasets might be registered more than once, if they arrive multiple times
        #run = 1
        for line in metadataFile:
            splitted = line.split('\t')
            fileName = splitted[0]
            instr = splitted[1] # Q_MS_DEVICE (controlled vocabulary)
            date_input = splitted[2]
            share = splitted[3]
            comment = splitted[4]
            method = splitted[5]
            repl = splitted[6]
            wf_type = splitted[7]

            date = datetime.datetime.strptime(date_input, "%y%m%d").strftime('%Y-%m-%d')
            parentSampleIdentifier = foundSamples[0].getSampleIdentifier()
            sa = transaction.getSampleForUpdate(parentSampleIdentifier)

            # register new experiment and sample
            numberOfExperiments = len(search_service.listExperiments("/" + space + "/" + project)) + run

            for eexp in existingExperiments:
                existingExperimentIDs.append(eexp.getExperimentIdentifier())

            newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(numberOfExperiments)

            while newExpID in existingExperimentIDs:
                numberOfExperiments += 1 
                newExpID = '/' + space + '/' + project + '/' + project + 'E' +str(numberOfExperiments)
            existingExperimentIDs.append(newExpID)

            newMSExperiment = transaction.createNewExperiment(newExpID, "Q_MS_MEASUREMENT")
            newMSExperiment.setPropertyValue('Q_CURRENT_STATUS', 'FINISHED')
            newMSExperiment.setPropertyValue('Q_MS_DEVICE', instr)
            newMSExperiment.setPropertyValue('Q_MEASUREMENT_FINISH_DATE', date)
            newMSExperiment.setPropertyValue('Q_EXTRACT_SHARE', share)
            newMSExperiment.setPropertyValue('Q_ADDITIONAL_INFO', comment)
            newMSExperiment.setPropertyValue('Q_MS_LCMS_METHOD', method.replace('@','').replace('+', '').replace('_100ms', ''))

            newSampleID = '/' + space + '/' + 'MS'+ str(run) + parentCode

            newMSSample = transaction.createNewSample(newSampleID, "Q_MS_RUN")
            newMSSample.setParentSampleIdentifiers([sa.getSampleIdentifier()])
            newMSSample.setExperiment(newMSExperiment)
            properties = xmltemplate % (repl, wf_type)
            newMSSample.setPropertyValue('Q_PROPERTIES', properties)
        
            run += 1
            tmpdir = tempfile.mkdtemp(dir=MZML_TMP)
            raw_path = os.path.join(incomingPath, os.path.join(name, fileName))
            stem, ext = os.path.splitext(fileName)

            #test if some or all files are left over from earlier conversion attempt (e.g. failure of transaction, but successful conversion)
            mzml_name = stem + '.mzML'
            mzml_dest = os.path.join(DROPBOX_PATH, mzml_name)
            gzip_dest = os.path.join(DROPBOX_PATH, mzml_name + '.gz')

            #conversion process ended successfully if gzip process deleted mzml and created mzml.gz
            converted_exists = not os.path.isfile(mzml_dest) and os.path.isfile(gzip_dest)
            if ext.lower() in VENDOR_FORMAT_EXTENSIONS:
                    openbis_format_code = VENDOR_FORMAT_EXTENSIONS[ext.lower()]
            else:
                raise ValueError("Invalid incoming file %s" % incomingPath)

            if True: #not converted_exists: (needed for mzml parsing)
                try:
                    convert = partial(convert_raw,
                            remote_base=REMOTE_BASE,
                            host=MSCONVERT_HOST,
                            timeout=CONVERSION_TIMEOUT,
                            user=MSCONVERT_USER)

                    mzml_path = os.path.join(tmpdir, mzml_name)
                    convert(raw_path, mzml_path)

                    os.rename(mzml_path, mzml_dest)
                finally:
                    shutil.rmtree(tmpdir)
            # parse some information from mzml
            instrument_accession = parse_instrument_accession(mzml_dest)
            time_stamp = GZipAndMoveMZMLDataSet(transaction, mzml_dest, newMSSample, converted_exists)
            if instrument_accession:
                newMSExperiment.setPropertyValue('Q_ONTOLOGY_INSTRUMENT_ID', instrument_accession)
            createRawDataSet(transaction, raw_path, newMSSample, openbis_format_code, time_stamp)
            
    # no metadata file: just one RAW file to convert and attach to samples
    else:
        search_service = transaction.getSearchService()
        # TODO allow complex barcodes in dropboxhandler so this can be changed to be more stable
        prefix = ms_prefix_pattern.findall(name)[0]
        ms_code = prefix+code
        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, ms_code))
        foundSamples = search_service.searchForSamples(sc)
        ms_samp = transaction.getSampleForUpdate(foundSamples[0].getSampleIdentifier())

        tmpdir = tempfile.mkdtemp(dir=MZML_TMP)
        raw_path = os.path.join(incomingPath, name)
        print raw_path
        stem, ext = os.path.splitext(name)
        if ext.lower() in VENDOR_FORMAT_EXTENSIONS:
            openbis_format_code = VENDOR_FORMAT_EXTENSIONS[ext.lower()]
        else:
            raise ValueError("Invalid incoming file %s" % incomingPath)
        try:
            convert = partial(convert_raw,
                    remote_base=REMOTE_BASE,
                    host=MSCONVERT_HOST,
                    timeout=CONVERSION_TIMEOUT,
                    user=MSCONVERT_USER)
            mzml_path = os.path.join(tmpdir, stem + '.mzML')
            convert(raw_path, mzml_path)

            mzml_name = os.path.basename(mzml_path)
            mzml_dest = os.path.join(DROPBOX_PATH, mzml_name)

            os.rename(mzml_path, mzml_dest)
        finally:
            shutil.rmtree(tmpdir)

        instrument_accession = parse_instrument_accession(mzml_dest)
        time_stamp = GZipAndMoveMZMLDataSet(transaction, mzml_dest, ms_samp)
        if instrument_accession:
                exp = ms_samp.getExperimentForUpdate()
                old_accession = MSRawExperiment.getPropertyValue('Q_ONTOLOGY_INSTRUMENT_ID')
                if old_accession and old_accession is not accession:
                    exp.setPropertyValue('Q_ONTOLOGY_INSTRUMENT_ID', instrument_accession)
                else:
                    raise ValueError("Found instrument accession "+instrument_accession+" in mzml, but "+old_accession+" in experiment!")

        createRawDataSet(transaction, raw_path, ms_samp, openbis_format_code, time_stamp)


def handle_QC_Run(transaction):
    # Get the name of the incoming file
    name = transaction.getIncoming().getName()
    incomingPath = transaction.getIncoming().getAbsolutePath()

    if len(bsa_run_pattern.findall(name)) > 0:
        code = BSA_MPC_BARCODE
        # The MS experiment
        msExp = transaction.getExperiment(BSA_MPC_EXPERIMENT_ID)
    else:
        code = BLANK_MPC_BARCODE
        # The MS experiment
        msExp = transaction.getExperiment(BLANK_MPC_EXPERIMENT_ID)

    stem, ext = os.path.splitext(name)

    # Convert the raw file and write it to an mzml tmp folder.
    # Sadly, I can not see a way to make this part of the transaction.
    tmpdir = tempfile.mkdtemp(dir=MZML_TMP)
    try:
        convert = partial(convert_raw,
                  remote_base=REMOTE_BASE,
                  host=MSCONVERT_HOST,
                  timeout=CONVERSION_TIMEOUT,
                  user=MSCONVERT_USER)
        if ext.lower() in VENDOR_FORMAT_EXTENSIONS:
            openbis_format_code = VENDOR_FORMAT_EXTENSIONS[ext.lower()]
        else:
            raise ValueError("Invalid incoming file %s" % incomingPath)

        mzml_path = os.path.join(tmpdir, stem + '.mzML')
        raw_path = os.path.join(incomingPath, name)
        convert(raw_path, mzml_path)

        mzml_name = os.path.basename(mzml_path)
        mzml_dest = os.path.join(DROPBOX_PATH, mzml_name)

        os.rename(mzml_path, mzml_dest)
    finally:
        shutil.rmtree(tmpdir)

    #TODO create new ms sample? if so, use normal qbic barcodes?
    msCode = "MS"+code

    search_service = transaction.getSearchService()
    sc = SearchCriteria()
    pc = SearchCriteria()
    pc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.PROJECT, BSA_MPC_PROJECT));
    sc.addSubCriteria(SearchSubCriteria.createExperimentCriteria(pc))

    foundSamples = search_service.searchForSamples(sc)

    run = 1
    for samp in foundSamples:
        if samp.getSampleType() == "Q_MS_RUN":
            existingRun = int(samp.getCode().split("_")[-1])
            if existingRun >= run:
                run = existingRun + 1

    msSample = transaction.createNewSample('/' + BSA_MPC_SPACE + '/' + msCode + "_" + str(run), "Q_MS_RUN")
    #set parent sample

    msSample.setParentSampleIdentifiers(["/"+BSA_MPC_SPACE+"/"+code])
    msSample.setExperiment(msExp)

    time_stamp = GZipAndMoveMZMLDataSet(transaction, mzml_dest, msSample)
    createRawDataSet(transaction, raw_path, msSample, openbis_format_code, time_stamp)
    
    for f in os.listdir(incomingPath):
        if ".testorig" in f:
            os.remove(os.path.realpath(os.path.join(incomingPath, f)))

def process(transaction):
    """Ask Andreas"""
    context = transaction.getRegistrationContext().getPersistentMap()

    # Get the incoming path of the transaction
    incomingPath = transaction.getIncoming().getAbsolutePath()
    name = transaction.getIncoming().getName()
    # If special format from Immuno Dropbox handle separately
    immuno = False 
    qc_run = len(bsa_run_pattern.findall(name)+blank_run_pattern.findall(name)) > 0
    for f in os.listdir(incomingPath):
        if "source_dropbox.txt" in f:
            source_file = open(os.path.join(incomingPath, f))
            source = source_file.readline()
            if "cloud-immuno" in source or "qeana18-immuno" in source:
                immuno = True
                handleImmunoFiles(transaction)
    if not immuno and qc_run:
        handle_QC_Run(transaction)
    if not immuno and not qc_run:
        # Get the name of the incoming file

        code = barcode_pattern.findall(name)[0]
        if extract_barcode(code):
            project = code[:5]
        else:
            raise ValueError("Invalid barcode: %s" % code)
    
        stem, ext = os.path.splitext(name)
        search_service = transaction.getSearchService()

        # Convert the raw file and write it to an mzml tmp folder.
        # Sadly, I can not see a way to make this part of the transaction.
        tmpdir = tempfile.mkdtemp(dir=MZML_TMP)
        try:
            convert = partial(convert_raw,
                      remote_base=REMOTE_BASE,
                      host=MSCONVERT_HOST,
                      timeout=CONVERSION_TIMEOUT,
                      user=MSCONVERT_USER)
            if ext.lower() in VENDOR_FORMAT_EXTENSIONS:
                openbis_format_code = VENDOR_FORMAT_EXTENSIONS[ext.lower()]
            else:
                raise ValueError("Invalid incoming file %s" % incomingPath)

            mzml_path = os.path.join(tmpdir, stem + '.mzML')
            raw_path = os.path.join(incomingPath, name) #raw file has the same name as the incoming folder, this is the path to this file!
            convert(raw_path, mzml_path)

            mzml_name = os.path.basename(mzml_path)
            mzml_dest = os.path.join(DROPBOX_PATH, mzml_name)

            os.rename(mzml_path, mzml_dest)
        finally:
            shutil.rmtree(tmpdir)

        # Try to find an existing MS sample
        sc = SearchCriteria()
        sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, "MS"+code))
        foundSamples = search_service.searchForSamples(sc)

        if len(foundSamples) > 0:
            msSample = transaction.getSampleForUpdate(foundSamples[0].getSampleIdentifier())
            #msSample.getExperiment() update experiment here
        else:
            # Find the test sample or ms sample without prefix (wash runs)
            sc = SearchCriteria()
            sc.addMatchClause(SearchCriteria.MatchClause.createAttributeMatch(SearchCriteria.MatchClauseAttribute.CODE, code))
            foundSamples = search_service.searchForSamples(sc)
            if len(foundSamples) < 1:
                raise SampleNotFoundError("Neither the sample "+code+" nor MS"+code+" was found.")
            sampleIdentifier = foundSamples[0].getSampleIdentifier()
            space = foundSamples[0].getSpace()
            sType = foundSamples[0].getSampleType()
            sa = transaction.getSampleForUpdate(sampleIdentifier)
            MSRawExperiment = None

            if(sType == "Q_MS_RUN"):
                msSample = sa
            else:
                # get or create MS-specific experiment/sample and
                # attach to the test sample
                expType = "Q_MS_MEASUREMENT"
                experiments = search_service.listExperiments("/" + space + "/" + project)
                experimentIDs = []
                for exp in experiments:
                    experimentIDs.append(exp.getExperimentIdentifier())
                    if exp.getExperimentType() == expType:
                        if isCurrentMSRun(
                            transaction,
                            sa.getExperiment().getExperimentIdentifier(),
                            exp.getExperimentIdentifier()
                        ):
                            MSRawExperiment = exp
                # no existing experiment for samples of this sample preparation found
                if not MSRawExperiment:
                    expID = experimentIDs[0]
                    i = 0
                    while expID in experimentIDs:
                        i += 1
                        expNum = len(experiments) + i
                        expID = '/' + space + '/' + project + \
                            '/' + project + 'E' + str(expNum)
                    MSRawExperiment = transaction.createNewExperiment(expID, expType)
                # create new ms sample
                msSample = transaction.createNewSample('/' + space + '/' + "MS"+code, "Q_MS_RUN")
                msSample.setParentSampleIdentifiers([sa.getSampleIdentifier()])
                msSample.setExperiment(MSRawExperiment)

        instrument_accession = parse_instrument_accession(mzml_dest)
        time_stamp = GZipAndMoveMZMLDataSet(transaction, mzml_dest, msSample)
        if instrument_accession:
            old_accession = None
            if not MSRawExperiment:
                MSRawExperiment = getExperimentForUpdate()
                old_accession = MSRawExperiment.getPropertyValue('Q_ONTOLOGY_INSTRUMENT_ID')
            if old_accession and old_accession is not accession:
                print "setting accession"
                MSRawExperiment.setPropertyValue('Q_ONTOLOGY_INSTRUMENT_ID', instrument_accession)
            else:
                raise ValueError("Found instrument accession "+instrument_accession+" in mzml, but "+old_accession+" in experiment!")

        createRawDataSet(transaction, raw_path, msSample, openbis_format_code, time_stamp)

        for f in os.listdir(incomingPath):
            if ".testorig" in f:
                os.remove(os.path.realpath(os.path.join(incomingPath, f)))