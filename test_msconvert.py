import os
import tempfile
import shutil
import subprocess
from unittest import TestCase
from unittest.mock import patch, MagicMock


class TestGoodMyModule(TestCase):
    def setUp(self):
        self._ch = MagicMock()
        self._apiv2 = (
            self._ch.systemsx.cisd.etlserver.registrator.api.v2
        )
        self._dto = (
            self._ch.systemsx.cisd.openbis.generic.shared.api.v1.dto
        )
        modules = {
            'ch': self._ch,
            'ch.systemsx.cisd.openbis.generic.shared.api.v1.dto': self._dto,
            'ch.systemsx.cisd.etlserver.registrator.api.v2': self._apiv2
        }

        self.module_patcher = patch.dict('sys.modules', modules)
        self.module_patcher.start()
        from ch.systemsx.cisd.openbis.generic.shared.api.v1.dto import (
            SearchCriteria, SearchSubCriteria
        )
        self.SearchCriteria = SearchCriteria
        self.SearchSubCriteria = SearchSubCriteria
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        self.module_patcher.stop()
        shutil.rmtree(self.tmp)

    def test_ssh(self):
        import etl_msconvert
        etl_msconvert.call_ssh(
            ['echo', '*'], 'localhost', timeout=10
        )[0] == b'*'

        with self.assertRaises(etl_msconvert.TimeoutError):
            etl_msconvert.call_ssh(
                ['sleep', '10'], 'localhost', timeout=1

            )

        with self.assertRaises(subprocess.CalledProcessError):
            etl_msconvert.call_ssh(['blubb'], 'localhost', timeout=1)

    def test_rsync(self):
        import etl_msconvert
        source = self.tmp + '/source'
        dest = self.tmp + '/dest'
        open(source, 'w').close()
        etl_msconvert.rsync(source, dest, timeout=1)
        os.unlink(dest)
        etl_msconvert.rsync(source, dest, source_host='localhost', timeout=1)
        os.unlink(dest)
        etl_msconvert.rsync(source, dest, dest_host='localhost', timeout=1)

    def test_msconvert(self):
        import etl_msconvert
        source = self.tmp + '/source'
        dest = self.tmp + '/dest'
        open(source, 'w').close()
        remote_base = os.path.join(self.tmp, 'remote')
        os.mkdir(remote_base)
        etl_msconvert.convert_raw(source, dest, remote_base=remote_base,
                                  host='localhost', timeout=1, dryrun=True)
