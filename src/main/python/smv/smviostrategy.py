#
# This file is licensed under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import abc
import sys
import re

from pyspark.sql import DataFrame

if sys.version_info >= (3, 4):
    ABC = abc.ABC
else:
    ABC = abc.ABCMeta('ABC', (), {})

class SmvIoStrategy(ABC):
    @abc.abstractmethod
    def read(self):
        """Read data from persisted"""

    @abc.abstractmethod
    def write(self, raw_data):
        """Write data to persist file/db"""

    @abc.abstractmethod
    def isPersisted(self):
        """Whether the data got successfully persisted before"""

    @abc.abstractmethod
    def remove(self):
        """Remove persisted file(s)"""


class SmvFileOnHdfsIoStrategy(SmvIoStrategy):
    def __init__(self, smvApp, fqn, ver_hex, postfix, file_path=None):
        self.smvApp = smvApp
        if (file_path is None):
            versioned_fqn = "{}_{}".format(fqn, ver_hex)
            output_dir = self.smvApp.all_data_dirs().outputDir
            self._file_path = "{}/{}.{}".format(output_dir, versioned_fqn, postfix)
        else:
            self._file_path = file_path

        self._lock_path = self._file_path + ".lock"

    def _smvLock(self):
        # get a lock for 1 hour
        return self.smvApp._jvm.org.tresamigos.smv.SmvLock(
            self._lock_path,
            3600 * 1000 
        )

    @abc.abstractmethod
    def _write(self, raw_data):
        """The raw io write action"""

    def write(self, dataframe):
        slock = self._smvLock()
        slock.lock()
        try:
            if (self.isPersisted()):
                self.smvApp.log.info("Relying on cached result {} found after lock acquired".format(self._file_path))
            else:
                self.smvApp.log.info("No cached result found. Caching result at {}".format(self._file_path))
                # Delete outputs in case data was partially written previously
                # since `isPersisted` test on schema file, this case only happens when schema was written half way
                self.remove()
                self._write(dataframe)
        finally:
            slock.unlock()

    def isPersisted(self):
        return self.smvApp._jvm.SmvHDFS.exists(self._file_path)

    def remove(self):
        self.smvApp._jvm.SmvHDFS.deleteFile(self._file_path)


class SmvCsvOnHdfsIoStrategy(SmvFileOnHdfsIoStrategy):
    def __init__(self, smvApp, fqn, ver_hex, file_path=None):
        super(SmvCsvOnHdfsIoStrategy, self).__init__(smvApp, fqn, ver_hex, 'csv', file_path)

    @property
    def _schema_path(self):
        return re.sub("\.csv$", ".schema", self._file_path)

    def _write(self, raw_data):
        """  """
        jdf = raw_data._jdf
        self.smvApp.j_smvPyClient.persistDF(self._file_path, jdf)

    def read(self):
        handler = self.smvApp.j_smvPyClient.createFileIOHandler(self._file_path)

        jdf = handler.csvFileWithSchema(None, self.smvApp.scalaNone())
        return DataFrame(jdf, self.smvApp.sqlContext)

    def isPersisted(self):
        handler = self.smvApp.j_smvPyClient.createFileIOHandler(self._file_path)

        try:
            handler.readSchema()
            return True
        except:
            return False

    def remove(self):
        self.smvApp._jvm.SmvHDFS.deleteFile(self._file_path)
        self.smvApp._jvm.SmvHDFS.deleteFile(self._schema_path)


class SmvJsonOnHdfsIoStrategy(SmvFileOnHdfsIoStrategy):
    def __init__(self, smvApp, path):
        super(SmvJsonOnHdfsIoStrategy, self).__init__(smvApp, None, None, None, path)

    def read(self):
        return self.smvApp._jvm.SmvHDFS.readFromFile(self._file_path)
    
    def _write(self, rawdata):
        self.smvApp._jvm.SmvHDFS.writeToFile(rawdata, self._file_path)


class SmvParquetOnHdfsIoStrategy(SmvFileOnHdfsIoStrategy):
    def __init__(self, smvApp, fqn, ver_hex, file_path=None):
        super(SmvParquetOnHdfsIoStrategy, self).__init__(smvApp, fqn, ver_hex, 'parquet', file_path)

    def read(self):
        return self.smvApp.sparkSession.read.parquet(self._file_path)

    def _write(self, rawdata):
        rawdata.write.parquet(self._file_path)