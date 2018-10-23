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

    @abc.abstractmethod
    def allOutput(self):
        """list of all output from this IO"""

# TODO: add lock, add publish
class SmvCsvOnHdfsIoStrategy(SmvIoStrategy):
    def __init__(self, smvApp, fqn, ver_hex):
        self.smvApp = smvApp
        self.fqn = fqn
        self.versioned_fqn = "{}_{}".format(fqn, ver_hex)

    def _output_base(self):
        output_dir = self.smvApp.all_data_dirs().outputDir
        return "{}/{}".format(output_dir, self.versioned_fqn)

    def _csv_path(self):
        return self._output_base() + ".csv"
    
    def _schema_path(self):
        return self._output_base() + ".schema"

    def _publish_csv_path(self):
        pubdir = self.smvApp.all_data_dirs().publishDir
        version = self.smvApp.all_data_dirs().publishVersion
        return "{}/{}/{}.csv".format(pubdir, version, self.fqn)

    def _lock_path(self):
        return self._csv_path() + ".lock"

    def _smvLock(self):
        # get a lock for 1 hour
        return self.smvApp._jvm.org.tresamigos.smv.SmvLock(
            self._lock_path(),
            3600 * 1000 
        )

    def read(self):
        handler = self.smvApp.j_smvPyClient.createFileIOHandler(self._csv_path())

        jdf = handler.csvFileWithSchema(None, self.smvApp.scalaNone())
        return DataFrame(jdf, self.smvApp.sqlContext)

    def write(self, dataframe):
        jdf = dataframe._jdf
        # TODO: add log
        slock = self._smvLock()

        slock.lock()
        try:
            if (self.isWritten()):
                self.smvApp.log.info("Relying on cached result {} for {} found after lock acquired".format(self._csv_path(), self.fqn))
            else:
                self.smvApp.log.info("No cached result found for {}. Caching result at {}".format(self.fqn, self._csv_path()))
                # Delete outputs in case data was partially written previously
                # since `isWritten` test on schema file, this case only happens when schema was written half way
                self.remove()
                self.smvApp.j_smvPyClient.persistDF(self._csv_path(), jdf)
        finally:
            slock.unlock()

    def isPersisted(self):
        handler = self.smvApp.j_smvPyClient.createFileIOHandler(self._csv_path())

        try:
            handler.readSchema()
            return True
        except:
            return False

    def remove(self):
        self.smvApp._jvm.SmvHDFS.deleteFile(self._csv_path())
        self.smvApp._jvm.SmvHDFS.deleteFile(self._schema_path())

    def allOutput(self):
        return [
            self._csv_path(),
            self._schema_path()
        ]

class SmvJsonOnHdfsIoStrategy(SmvIoStrategy):
    def __init__(self, smvApp, path):
        self._jvm = smvApp._jvm
        self.path = path

    def read(self):
        return self._jvm.SmvHDFS.readFromFile(self.path)
    
    def write(self, rawdata):
        self._jvm.SmvHDFS.writeToFile(rawdata, self.path)

    def isPersisted(self):
        return self._jvm.SmvHDFS.exists(self.path)

    def remove(self):
        self._jvm.SmvHDFS.deleteFile(self.path)

    def allOutput(self):
        return [self.path]