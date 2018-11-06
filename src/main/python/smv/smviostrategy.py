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
import binascii

from pyspark.sql import DataFrame

if sys.version_info >= (3, 4):
    ABC = abc.ABC
else:
    ABC = abc.ABCMeta('ABC', (), {})

# If using Python 2, prefer cPickle because it is faster
# If using Python 3, there is no cPickle (cPickle is now the implementation of pickle)
# see https://docs.python.org/3.1/whatsnew/3.0.html#library-changes
try:
    import cPickle as pickle_lib
except ImportError:
    import pickle as pickle_lib

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
    """Abstract class for persisting data to Hdfs file system
        handling general tasks as file name creation, locking when write, etc. 

        Args:
            smvApp(SmvApp): 
            fqn(str): data/module's FQN/Name
            ver_hex(str): data/module's version hex string
            postfix(str): persisted file's postfix
            file_path(str): parameters "fqn", "ver_hex" and "postfix" are used to create
                a data file path. However if "file_path" is provided, all the other 3 
                parameters are ignored
    """
    def __init__(self, smvApp, fqn=None, ver_hex=None, postfix=None, file_path=None):
        self.smvApp = smvApp
        if (file_path is None):
            versioned_fqn = "{}_{}".format(fqn, ver_hex)
            output_dir = self.smvApp.all_data_dirs().outputDir
            self._file_path = "{}/{}.{}".format(output_dir, versioned_fqn, postfix)
        else:
            self._file_path = file_path

    @abc.abstractmethod
    def _read(self):
        """The raw io read action"""

    def read(self):
        # May add lock or other logic here in future
        return self._read()

    @abc.abstractmethod
    def _write(self, raw_data):
        """The raw io write action"""

    def write(self, dataframe):
        # May add lock or other logic here in future
        self._write(dataframe)

    def isPersisted(self):
        return self.smvApp._jvm.SmvHDFS.exists(self._file_path)

    def remove(self):
        self.smvApp._jvm.SmvHDFS.deleteFile(self._file_path)


class SmvCsvOnHdfsIoStrategy(SmvFileOnHdfsIoStrategy):
    """Persist strategy for using Smv CSV IO handler

        Args:
            smvApp(SmvApp):
            fqn(str): data/module's FQN/Name
            ver_hex(str): data/module's version hex string
            file_path(str): parameters "fqn", "ver_hex" are used to create
                a data file path. However if "file_path" is provided, all the other 2
                parameters are ignored
    """
    def __init__(self, smvApp, fqn, ver_hex, file_path=None):
        super(SmvCsvOnHdfsIoStrategy, self).__init__(smvApp, fqn, ver_hex, 'csv', file_path)

    @property
    def _schema_path(self):
        return re.sub("\.csv$", ".schema", self._file_path)

    def _write(self, raw_data):
        jdf = raw_data._jdf
        # this call creates both .csv and .schema file from the scala side
        self.smvApp.j_smvPyClient.persistDF(self._file_path, jdf)

    def _read(self):
        handler = self.smvApp.j_smvPyClient.createFileIOHandler(self._file_path)

        jdf = handler.csvFileWithSchema(None, self.smvApp.scalaNone())
        return DataFrame(jdf, self.smvApp.sqlContext)

    def isPersisted(self):
        # since within the persistDF call on scala side, schema was written after
        # csv file, so we can use the schema file as a semaphore
        return self.smvApp._jvm.SmvHDFS.exists(self._schema_path)

    def remove(self):
        self.smvApp._jvm.SmvHDFS.deleteFile(self._file_path)
        self.smvApp._jvm.SmvHDFS.deleteFile(self._schema_path)


class SmvJsonOnHdfsIoStrategy(SmvFileOnHdfsIoStrategy):
    def __init__(self, smvApp, path):
        super(SmvJsonOnHdfsIoStrategy, self).__init__(smvApp, None, None, None, path)

    def _read(self):
        return self.smvApp._jvm.SmvHDFS.readFromFile(self._file_path)
    
    def _write(self, rawdata):
        self.smvApp._jvm.SmvHDFS.writeToFile(rawdata, self._file_path)


class SmvPicklableOnHdfsIoStrategy(SmvFileOnHdfsIoStrategy):
    def __init__(self, smvApp, fqn, ver_hex, file_path=None):
        super(SmvPicklableOnHdfsIoStrategy, self).__init__(smvApp, fqn, ver_hex, 'pickle', file_path)

    def _read(self):
        # reverses result of applying _write. see _write for explanation.
        hex_encoded_pickle_as_str = self.smvApp._jvm.SmvHDFS.readFromFile(self._file_path)
        pickled_res_as_str = binascii.unhexlify(hex_encoded_pickle_as_str)
        return pickle_lib.loads(pickled_res_as_str)

    def _write(self, rawdata):
        pickled_res = pickle_lib.dumps(rawdata, -1)
        # pickle may contain problematic characters like newlines, so we
        # encode the pickle it as a hex string
        hex_encoded_pickle = binascii.hexlify(pickled_res)
        # encoding will be a bytestring object if in Python 3, so need to convert it to string
        # str.decode converts string to utf8 in python 2 and bytes to str in Python 3
        hex_encoded_pickle_as_str = hex_encoded_pickle.decode()
        self.smvApp._jvm.SmvHDFS.writeToFile(hex_encoded_pickle_as_str, self._file_path)


class SmvParquetOnHdfsIoStrategy(SmvFileOnHdfsIoStrategy):
    """Persist strategy for using Spark native parquet

        Args:
            smvApp(SmvApp):
            fqn(str): data/module's FQN/Name
            ver_hex(str): data/module's version hex string
            file_path(str): parameters "fqn", "ver_hex" are used to create
                a data file path. However if "file_path" is provided, all the other 2
                parameters are ignored
    """
    def __init__(self, smvApp, fqn, ver_hex, file_path=None):
        super(SmvParquetOnHdfsIoStrategy, self).__init__(smvApp, fqn, ver_hex, 'parquet', file_path)

    @property
    def _semaphore_path(self):
        return re.sub("\.parquet$", ".semaphore", self._file_path)

    def _read(self):
        return self.smvApp.sparkSession.read.parquet(self._file_path)

    def _write(self, rawdata):
        rawdata.write.parquet(self._file_path)
        self.smvApp._jvm.SmvHDFS.createFileAtomic(self._semaphore_path)

    def isPersisted(self):
        return self.smvApp._jvm.SmvHDFS.exists(self._semaphore_path)