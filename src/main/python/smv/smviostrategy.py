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
from smv.utils import scala_seq_to_list
import smv
from smv.error import SmvRuntimeError

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
    """Base class for all module I/O, including read, write and persistence"""
    @abc.abstractmethod
    def read(self):
        """Read data from persisted"""

    @abc.abstractmethod
    def write(self, raw_data):
        """Write data to persist file/db"""

class SmvPersistenceStrategy(SmvIoStrategy):
    """Base class for IO strategy which used for persisting data"""
    @abc.abstractmethod
    def isPersisted(self):
        """Whether the data got successfully persisted before"""

    @abc.abstractmethod
    def remove(self):
        """Remove persisted file(s)"""

class SmvNonOpPersistenceStrategy(SmvPersistenceStrategy):
    """Never persist, isPersisted always returns false"""
    def read(self):
        pass

    def write(self, raw_data):
        pass

    def isPersisted(self):
        return False

    def remove(self):
        pass

class SmvFileOnHdfsPersistenceStrategy(SmvPersistenceStrategy):
    """Abstract class for persisting data to Hdfs file system
        handling general tasks as file name creation, locking when write, etc.

        Args:
            smvApp(SmvApp):
            versioned_fqn(str): data/module's FQN/Name with hash_of_hash
            postfix(str): persisted file's postfix
            file_path(str): parameters "versioned_fqn" and "postfix" are used to create
                a data file path. However if "file_path" is provided, all the other 3
                parameters are ignored
    """
    def __init__(self, smvApp, versioned_fqn=None, postfix=None, file_path=None):
        self.smvApp = smvApp
        if (file_path is None):
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


class SmvCsvPersistenceStrategy(SmvFileOnHdfsPersistenceStrategy):
    """Persist strategy for using Smv CSV IO handler

        Args:
            smvApp(SmvApp):
            versioned_fqn(str): data/module's FQN/Name with hash_of_hash
            file_path(str): parameter "versioned_fqn" is used to create
                a data file path. However if "file_path" is provided, all the other 2
                parameters are ignored
    """
    def __init__(self, smvApp, versioned_fqn, file_path=None):
        super(SmvCsvPersistenceStrategy, self).__init__(smvApp, versioned_fqn, 'csv', file_path)

    @property
    def _schema_path(self):
        return re.sub("\.csv$", ".schema", self._file_path)

    def _write(self, raw_data):
        smv.logger.info("Output path: {}".format(self._file_path))
        # this call creates both .csv and .schema file from the scala side
        record_count = self.smvApp.j_smvPyClient.persistDF(self._file_path, raw_data._jdf)
        smv.logger.info("N: {}".format(record_count))

    def _read(self):
        smv_schema = self.smvApp.smvSchemaObj.fromFile(self.smvApp.j_smvApp.sc(), self._schema_path)

        terminateLogger = self.smvApp._jvm.SmvPythonHelper.getTerminateParserLogger()
        handler = self.smvApp.j_smvPyClient.createFileIOHandler(self._file_path)

        jdf = handler.csvFileWithSchema(None, smv_schema, terminateLogger)
        return DataFrame(jdf, self.smvApp.sqlContext)

    def isPersisted(self):
        # since within the persistDF call on scala side, schema was written after
        # csv file, so we can use the schema file as a semaphore
        return self.smvApp._jvm.SmvHDFS.exists(self._schema_path)

    def remove(self):
        self.smvApp._jvm.SmvHDFS.deleteFile(self._file_path)
        self.smvApp._jvm.SmvHDFS.deleteFile(self._schema_path)


class SmvJsonOnHdfsPersistenceStrategy(SmvFileOnHdfsPersistenceStrategy):
    def __init__(self, smvApp, path):
        super(SmvJsonOnHdfsPersistenceStrategy, self).__init__(smvApp, None, None, path)

    def _read(self):
        return self.smvApp._jvm.SmvHDFS.readFromFile(self._file_path)

    def _write(self, rawdata):
        self.smvApp._jvm.SmvHDFS.writeToFile(rawdata, self._file_path)


class SmvPicklablePersistenceStrategy(SmvFileOnHdfsPersistenceStrategy):
    def __init__(self, smvApp, versioned_fqn, file_path=None):
        super(SmvPicklablePersistenceStrategy, self).__init__(smvApp, versioned_fqn, 'pickle', file_path)

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


class SmvParquetPersistenceStrategy(SmvFileOnHdfsPersistenceStrategy):
    """Persist strategy for using Spark native parquet

        Args:
            smvApp(SmvApp):
            versioned_fqn(str): data/module's FQN/Name with hash_of_hash
            file_path(str): parameter "versioned_fqn" is used to create
                a data file path. However if "file_path" is provided, all the other 2
                parameters are ignored
    """
    def __init__(self, smvApp, versioned_fqn, file_path=None):
        super(SmvParquetPersistenceStrategy, self).__init__(smvApp, versioned_fqn, 'parquet', file_path)

    @property
    def _semaphore_path(self):
        return re.sub("\.parquet$", ".semaphore", self._file_path)

    def _read(self):
        return self.smvApp.sparkSession.read.parquet(self._file_path)

    def _write(self, rawdata):
        rawdata.write.parquet(self._file_path)
        self.smvApp._jvm.SmvHDFS.createFileAtomic(self._semaphore_path)

    def remove(self):
        self.smvApp._jvm.SmvHDFS.deleteFile(self._file_path)
        self.smvApp._jvm.SmvHDFS.deleteFile(self._semaphore_path)

    def isPersisted(self):
        return self.smvApp._jvm.SmvHDFS.exists(self._semaphore_path)


class SmvJdbcIoStrategy(SmvIoStrategy):
    """Persist strategy for spark JDBC IO

        Args:
            smvApp(SmvApp):
            conn_info(SmvConnectionInfo): Jdbc connection info
            table_name(str): the table to read from/write to
            write_mode(str): spark df writer's SaveMode
    """
    def __init__(self, smvApp, conn_info, table_name, write_mode="errorifexists"):
        self.smvApp = smvApp
        self.conn = conn_info
        self.table = table_name
        self.write_mode = write_mode

    def read(self):
        conn = self.conn
        builder = conn._connect_for_read(self.smvApp)

        return builder\
            .option('dbtable', self.table)\
            .load()

    def write(self, raw_data):
        conn = self.conn
        builder = raw_data.write\
            .format("jdbc") \
            .mode(self.write_mode) \
            .option('url', conn.url)

        if (conn.driver is not None):
            builder = builder.option('driver', conn.driver)
        if (conn.user is not None):
            builder = builder.option('user', conn.user)
        if (conn.password is not None):
            builder = builder.option('password', conn.password)

        builder \
            .option("dbtable", self.table) \
            .save()


class SmvHiveIoStrategy(SmvIoStrategy):
    """Persist strategy for spark Hive IO

        Args:
            smvApp(SmvApp):
            conn_info(SmvConnectionInfo): Hive connection info
            table_name(str): the table to read from/write to
            write_mode(str): spark df writer's SaveMode
    """
    def __init__(self, smvApp, conn_info, table_name, write_mode="errorifexists"):
        self.smvApp = smvApp
        self.conn = conn_info
        self.table = table_name
        self.write_mode = write_mode

    def _table_with_schema(self):
        conn = self.conn
        if (conn.schema is None):
            return self.table
        else:
            return "{}.{}".format(conn.schema, self.table)

    def read(self):
        query = "select * from {}".format(self._table_with_schema())
        return self.smvApp.sqlContext.sql(query)

    def write(self, raw_data):
        # TODO: write_mode == 'Ignore'
        _write_mode = self.write_mode.lower()
        raw_data.createOrReplaceTempView("dftable")
        if (_write_mode == 'overwrite' or _write_mode == 'errorifexists'):
            if (_write_mode == 'overwrite'):
                self.smvApp.sqlContext.sql("drop table if exists {}".format(self._table_with_schema()))
            self.smvApp.sqlContext.sql("create table {} as select * from dftable".format(self._table_with_schema()))
        elif (_write_mode == 'append'):
            self.smvApp.sqlContext.sql("insert into table {} select * from dftable".format(self._table_with_schema()))

    # TODO: we should allow persisting intermidiate results in Hive also
    # For that case, however need to specify a convention to store semaphore


class SmvTextOnHdfsIoStrategy(SmvIoStrategy):
    """Simple read/write a small text file on Hdfs"""
    def __init__(self, smvApp, path):
        self.smvApp = smvApp
        self._file_path = path

    def read(self):
        return self.smvApp._jvm.SmvHDFS.readFromFile(self._file_path)

    def write(self, rawdata):
        self.smvApp._jvm.SmvHDFS.writeToFile(rawdata, self._file_path)


class SmvXmlOnHdfsIoStrategy(SmvIoStrategy):
    """Read/write Xml file on Hdfs using Spark DF reader/writer"""
    def __init__(self, smvApp, path, rowTag, schema=None):
        self.smvApp = smvApp
        self._file_path = path
        self._rowTag = rowTag
        self._schema = schema

    def read(self):
        # TODO: look for possibilities to feed to readerLogger
        reader = self.smvApp.sqlContext\
            .read.format('com.databricks.spark.xml')\
            .options(rowTag=self._rowTag)

        # If no schema specified, infer from data
        if (self._schema is not None):
            return reader.load(self._file_path, schema=self._schema)
        else:
            return reader.load(self._file_path)

    def write(self, rawdata):
        raise NotImplementedError("SmvXmlOnHdfsIoStrategy's write method is not implemented")


class SmvSchemaOnHdfsIoStrategy(SmvIoStrategy):
    """Read/write of an SmvSchema file on Hdfs"""
    def __init__(self, smvApp, path, write_mode="overwrite"):
        self.smvApp = smvApp
        self._file_path = path
        self._write_mode = write_mode

    def read(self):
        # To be backward compatable read using spark sc.textFile
        smv_schema = self.smvApp.smvSchemaObj.fromFile(
            self.smvApp.j_smvApp.sc(),
            self._file_path
        )
        return smv_schema

    def _remove(self):
        self.smvApp._jvm.SmvHDFS.deleteFile(self._file_path)

    def write(self, smvSchema):
        schema_str = "\n".join(scala_seq_to_list(self.smvApp._jvm, smvSchema.toStringsWithMeta()))
        if (self._write_mode.lower() == "overwrite"):
            self._remove()
        else:
            raise SmvRuntimeError("Write mode {} is not implemented yet. (Only support overwrite)".format(self._write_mode))

        self.smvApp._jvm.SmvHDFS.writeToFile(schema_str, self._file_path)


class SmvCsvOnHdfsIoStrategy(SmvIoStrategy):
    """Simply read/write of csv, given schema. Not for persisting,
        which should be handled by SmvCsvPersistenceStrategy"""
    def __init__(self, smvApp, path, smvSchema, logger, write_mode="overwrite"):
        self.smvApp = smvApp
        self._file_path = path
        self._smv_schema = smvSchema
        self._logger = logger
        self._write_mode = write_mode

    def read(self):
        handler = self.smvApp.j_smvPyClient.createFileIOHandler(self._file_path)

        jdf = handler.csvFileWithSchema(None, self._smv_schema, self._logger)
        return DataFrame(jdf, self.smvApp.sqlContext)

    def _remove(self):
        self.smvApp._jvm.SmvHDFS.deleteFile(self._file_path)

    def write(self, raw_data):
        jdf = raw_data._jdf

        if (self._write_mode.lower() == "overwrite"):
            self._remove()
        else:
            raise SmvRuntimeError("Write mode {} is not implemented yet. (Only support overwrite)".format(self._write_mode))

        handler = self.smvApp.j_smvPyClient.createFileIOHandler(self._file_path)
        handler.saveAsCsv(jdf, self._smv_schema)
