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
import os
import json

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from py4j.protocol import Py4JJavaError

import smv
from smv.iomod.base import SmvInput, AsTable, AsFile
from smv.smvmodule import SparkDfGenMod
from smv.smviostrategy import SmvJdbcIoStrategy, SmvHiveIoStrategy, \
    SmvSchemaOnHdfsIoStrategy, SmvCsvOnHdfsIoStrategy, SmvTextOnHdfsIoStrategy,\
    SmvXmlOnHdfsIoStrategy
from smv.dqm import SmvDQM
from smv.utils import lazy_property, smvhash
from smv.error import SmvRuntimeError


class SmvJdbcInputTable(SparkDfGenMod, SmvInput, AsTable):
    """
        User need to implement

            - connectionName
            - tableName
    """

    def connectionType(self):
        return 'jdbc'

    def instanceValHash(self):
        """Jdbc input hash depends on connection and table name
        """

        _conn_hash = self.connectionHash()
        smv.logger.debug("{} connectionHash: {}".format(self.fqn(), _conn_hash))

        _table_hash = self.tableNameHash()
        smv.logger.debug("{} tableNameHash: {}".format(self.fqn(), _table_hash))

        res = _conn_hash + _table_hash
        return res

    def _get_input_data(self):
        conn = self.get_connection()
        return SmvJdbcIoStrategy(self.smvApp, conn, self.tableName()).read()


class SmvHiveInputTable(SparkDfGenMod, SmvInput, AsTable):
    """
        User need to implement:

            - connectionName
            - tableName
    """

    def connectionType(self):
        return 'hive'

    def _get_input_data(self):
        conn = self.get_connection()
        return SmvHiveIoStrategy(self.smvApp, conn, self.tableName()).read()

    def instanceValHash(self):
        """Hive input hash depends on connection and table name
        """

        _conn_hash = self.connectionHash()
        smv.logger.debug("{} connectionHash: {}".format(self.fqn(), _conn_hash))

        _table_hash = self.tableNameHash()
        smv.logger.debug("{} tableNameHash: {}".format(self.fqn(), _table_hash))

        res = _conn_hash + _table_hash
        return res



class InputFileWithSchema(SmvInput, AsFile):
    """Base class for input files which has input schema"""

    def schemaConnectionName(self):
        """Optional method to specify a schema connection"""
        return None

    def schemaFileName(self):
        """Optional name of the schema file relative to the
            schema connection path
        """
        return None

    def userSchema(self):
        """User-defined schema

            Override this method to define your own schema for the target file.
            Schema declared in this way take priority over .schema files. For Csv
            input, Schema should be specified in the format
            "colName1:colType1;colName2:colType2"

            Returns:
                (string):
        """
        return None

    def _get_schema_connection(self):
        """Return a schema connection with the following priority:

            - User specified in current module through schemaConnectionName method
            - Configured in the global props files with prop key "smv.schemaConn"
            - Connection for data (user specified through connectionName method)

            Since in some cases user may not have write access to the data folder,
            need to provide more flexibility on where the schema files can come from.
        """
        name = self.schemaConnectionName()
        props = self.smvApp.py_smvconf.merged_props()
        global_schema_conn = props.get('smv.schemaConn')
        if (name is not None):
            return self.smvApp.get_connection_by_name(name)
        elif (global_schema_conn is not None):
            return self.smvApp.get_connection_by_name(global_schema_conn)
        else:
            return self.get_connection()

    def _get_schema_file_name(self):
        """The schema_file_name is determined by the following logic

                - schemaFileName
                - fileName replace the post-fix to schema
        """
        if (self.schemaFileName() is not None):
            return self.schemaFileName()
        else:
            return self.fileName().rsplit(".", 1)[0] + ".schema"

    def _full_path(self):
        return os.path.join(self.get_connection().path, self.fileName())

    def _full_schema_path(self):
        return os.path.join(self._get_schema_connection().path,
            self._get_schema_file_name())

    def _file_hash(self, path, msg):
        _file_path_hash = smvhash(path)
        smv.logger.debug("{} {} file path hash: {}".format(self.fqn(), msg, _file_path_hash))

        # It is possible that the file doesn't exist
        try:
            _m_time = self.smvApp._jvm.SmvHDFS.modificationTime(path)
        except Py4JJavaError:
            _m_time = 0

        smv.logger.debug("{} {} file mtime: {}".format(self.fqn(), msg, _m_time))

        res = _file_path_hash + _m_time
        return res

    def instanceValHash(self):
        """Hash of file with schema include data file hash (path and mtime),
            and schema hash (userSchema or schema file)
        """

        _data_file_hash = self._file_hash(self._full_path(), "data")
        smv.logger.debug("{} data file hash: {}".format(self.fqn(), _data_file_hash))

        if (self.userSchema() is not None):
            _schema_hash = smvhash(self.userSchema())
        else:
            _schema_hash = self._file_hash(self._full_schema_path(), "schema")
        smv.logger.debug("{} schema hash: {}".format(self.fqn(), _schema_hash))

        res = _data_file_hash + _schema_hash
        return res



class SmvXmlInputFile(SparkDfGenMod, InputFileWithSchema):
    """Input from file in XML format
        User need to implement:

            - rowTag: required
            - connectionName: required
            - fileName: required
            - schemaConnectionName: optional
            - schemaFileName: optional
            - userSchema: optional
    """

    @abc.abstractmethod
    def rowTag(self):
        """XML tag for identifying a record (row)"""
        pass

    def _schema(self):
        """load schema from userSchema (as a json string) or a json file"""
        def str_to_schema(s):
            return StructType.fromJson(json.loads(s))

        if (self.userSchema() is not None):
            return str_to_schema(self.userSchema())
        else:
            s_path = self._get_schema_file_name()
            try:
                s = SmvTextOnHdfsIoStrategy(self.smvApp, s_path).read()
                return str_to_schema(s)
            except:
                return None

    def _get_input_data(self):
        """readin xml data"""
        file_path = os.path.join(self.get_connection().path, self.fileName())
        return SmvXmlOnHdfsIoStrategy(
            self.smvApp,
            file_path,
            self.rowTag(),
            self._schema()
        ).read()

class WithCsvParser(SmvInput):
    """Mixin for input modules to parse csv data"""

    def failAtParsingError(self):
        """When set, any parsing error will throw an exception to make sure we can stop early.
            To tolerant some parsing error, user can

            - Override failAtParsingError to False
            - Set dqm to SmvDQM().add(FailParserCountPolicy(10))
                for tolerant <=10 parsing errors
        """
        return True

    def dqm(self):
        """DQM policy

            Override this method to define your own DQM policy (optional).
            Default is an empty policy.

            Returns:
                (SmvDQM): a DQM policy
        """
        return SmvDQM()

    @lazy_property
    def _dqmValidator(self):
        return self.smvApp._jvm.DQMValidator(self.dqm())

    def _readerLogger(self):
        if (self.failAtParsingError()):
            return self.smvApp._jvm.SmvPythonHelper.getTerminateParserLogger()
        else:
            return self._dqmValidator.createParserValidator()

class WithSmvSchema(InputFileWithSchema):
    def csvAttr(self):
        """Specifies the csv file format.  Corresponds to the CsvAttributes case class in Scala.
            Derive from smvSchema if not specified by user.

            Override this method if user want to specify CsvAttributes which is different from
            the one can be derived from smvSchema
        """
        return None

    def smvSchema(self):
        """Return the schema specified by user either through
            userSchema method, or through a schema file. The priority is the following:

                - userSchema
                - schema_file_name under schema_connection

        """
        if (self.userSchema() is not None):
            schema = self.smvApp.smvSchemaObj.fromString(self.userSchema())
        else:
            schema_file_name = self._get_schema_file_name()
            conn = self._get_schema_connection()
            abs_file_path = os.path.join(conn.path, schema_file_name)

            schema = SmvSchemaOnHdfsIoStrategy(self.smvApp, abs_file_path).read()

        if (self.csvAttr() is not None):
            return schema.addCsvAttributes(self.csvAttr())
        else:
            return schema


class SmvCsvInputFile(SparkDfGenMod, WithSmvSchema, WithCsvParser):
    """Csv file input
        User need to implement:

            - connectionName: required
            - fileName: required
            - schemaConnectionName: optional
            - schemaFileName: optional
            - userSchema: optional
            - csvAttr: optional
            - failAtParsingError: optional, default True
            - dqm: optional, default SmvDQM()
    """

    def _get_input_data(self):
        self._assert_file_postfix(".csv")

        file_path = os.path.join(self.get_connection().path, self.fileName())

        return SmvCsvOnHdfsIoStrategy(
            self.smvApp,
            file_path,
            self.smvSchema(),
            self._readerLogger()
        ).read()


class SmvMultiCsvInputFiles(SparkDfGenMod, WithSmvSchema, WithCsvParser):
    """Multiple Csv files under the same dir input
        User need to implement:

            - connectionName: required
            - dirName: required
            - schemaConnectionName: optional
            - schemaFileName: optional
            - userSchema: optional
            - csvAttr: optional
            - failAtParsingError: optional, default True
            - dqm: optional, default SmvDQM()
    """

    @abc.abstractmethod
    def dirName(self):
        """Path to the directory containing the csv files
            relative to the path defined in the connection

            Returns:
                (str)
        """

    # Override schema_file_name logic to depend on dir name instead of file name
    def _get_schema_file_name(self):
        """The schema_file_name is determined by the following logic

                - schemaFileName
                - dirName with post-fix schema
        """
        if (self.schemaFileName() is not None):
            return self.schemaFileName()
        else:
            return self.dirName() + ".schema"

    def fileName(self):
        return self.dirName()

    def _get_input_data(self):
        dir_path = os.path.join(self.get_connection().path, self.dirName())
        smv_schema = self.smvSchema()

        flist = self.smvApp._jvm.SmvHDFS.dirList(dir_path).array()
        # ignore all hidden files in the data dir
        filesInDir = [os.path.join(dir_path, n) for n in flist if not n.startswith(".")]

        if (not filesInDir):
            raise SmvRuntimeError("There are no data files in {}".format(dir_path))

        combinedDf = None
        reader_logger = self._readerLogger()
        for filePath in filesInDir:
            df = SmvCsvOnHdfsIoStrategy(
                self.smvApp,
                filePath,
                smv_schema,
                reader_logger
            ).read()
            combinedDf = df if (combinedDf is None) else combinedDf.unionAll(df)

        return combinedDf


class SmvCsvStringInputData(SparkDfGenMod, WithCsvParser):
    """Input data defined by a schema string and data string

        User need to implement:

            - schemaStr(): required
            - dataStr(): required
            - failAtParsingError(): optional
            - dqm(): optional
    """

    def smvSchema(self):
        return self.smvApp.smvSchemaObj.fromString(self.schemaStr())

    def _get_input_data(self):
        return self.smvApp.createDFWithLogger(self.schemaStr(), self.dataStr(), self._readerLogger())

    @abc.abstractmethod
    def schemaStr(self):
        """Smv Schema string.

            E.g. "id:String; dt:Timestamp"

            Returns:
                (str): schema
        """

    @abc.abstractmethod
    def dataStr(self):
        """Smv data string.

            E.g. "212,2016-10-03;119,2015-01-07"

            Returns:
                (str): data
        """

    def connectionType(self):
        return None

    def connectionName(self):
        return None


__all__ = [
    'SmvJdbcInputTable',
    'SmvHiveInputTable',
    'SmvXmlInputFile',
    'SmvCsvInputFile',
    'SmvMultiCsvInputFiles',
    'SmvCsvStringInputData',
]
