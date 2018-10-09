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
"""SMV DataSet Framework's SmvInputBase interface

This module defines the abstract classes which formed the SmvDataSet's
input DS Framework for clients' projects
"""

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

import abc
import sys
import json
import re

from smv.dqm import FailParserCountPolicy
from smv.error import SmvRuntimeError
from smv.smvdataset import SmvDataSet

if sys.version_info >= (3, 4):
    ABC = abc.ABC
else:
    ABC = abc.ABCMeta('ABC', (), {})


def _smvhash(text):
    """Python's hash function will return different numbers from run to
    from, starting from 3.  Provide a deterministic hash function for
    use to calculate sourceCodeHash.
    """
    import binascii
    return binascii.crc32(text.encode())


class SmvInputBase(SmvDataSet, ABC):
    """SmvDataSet representing external input
        Concrete class need to provide:
          - readAsDF
          - dataSrcHash (optional)
          - schemaHash (optional)
    """

    def isEphemeral(self):
        return True

    def dsType(self):
        return "Input"

    def requiresDS(self):
        return []

    def run(self, df):
        """Post-processing for input data

            Args:
                df (DataFrame): input data

            Returns:
                (DataFrame): processed data
        """
        return df

    @abc.abstractmethod
    def readAsDF(self, readerLogger):
        """User defined data reader

            Returns:
                (DataFrame): data from raw input
        """
        pass

    def dataSrcHash(self):
        """Hash computed on data source

            Returns:
                (int): user defined hash derived from raw data (default: 0)
        """
        return 0

    def schemaHash(self):
        """Hash computed from schema

            Returns:
                (int): user defined hash derived from input schema (default: 0)
        """
        return 0

    def instanceValHash(self):
        """Hash computed based on instance values of the dataset
            Default to dataSrcHash + schemaHash
        """
        res = self.dataSrcHash() + self.schemaHash()
        # ensure python's numeric type can fit in a java.lang.Integer
        return int(res) & 0x7fffffff

    def doRun(self, validator, known):
        if (validator is None):
            readerLogger = self.smvApp._jvm.SmvPythonHelper.getTerminateParserLogger()
        else:
            readerLogger = validator.createParserValidator()
        result = self.run(self.readAsDF(readerLogger))
        self.assert_result_is_dataframe(result)
        return result._jdf


class SmvInputFromFile(SmvInputBase):
    """Base class for any input based on files on HDFS or local

        Concrete class need to provide:

            * path (str): path relative to smv.inputDir, or
            * fullPath (str): file full path with protocol
            * fullSchemaPath (str): schema file's full path, or
            * schema (StructType): schema object

    """
    def path(self):
        """Relative path to smv.dataDir config parameter"""
        pass

    def fullPath(self):
        """Full path to the input (file/dir or glob pattern)"""
        return "{}/{}".format(self.smvApp.inputDir(), self.path())

    def fullSchemaPath(self):
        """Full path to the schema file"""
        know_types = ['.gz', '.csv', '.tsv', '.xml']
        base = self.fullPath()
        for t in know_types:
            base = re.sub(t + '$', '', base)
        return base + '.schema'

    def schema(self):
        """User specified schema
            Returns:
                (StructType): input data's schema
        """
        return None

    def dataSrcHash(self):
        """Hash computed on data source
            Based on file's mtime, and file's full path
        """
        mTime = self.smvApp._jvm.SmvHDFS.modificationTime(self.fullPath())
        pathHash = _smvhash(self.fullPath())
        return mTime + pathHash

    def schemaHash(self):
        """Hash computed from schema
            Based on input schema as a StructType
        """
        if (self.schema() is not None):
            return _smvhash(self.schema().simpleString())
        else:
            return 0


class SmvXmlFile(SmvInputFromFile):
    """Input from file in XML format
        Concrete class need to provide:

            * rowTag (str): XML tag for identifying a row
            * path (str): File path relative to smv.InputDir. Or
            * fullPath (str): file full path with protocol
            * fullSchemaPath (str): full path of the schema JSON file or None (infer schema)
    """

    @abc.abstractmethod
    def rowTag(self):
        """XML tag for identifying a record (row)"""
        pass

    def schema(self):
        """load schema from the json file"""
        if (self.fullSchemaPath() is None):
            return None
        else:
            with open(self.fullSchemaPath(), "r") as sj:
                schema_st = sj.read()
            return StructType.fromJson(json.loads(schema_st))

    def readAsDF(self, readerLogger):
        """readin xml data"""

        # TODO: look for possibilities to feed to readerLogger
        reader = self.smvApp.sqlContext\
            .read.format('com.databricks.spark.xml')\
            .options(rowTag=self.rowTag())

        # If no schema specified, infer from data
        if (self.schema() is not None):
            return reader.load(self.fullPath(), schema=self.schema())
        else:
            return reader.load(self.fullPath())


class WithParser(SmvInputBase):
    """Input uses SmvSchema and Csv parser"""

    def dqmWithTypeSpecificPolicy(self):
        """for parsers we should get the type specific dqm policy from the
           concrete scala proxy class that is the actual input (e.g. SmvCsvFile)"""
        userDqm = self.dqm()

        if self.failAtParsingError():
            res = userDqm.add(FailParserCountPolicy(1)).addAction()
        elif self.forceParserCheck():
            res = userDqm.addAction()
        else:
            res = userDqm
        return res

    def forceParserCheck(self):
        return True

    def failAtParsingError(self):
        return True

    @abc.abstractmethod
    def smvSchema(self):
        """Returns SmvSchema, as the Scala SmvSchema class
        """
        pass

    def schema(self):
        j_schema = self.smvSchema().toStructType()
        return StructType.fromJson(json.loads(j_schema.json()))

    def csvAttr(self):
        """Specifies the csv file format.  Corresponds to the CsvAttributes case class in Scala.
            Derive from smvSchema if not specified by user.

            Override this method if user want to specify CsvAttributes which is different from
            the one can be derived from smvSchema
        """
        return self.smvSchema().extractCsvAttributes()

    def userSchema(self):
        """Get user-defined schema

            Override this method to define your own schema for the target file.
            Schema declared in this way take priority over .schema files. Schema
            should be specified in the format "colName1:colType1;colName2:colType2"

            Returns:
                (string):
        """
        return None

    def schemaHash(self):
        # when smvSchema is provided, use it to calculate hash, since it also
        # includes CsvAttributes info
        return self.smvSchema().schemaHash()


class SmvCsvFile(WithParser, SmvInputFromFile):
    """Input from a file in CSV format
        Base class for CSV file input.
        User need to define path method.

        Example:
        >>> class MyCsvFile(SmvCsvFile):
        >>>     def path(self):
        >>>         return "path/relative/to/smvInputDir/file.csv"
    """

    def smvSchema(self):
        smvSchemaObj = self.smvApp.j_smvPyClient.getSmvSchema()
        if (self.userSchema() is not None):
            return smvSchemaObj.fromString(self.userSchema())
        else:
            return smvSchemaObj.fromFile(self.smvApp.j_smvApp.sc(), self.fullSchemaPath())


    def readAsDF(self, readerLogger):
        jdf = self.smvApp.j_smvPyClient.readCsvFromFile(
            self.fullPath(),
            self.smvSchema(),
            self.csvAttr(),
            readerLogger
        )
        return DataFrame(jdf, self.smvApp.sqlContext)


class SmvSqlCsvFile(SmvCsvFile):
    """Input from a file in CSV format and using a SQL query to access it
    """

    # temporary table name
    tableName = "df"

    def query(self):
        """Query used to extract data from the table which reads the CSV file

            Override this to specify your own query (optional). Default is
            equivalent to 'select * from ' + tableName.

            Returns:
                (str): query
        """
        return "select * from " + self.tableName

    def run(self, df):
        # temporarily register DataFrame of input CSV file as a table
        df.registerTempTable(self.tableName)

        # execute the table query
        res = self.smvApp.sqlContext.sql(self.query())

        # drop the temporary table
        self.smvApp.sqlContext.sql("drop table " + self.tableName)

        return res


class SmvMultiCsvFiles(SmvCsvFile):
    """Raw input from multiple csv files sharing single schema

        Instead of a single input file, specify a data dir with files which share
        the same schema.
    """
    def path(self):
        return self.dir()

    def readAsDF(self, readerLogger):
        flist = self.smvApp._jvm.SmvHDFS.dirList(self.fullPath()).array()
        # ignore all hidden files in the data dir
        filesInDir = ["{}/{}".format(self.fullPath(), n) for n in flist if not n.startswith(".")]

        if (not filesInDir):
            raise SmvRuntimeError("There are no data files in {}".format(self.fullPath()))

        combinedJdf = None
        for filePath in filesInDir:
            jdf = self.smvApp.j_smvPyClient.readCsvFromFile(
                filePath,
                self.smvSchema(),
                self.csvAttr(),
                readerLogger
            )
            combinedJdf = jdf if (combinedJdf is None) else combinedJdf.unionAll(jdf)

        return DataFrame(combinedJdf, self.smvApp.sqlContext)

    def description(self):
        return "Input dir: @" + self.dir()

    @abc.abstractmethod
    def dir(self):
        """Path to the directory containing the csv files and their schema

            Returns:
                (str): path
        """


class SmvCsvStringData(WithParser):
    """Input data defined by a schema string and data string
    """

    def smvSchema(self):
        smvSchemaObj = self.smvApp.j_smvPyClient.getSmvSchema()
        return smvSchemaObj.fromString(self.schemaStr())

    def readAsDF(self, readerLogger):
        return self.smvApp.createDFWithLogger(self.schemaStr(), self.dataStr(), readerLogger)

    def dataSrcHash(self):
        return _smvhash(self.dataStr())

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


class SmvJdbcTable(SmvInputBase):
    """Input from a table read through JDBC
    """
    def description(self):
        return "JDBC table {}".format(self.tableName())

    def jdbcUrl(self):
        """User can override this, default use the jdbcUrl setting in smvConfig"""
        return self.smvApp.jdbcUrl()

    def readAsDF(self, readerLogger):
        if (self.tableQuery() is None):
            tableNameOrQuery = self.tableName()
        else:
            tableNameOrQuery = "({}) as TMP_{}".format(
                self.tableQuery(), self.tableName()
            )

        return self.smvApp.sqlContext.read\
            .format('jdbc')\
            .option('url', self.jdbcUrl())\
            .option('dbtable', tableNameOrQuery)\
            .load()

    @abc.abstractmethod
    def tableName(self):
        """User-specified name for the table to extract input from

            Override this to specify your own table name.

            Returns:
                (str): table name
        """
        pass

    def tableQuery(self):
        """Query used to extract data from Hive table

            Override this to specify your own query (optional). Default is
            equivalent to 'select * from ' + tableName().

            Returns:
                (str): query
        """
        return None


class SmvHiveTable(SmvInputBase):
    """Input from a Hive table
    """

    def readAsDF(self, readerLogger):
        if (self.tableQuery() is None):
            query = "select * from {}".format(self.tableName())
        else:
            query = self.tableQuery()
        return self.smvApp.sqlContext.sql(query)

    @abc.abstractmethod
    def tableName(self):
        """User-specified name Hive hive table to extract input from

            Override this to specify your own table name.

            Returns:
                (str): table name
        """
        pass

    def tableQuery(self):
        """Query used to extract data from Hive table

            Override this to specify your own query (optional). Default is
            equivalent to 'select * from ' + tableName().

            Returns:
                (str): query
        """
        return None


__all__ = [
    'SmvInputFromFile',
    'SmvXmlFile',
    'SmvMultiCsvFiles',
    'SmvCsvFile',
    'SmvSqlCsvFile',
    'SmvCsvStringData',
    'SmvJdbcTable',
    'SmvHiveTable'
]
