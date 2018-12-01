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

This module defines the abstract classes which formed the SmvGenericModule's
input DS Framework for clients' projects
"""

import abc

from smv.iomod import SmvCsvInputFile, SmvMultiCsvInputFiles, SmvHiveInputTable, SmvCsvStringInputData
from smv.conn import SmvHiveConnectionInfo


class SmvCsvFile(SmvCsvInputFile):
    """Input from a file in CSV format
        Base class for CSV file input.
        User need to define path method.

        Example:
        >>> class MyCsvFile(SmvCsvFile):
        >>>     def path(self):
        >>>         return "path/relative/to/smvInputDir/file.csv"
    """

    @abc.abstractmethod
    def path(self):
        """relative path to csv file"""

    def fileName(self):
        return self.path()

    def run(self, df):
        return df

    def doRun(self, known):
        df = super(SmvCsvFile, self).doRun(known)
        return self.run(df)

    def connectionName(self):
        return None

    def get_connection(self):
        return self.smvApp.input_connection()


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


class SmvMultiCsvFiles(SmvMultiCsvInputFiles):
    """Raw input from multiple csv files sharing single schema

        Instead of a single input file, specify a data dir with files which share
        the same schema.
    """
    def dirName(self):
        return self.dir()

    def run(self, df):
        return df

    def doRun(self, known):
        df = super(SmvMultiCsvFiles, self).doRun(known)
        return self.run(df)

    def connectionName(self):
        return None

    def get_connection(self):
        return self.smvApp.input_connection()

    @abc.abstractmethod
    def dir(self):
        """Path to the directory containing the csv files and their schema

            Returns:
                (str): path
        """


class SmvHiveTable(SmvHiveInputTable):
    """Input from a Hive table
        This is for backward compatability. Will be deprecated. Please use
        iomod.SmvHiveInputTable instead.

        User need to implement:

            - tableName

        Custom query at reading is no more supported, please use downstream
        module to process data
    """

    @abc.abstractmethod
    def tableName(self):
        """User-specified name Hive hive table to extract input from

            Override this to specify your own table name.

            Returns:
                (str): table name
        """
        pass

    def connectionName(self):
        # Since old hive interface does not support separate schema
        # specification, no need for a connection name
        return None

    def get_connection(self):
        # Create and empty connection info so SmvHiveInputTable will
        # default to refer to the tableName without a schema name
        return SmvHiveConnectionInfo("hiveschema", {})


class SmvCsvStringData(SmvCsvStringInputData):
    pass

__all__ = [
    'SmvMultiCsvFiles',
    'SmvCsvFile',
    'SmvSqlCsvFile',
    'SmvCsvStringData',
    'SmvHiveTable'
]
