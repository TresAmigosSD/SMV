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

import os
import re

from smv.iomod.base import SmvSparkDfOutput, AsTable, AsFile
from smv.smviostrategy import SmvCsvOnHdfsIoStrategy, SmvJdbcIoStrategy, SmvHiveIoStrategy, SmvSchemaOnHdfsIoStrategy
from smv.csv_attributes import CsvAttributes
from smv.utils import scala_seq_to_list

class WithSparkDfWriter(object):
    """Mixin for output modules using spark df writer"""
    def writeMode(self):
        """
            Write mode for Spark DataFrameWriter.
            Valid values:

                - "append"
                - "overwrite"
                - "ignore"
                - "error" or "errorifexists" (default)
        """
        return "errorifexists"


class SmvJdbcOutputTable(SmvSparkDfOutput, WithSparkDfWriter, AsTable):
    """
        User need to implement

            - requiresDS
            - connectionName
            - tableName
            - writeMode: optional, default "errorifexists"
    """

    def connectionType(self):
        return 'jdbc'

    def doRun(self, known):
        data = self.get_spark_df(known)
        conn = self.get_connection()

        SmvJdbcIoStrategy(self.smvApp, conn, self.tableName(), self.writeMode())\
            .write(data)

        # return data back for meta calculation
        # TODO: need to review whether we should even calculate meta for output
        return data


class SmvHiveOutputTable(SmvSparkDfOutput, WithSparkDfWriter, AsTable):
    """
        User need to implement

            - requiresDS
            - connectionName
            - tableName
            - writeMode: optional, default "errorifexists"
    """

    def connectionType(self):
        return 'hive'

    def doRun(self, known):
        data = self.get_spark_df(known)
        conn = self.get_connection()

        SmvHiveIoStrategy(self.smvApp, conn, self.tableName(), self.writeMode())\
            .write(data)

        return data


class SmvCsvOutputFile(SmvSparkDfOutput, AsFile):
    """
        User need to implement

            - requiresDS
            - connectionName
            - fileName
    """
    def writeMode(self):
        """Default write mode is overwrite, and currently only support overwrite
        """
        return "overwrite"

    def doRun(self, known):
        data = self.get_spark_df(known)
        file_path = os.path.join(self.get_connection().path, self.fileName())
        schema_path = re.sub("\.csv$", ".schema", file_path)

        schema = self.smvApp.smvSchemaObj.fromDataFrame(data._jdf, "_SmvStrNull_", self.smvApp.scalaOption(CsvAttributes()))

        SmvCsvOnHdfsIoStrategy(self.smvApp, file_path, schema, None, self.writeMode()).write(data)
        SmvSchemaOnHdfsIoStrategy(self.smvApp, schema_path, self.writeMode()).write(schema)
        return data

__all__ = [
    'SmvJdbcOutputTable',
    'SmvHiveOutputTable',
    'SmvCsvOutputFile',
]
