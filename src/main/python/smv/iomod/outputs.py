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

from smv.iomod.base import SmvSparkDfOutput, AsTable
from smv.smviostrategy import SmvJdbcIoStractegy


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

    def doRun(self, known):
        data = self.get_spark_df(known)
        conn = self.get_connection()

        SmvJdbcIoStractegy(self.smvApp, conn, self.tableName(), self.writeMode())\
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

    def doRun(self, known):
        data = self.get_spark_df(known)

        data.write\
            .mode(self.writeMode())\
            .saveAsTable(self.table_with_schema())

        return data


__all__ = [
    'SmvJdbcOutputTable',
    'SmvHiveOutputTable',
]
