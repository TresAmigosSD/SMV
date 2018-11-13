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

from smv.io.base import SmvOutput, AsTable


class SmvJdbcOutputTable(SmvOutput, AsTable):
    """
        User need to implement 

            - requiresDS
            - connectionName
            - tableName
            - writeMode: optional, default "errorifexists"
    """

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


    def doRun(self, known):
        self.assert_single_input()
        i = self.RunParams(known)

        conn = self.get_connection()
        data = i[self.requiresDS()[0]]

        builder = data.write\
            .format("jdbc") \
            .mode(self.writeMode()) \
            .option('url', conn.url)

        if (conn.driver is not None):
            builder = builder.option('driver', conn.driver)
        if (conn.user is not None):
            builder = builder.option('user', conn.user)
        if (conn.password is not None):
            builder = builder.option('password', conn.password)

        builder \
            .option("dbtable", self.tableName()) \
            .save()

        # return data back for meta calculation
        # TODO: need to review whether we should even calculate meta for output
        return data


__all__ = [
    'SmvJdbcOutputTable',
]