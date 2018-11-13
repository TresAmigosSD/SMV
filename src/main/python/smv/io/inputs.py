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

from smv.io.base import SmvInput


class SmvJdbcInputTable(SmvInput):
    """
        User need to implement 

            - connectionName
            - tableName
    """

    def doRun(self, known):
        conn = self.get_connection()
        builder = self.smvApp.sqlContext.read\
            .format('jdbc')\
            .option('url', conn.url)

        if (conn.driver is not None):
            builder = builder.option('driver', conn.driver)
        if (conn.user is not None):
            builder = builder.option('user', conn.user)
        if (conn.password is not None):
            builder = builder.option('password', conn.password)

        return builder\
            .option('dbtable', self.tableName())\
            .load()

__all__ = [
    'SmvJdbcInputTable',
]