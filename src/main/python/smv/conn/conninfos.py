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

from smv.conn.smvconnectioninfo import SmvConnectionInfo
import re

class SmvJdbcConnectionInfo(SmvConnectionInfo):
    """Connection Info for connection type "jdbc"

        Attributes:

            - url
            - driver
            - user
            - password
    """
    @staticmethod
    def provider_type():
        return "jdbc"

    @staticmethod
    def attributes():
        return ["url", "driver", "user", "password"]


class SmvHiveConnectionInfo(SmvConnectionInfo):
    """Connection Info for connection type "hive"

        Attributes:

            - schema
    """
    @staticmethod
    def provider_type():
        return "hive"

    @staticmethod
    def attributes():
        return ['schema']

class SmvHdfsConnectionInfo(SmvConnectionInfo):
    """Connection Info for connection type "hdfs"

        Attributes:

            - path

        Path attribute takes standard Hadoop HDFS api url. For example

            - file:///tmp/my_data
            - hdfs:///app/data
            - /tmp/data
            - data
    """
    @staticmethod
    def provider_type():
        return "hdfs"

    @staticmethod
    def attributes():
        return ['path']

    def get_content_list(self, smvApp, pattern=None, ignore_case=True):
        """Return a list of file/table names which match the pattern
        """
        all_files = [str(f) for f in smvApp._jvm.SmvPythonHelper.getDirList(self.path)]
        if (pattern is None):
            return all_files
        else:
            if (ignore_case):
                to_match = re.compile(pattern, re.IGNORECASE)
            else:
                to_match = re.compile(pattern)
            return [str(f) for f in all_files if to_match.match(f)]

SmvHdfsEmptyConn = SmvHdfsConnectionInfo(
    "emptydir",
    {"smv.conn.emptydir.path": ""}
)
