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

from smv import SmvModule, SmvModuleLink
from stage1.output import A
from pyspark.sql.functions import col, lit

L = SmvModuleLink(A)

class B(SmvModule):

    def requiresDS(self): return [L]
    def run(self, i):
        df = i[L]
        return df.smvSelectPlus((col("v") + 1).alias("v2"))
