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
"""SMV Schema Meta Operations

    Provides helper functions for SmvDesc and SmvLabel operations
"""
from smv.error import SmvRuntimeError

smv_label = "smvLabel"
smv_desc = "smvDesc"

class SchemaMetaOps(object):
    def __init__(self, df):
        self.df = df

    def getDesc(self, colName):
        try:
            meta = (col.metadata for col in self.df.schema.fields if col.name == colName).next()
        except:
            raise SmvRuntimeError("column name {} not found".format(colName))

        return meta.get(smv_desc, u'')

    def addDesc(self, *colDescs):
        if not colDescs: raise SmvRuntimeError("must provide description argument")
        colDict = dict(colDescs)
        for col in self.df.schema.fields:
            if colDict.has_key(col.name):
                col.metadata[smv_desc] = colDict[col.name]
        return self.df