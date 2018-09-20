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

def getMetaDesc(m):
    return m.get(smv_desc, u'')

class SchemaMetaOps(object):
    def __init__(self, df):
        self.df = df

    def getDesc(self, colName):
        if colName is None:
            return [(col.name, getMetaDesc(col.metadata)) for col in self.df.schema.fields]
        try:
            meta = (col.metadata for col in self.df.schema.fields if col.name == colName).next()
        except:
            raise SmvRuntimeError("column name {} not found".format(colName))

        return getMetaDesc(meta)

    def addDesc(self, *colDescs):
        if not colDescs: raise SmvRuntimeError("must provide (name, description) pair to add")

        # convert list [(name, desc), ...] to a dictionary {name: desc, ...}
        addDict = dict(colDescs)

        for col in self.df.schema.fields:
            if addDict.has_key(col.name):
                col.metadata[smv_desc] = addDict[col.name]

        return self.df

    def smvRemoveDesc(self, *colNames):
        removeAll = not bool(colNames)

        # convert list [name, ...] to a dictionary {name: True, ...}
        if not removeAll: removeDict = {name: True for name in colNames}

        for col in self.df.schema.fields:
            if removeAll or removeDict.has_key(col.name):
                col.metadata.pop(smv_desc, None)

        return self.df
        