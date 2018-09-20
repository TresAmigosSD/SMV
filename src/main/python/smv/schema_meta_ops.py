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

def getMetaLabels(m):
    return m.get(smv_label, set())

class SchemaMetaOps(object):
    def __init__(self, df):
        self.df = df

    def getMetaByName(self, colName):
        """Returns the metadata of the first column that matches the column name

            Will throw if there's no column that matches the specified name
        """
        try:
            meta = next(col.metadata for col in self.df.schema.fields if col.name == colName)
        except:
            raise SmvRuntimeError("column name {} not found".format(colName))
        return meta

    def getDesc(self, colName):
        if colName is None:
            return [(col.name, getMetaDesc(col.metadata)) for col in self.df.schema.fields]
        return getMetaDesc(self.getMetaByName(colName))

    def addDesc(self, *colDescs):
        if not colDescs:
            raise SmvRuntimeError("must provide (name, description) pair to add")

        addDict = dict(colDescs)

        for col in self.df.schema.fields:
            if col.name in addDict:
                col.metadata[smv_desc] = addDict[col.name]

        return self.df

    def removeDesc(self, *colNames):
        removeAll = not bool(colNames)
        if not removeAll:
            removeSet = set(colNames)

        for col in self.df.schema.fields:
            if removeAll or col.name in removeSet:
                col.metadata.pop(smv_desc, None)

        return self.df
        
    def getLabel(self, colName):
        if colName is None:
            return [(col.name, list(getMetaLabels(col.metadata))) for col in self.df.schema.fields]
        return list(getMetaLabels(self.getMetaByName(colName)))

    def addLabel(self, colNames, labels):
        if not labels:
            raise SmvRuntimeError("must provide a list of labels to add")
        
        addToAll = not bool(colNames)
        if not addToAll:
            addSet = set(colNames)

        for col in self.df.schema.fields:
            if addToAll or col.name in addSet:
                col.metadata[smv_label] = getMetaLabels(col.metadata) | set(labels)

        return self.df

    def removeLabel(self, colNames = None, labels = None):
        allLabel = not bool(labels)
        allCol = not bool(colNames)
        if not allCol:
            removeSet = set(colNames)

        for col in self.df.schema.fields:
            if allCol or col.name in removeSet:
                col.metadata[smv_label] = set() if allLabel else getMetaLabels(col.metadata) - set(labels)

        return self.df

    def colsWithLabel(self, labels = None):
        def match(meta):
            return set(labels) <= getMetaLabels(meta) if bool(labels) else not getMetaLabels(meta)

        ret = [col.name for col in self.df.schema.fields if match(col.metadata)]

        if not ret:
            if bool(labels):
                raise SmvRuntimeError("there are no columns labeled with {{{}}} in {}"\
                    .format(", ".join(labels), self.df))
            else:
                raise SmvRuntimeError("there are no unlabeled columns in {}"\
                    .format(self.df))

        return ret