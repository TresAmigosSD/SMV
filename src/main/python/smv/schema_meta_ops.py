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
import json
from pyspark.sql import DataFrame
from smv.error import SmvRuntimeError

smv_label = "smvLabel"
smv_desc = "smvDesc"

def getMetaDesc(m):
    return m.get(smv_desc, u'')

def getMetaLabels(m):
    return m.get(smv_label, [])

def setMetaDesc(m, desc):
    m[smv_desc] = desc
    return m

def removeMetaDesc(m):
    m.pop(smv_desc, None)
    return m

def setMetaLabel(m, labels):
    m[smv_label] = list(set(getMetaLabels(m)) | set(labels))
    return m

def removeMetaLabel(m, labels, removeAll):
    m[smv_label] = [] if removeAll else list(set(getMetaLabels(m)) - set(labels))
    return m

class SchemaMetaOps(object):
    def __init__(self, df):
        self.df = df
        self.jdf = df._jdf
        self._sql_ctx = df.sql_ctx
        self._jPythonHelper = df._sc._jvm.SmvPythonHelper

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

        jdf = self._jPythonHelper.smvColMeta(self.jdf,\
            [(col.name, json.dumps(setMetaDesc(col.metadata, addDict[col.name])))\
            for col in self.df.schema.fields if col.name in addDict])

        return DataFrame(jdf, self._sql_ctx)

    def removeDesc(self, *colNames):
        removeAll = not bool(colNames)
        if not removeAll:
            removeSet = set(colNames)

        jdf = self._jPythonHelper.smvColMeta(self.jdf,\
            [(col.name, json.dumps(removeMetaDesc(col.metadata)))\
            for col in self.df.schema.fields if removeAll or col.name in removeSet])

        return DataFrame(jdf, self._sql_ctx)
        
    def getLabel(self, colName):
        if colName is None:
            return [(col.name, getMetaLabels(col.metadata)) for col in self.df.schema.fields]
        return getMetaLabels(self.getMetaByName(colName))

    def addLabel(self, colNames, labels):
        if not labels:
            raise SmvRuntimeError("must provide a list of labels to add")
        
        addToAll = not bool(colNames)
        if not addToAll:
            addSet = set(colNames)

        jdf = self._jPythonHelper.smvColMeta(self.jdf,\
            [(col.name, json.dumps(setMetaLabel(col.metadata, labels)))\
            for col in self.df.schema.fields if addToAll or col.name in addSet])

        return DataFrame(jdf, self._sql_ctx)

    def removeLabel(self, colNames = None, labels = None):
        allLabel = not bool(labels)
        allCol = not bool(colNames)
        if not allCol:
            removeSet = set(colNames)

        jdf = self._jPythonHelper.smvColMeta(self.jdf,\
            [(col.name, json.dumps(removeMetaLabel(col.metadata, labels, allLabel)))\
            for col in self.df.schema.fields if allCol or col.name in removeSet])

        return DataFrame(jdf, self._sql_ctx)

    def colsWithLabel(self, labels = None):
        def match(meta):
            return set(labels) <= set(getMetaLabels(meta)) if bool(labels) else not getMetaLabels(meta)

        ret = [col.name for col in self.df.schema.fields if match(col.metadata)]

        if not ret:
            if bool(labels):
                raise SmvRuntimeError("there are no columns labeled with {{{}}} in {}"\
                    .format(", ".join(labels), self.df))
            else:
                raise SmvRuntimeError("there are no unlabeled columns in {}"\
                    .format(self.df))

        return ret