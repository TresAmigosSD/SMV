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

def _getMetaDesc(meta):
    return meta.get(smv_desc, u'')

def _getMetaLabels(meta):
    return meta.get(smv_label, [])

def _setMetaDesc(meta, desc):
    meta[smv_desc] = desc
    return meta

def _removeMetaDesc(meta):
    meta.pop(smv_desc, None)
    return meta

def _setMetaLabel(meta, labels):
    meta[smv_label] = list(set(_getMetaLabels(meta)) | set(labels))
    return meta

def _removeMetaLabel(meta, labels):
    meta[smv_label] = [] if not labels else list(set(_getMetaLabels(meta)) - set(labels))
    return meta

class SchemaMetaOps(object):
    def __init__(self, df):
        self.df = df
        self.jdf = df._jdf
        self.fields = df.schema.fields
        self._sql_ctx = df.sql_ctx
        self._jPythonHelper = df._sc._jvm.SmvPythonHelper

    def _getMetaByName(self, colName):
        """Returns the metadata of the first column that matches the column name

            Will throw if there's no column matching the specified name

            Args:
                colName (string) the name of the column that is being looked for
            
            Returns:
                (dict) the metadata of the given column
        """
        try:
            meta = next(col.metadata for col in self.fields if col.name == colName)
        except StopIteration:
            raise SmvRuntimeError("column name {} not found".format(colName))
        return meta

    def _updateColMeta(self, colShouldUpdate, colUpdateMeta):
        """Returns a DataFrame with the specified meta data updated in specified columns

            Args:
                colShouldUpdate (function):
                    input - (dict) a DataFrame column
                    output - (bool) whether we should update the given column

                colUpdateMeta (function):
                    input - (dict) a DataFrame column
                    output - (dict) the metadata that should be assigned to the given column

            Returns:
                (DataFrame) a new DataFrame containing the updated metadata
        """
        cols = [col for col in self.fields if colShouldUpdate(col)]
        colMeta = [(col.name, json.dumps(colUpdateMeta(col))) for col in cols]
        jdf = self._jPythonHelper.smvColMeta(self.jdf, colMeta)
        return DataFrame(jdf, self._sql_ctx)

    def _checkColExistence(self, colNames):
        """Check if the given column names exist in the DataFrame

            Will throw if some of the column names are not found

            Args:
                colNames (list(string)) a list of column names to check
        """
        invalidCols = set(colNames) - set(self.df.columns)
        if invalidCols:
            raise SmvRuntimeError("{} does not have columns {}".format(self.df, ", ".join(invalidCols)))

    def getDesc(self, colName):
        """Returns column description(s)

            If colName is empty, returns descriptions for all columns
        """
        if colName is None:
            return [(col.name, _getMetaDesc(col.metadata)) for col in self.fields]
        return _getMetaDesc(self._getMetaByName(colName))

    def getLabel(self, colName):
        """Returns a list of column label(s)

            If colName is empty, returns labels for all columns
        """
        if colName is None:
            return [(col.name, _getMetaLabels(col.metadata)) for col in self.fields]
        return _getMetaLabels(self._getMetaByName(colName))

    def addDesc(self, *colDescs):
        """Adds column descriptions
        """
        if not colDescs:
            raise SmvRuntimeError("must provide (name, description) pair to add")

        self._checkColExistence([tup[0] for tup in colDescs])

        addDict = dict(colDescs)

        def colShouldUpdate(col):
            return col.name in addDict

        def colUpdateMeta(col):
            return _setMetaDesc(col.metadata, addDict[col.name])
        
        return self._updateColMeta(colShouldUpdate, colUpdateMeta)

    def removeDesc(self, *colNames):
        """Removes description for the given columns from the Dataframe

            If colNames are empty, removes descriptions of all columns
        """
        if colNames:
            self._checkColExistence(colNames)
            removeSet = set(colNames)

        def colShouldUpdate(col):
            return not colNames or col.name in removeSet

        def colUpdateMeta(col):
            return _removeMetaDesc(col.metadata)

        return self._updateColMeta(colShouldUpdate, colUpdateMeta)

    def addLabel(self, colNames, labels):
        """Adds labels to the specified columns

            If colNames are empty, adds the same set of labels to all columns
        """
        if not labels:
            raise SmvRuntimeError("must provide a list of labels to add")

        if colNames:
            self._checkColExistence(colNames)
            addSet = set(colNames)

        def colShouldUpdate(col):
            return not colNames or col.name in addSet

        def colUpdateMeta(col):
            return _setMetaLabel(col.metadata, labels)

        return self._updateColMeta(colShouldUpdate, colUpdateMeta)

    def removeLabel(self, colNames = None, labels = None):
        """Removes labels from the specified columns

            If colNames are empty, removes the same set of labels from all columns
            If labels are empty, removes all labels from the given columns
            If they are both empty, removes all labels from all columns
        """
        if colNames:
            self._checkColExistence(colNames)
            removeSet = set(colNames)

        def colShouldUpdate(col):
            return not colNames or col.name in removeSet

        def colUpdateMeta(col):
            return _removeMetaLabel(col.metadata, labels)

        return self._updateColMeta(colShouldUpdate, colUpdateMeta)

    def colsWithLabel(self, labels = None):
        """Returns all column names in the data frame that contain all the specified labels

            If labels are empty, returns names of unlabeled columns
        """
        def metaLabelMatched(meta):
            if labels:
                # if labels are provided, match the column whose labels contain the given ones
                return set(labels) <= set(_getMetaLabels(meta))
            else:
                # if labels are empty, match the column with no label
                return not _getMetaLabels(meta)

        ret = [col.name for col in self.fields if metaLabelMatched(col.metadata)]

        if not ret:
            if labels:
                raise SmvRuntimeError("there are no columns labeled with {{{}}} in {}"\
                    .format(", ".join(labels), self.df))
            else:
                raise SmvRuntimeError("there are no unlabeled columns in {}"\
                    .format(self.df))

        return ret