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

from smv import smvPy, smv_copy_array
from pyspark.sql import HiveContext, DataFrame

def ExactMatchPreFilter(colName, expr):
    return smvPy._jvm.org.tresamigos.smv.matcher2.ExactMatchPreFilter(colName, expr._jc)

def GroupCondition(expr):
    return smvPy._jvm.org.tresamigos.smv.matcher2.GroupCondition(expr._jc)

def ExactLogic(colName, expr):
    return smvPy._jvm.org.tresamigos.smv.matcher2.ExactLogic(colName, expr._jc)

def FuzzyLogic(colName, predicate, valueExpr, threshold):
    pjc = None if predicate is None else predicate._jc
    return smvPy._jvm.org.tresamigos.smv.matcher2.FuzzyLogic(colName, pjc, valueExpr._jc, threshold)

class SmvEntityMatcher(object):
    def __init__(self,
        leftId,
        rightId,
        exactMatchFilter,
        groupCondition,
        levelLogics
    ):
        jlls = smvPy.sc._gateway.new_array(smvPy._jvm.org.tresamigos.smv.matcher2.LevelLogic, len(levelLogics))
        for i in range(0, len(jlls)):
            jlls[i] = levelLogics[i]

        self.jem = smvPy._jvm.org.tresamigos.smv.python.SmvPythonHelper.createMatcher(
            leftId,
            rightId,
            exactMatchFilter,
            groupCondition,
            jlls
        )

    def doMatch(self, df1, df2, keepOriginalCols=True):
        jres = self.jem.doMatch(df1._jdf, df2._jdf, keepOriginalCols)
        return DataFrame(jres, smvPy.sqlContext)
