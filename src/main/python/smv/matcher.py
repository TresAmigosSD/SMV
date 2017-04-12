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

from smv import SmvApp
from smv.utils import smv_copy_array

from pyspark.sql import HiveContext, DataFrame

def ExactMatchPreFilter(colName, expr):
    return SmvApp.getInstance()._jvm.org.tresamigos.smv.matcher.ExactMatchPreFilter(colName, expr._jc)

def NoOpPreFilter():
    return None

def GroupCondition(expr):
    return SmvApp.getInstance()._jvm.org.tresamigos.smv.matcher.GroupCondition(expr._jc)

def NoOpGroupCondition():
    return None

def ExactLogic(colName, expr):
    return SmvApp.getInstance()._jvm.org.tresamigos.smv.matcher.ExactLogic(colName, expr._jc)

def FuzzyLogic(colName, predicate, valueExpr, threshold):
    pjc = predicate._jc
    return SmvApp.getInstance()._jvm.org.tresamigos.smv.matcher.FuzzyLogic(colName, pjc, valueExpr._jc, threshold)

class SmvEntityMatcher(object):
    def __init__(self,
        leftId,
        rightId,
        exactMatchFilter,
        groupCondition,
        levelLogics
    ):
        jlls = SmvApp.getInstance().sc._gateway.new_array(SmvApp.getInstance()._jvm.org.tresamigos.smv.matcher.LevelLogic, len(levelLogics))
        for i in range(0, len(jlls)):
            jlls[i] = levelLogics[i]

        self.jem = SmvApp.getInstance()._jvm.org.tresamigos.smv.python.SmvPythonHelper.createMatcher(
            leftId,
            rightId,
            exactMatchFilter,
            groupCondition,
            jlls
        )

    def doMatch(self, df1, df2, keepOriginalCols=True):
        jres = self.jem.doMatch(df1._jdf, df2._jdf, keepOriginalCols)
        return DataFrame(jres, SmvApp.getInstance().sqlContext)
