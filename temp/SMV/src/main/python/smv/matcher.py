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
    """Specify the top-level exact match

        Args:
            colName (string): level name used in the output DF
            expr (Column): match logic condition Column

        Example:
            >>> ExactMatchPreFilter("Full_Name_Match", col("full_name") == col("_full_name"))

        Returns:
            (ExactMatchPreFilter)
    """
    return SmvApp.getInstance()._jvm.org.tresamigos.smv.matcher.ExactMatchPreFilter(colName, expr._jc)

def NoOpPreFilter():
    """Always returns None

        Returns:
            (None)
    """
    return None

def GroupCondition(expr):
    """Specify the shared matching condition of all the levels (except the top-level exact match)

        Args:
            expr (Column): shared matching condition

        Note:
            `expr` should be in "left == right" form so that it can really optimize the process by reducing searching space

        Example:
            >>> GroupCondition(soundex("first_name") == soundex("_first_name"))

        Returns:
            (GroupCondition)
    """
    return SmvApp.getInstance()._jvm.org.tresamigos.smv.matcher.GroupCondition(expr._jc)

def NoOpGroupCondition():
    """Always returns None

        Returns:
            (None)
    """
    return None

def ExactLogic(colName, expr):
    """Level match with exact logic

        Args:
            colName (string): level name used in the output DF
            expr (Column): match logic colName

        Example:
            >>> ExactLogic("First_Name_Match", col("first_name") == col("_first_name"))

        Returns:
            (ExactLogic)
    """
    return SmvApp.getInstance()._jvm.org.tresamigos.smv.matcher.ExactLogic(colName, expr._jc)

def FuzzyLogic(colName, predicate, valueExpr, threshold):
    """Level match with fuzzy logic

        Args:
            colName (string): level name used in the output DF
            predicate (Column): a condition column, no match if this condition evaluates as false
            valueExpr (Column): a value column, which typically return a score. Higher score means higher chance of matching
            threshold (float): No match if the evaluated `valueExpr` < threshold

        Example:
            >>> FuzzyLogic("Levenshtein_City", lit(True), normlevenshtein(col("city"),col("_city")), 0.9)

        Returns:
            (FuzzyLogic)
    """
    pjc = predicate._jc
    return SmvApp.getInstance()._jvm.org.tresamigos.smv.matcher.FuzzyLogic(colName, pjc, valueExpr._jc, threshold)

class SmvEntityMatcher(object):
    """Perform multiple level entity matching with exact and/or fuzzy logic

        Args:
            leftId (string): id column name of left DF (df1)
            rightId (string): id column name of right DF (df2)
            exactMatchFilter (ExactMatchPreFilter): exact match condition, if records matched no further tests will be performed
            groupCondition (GroupCondition): for exact match leftovers, a deterministic condition to narrow down the search space
            levelLogics (list(ExactLogic or FuzzyLogic)): A list of level match conditions (always weaker than exactMatchFilter), all of them will be tested

        Example:
            code::

                SmvEntityMatcher("id", "_id",
                    ExactMatchPreFilter("Full_Name_Match", col("full_name") == col("_full_name")),
                    GroupCondition(soundex("first_name") == soundex("_first_name")),
                    [
                        ExactLogic("First_Name_Match", col("first_name") == col("_first_name")),
                        FuzzyLogic("Levenshtein_City", lit(True), normlevenshtein(col("city"),col("_city")), 0.9)
                    ]
                )
    """
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
        """Apply `SmvEntityMatcher` to the 2 DataFrames

            Args:
                df1 (DataFrame): DataFrame 1 with an id column with name "id"
                df2 (DataFrame): DataFrame 2 with an id column with name "id"
                keepOriginalCols (boolean): whether to keep all input columns of df1 and df2, defaults to true

            Example:
                code::

                    SmvEntityMatcher("id", "_id",
                        ExactMatchPreFilter("Full_Name_Match", col("full_name") == col("_full_name")),
                        GroupCondition(soundex("first_name") == soundex("_first_name")),
                        [
                            ExactLogic("First_Name_Match", col("first_name") == col("_first_name")),
                            FuzzyLogic("Levenshtein_City", lit(True), normlevenshtein(col("city"),col("_city")), 0.9)
                        ]
                    ).doMatch(df1, df2, False)

            Returns:
                (DataFrame): a DataFrame with df1's id and df2's id and match flags of all the levels. For levels with fuzzy logic, the matching score is also provided. A column named "MatchBitmap" also provided to summarize all the matching flags. When keepOriginalCols is true, input columns are also kept
        """
        jres = self.jem.doMatch(df1._jdf, df2._jdf, keepOriginalCols)
        return DataFrame(jres, SmvApp.getInstance().sqlContext)
