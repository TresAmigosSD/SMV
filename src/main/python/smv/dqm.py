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
"""SMV DataSet Framework interface

This module defines the abstract classes which formed the SmvGenericModule Framework for clients' projects
"""

import traceback

from smv.smvapp import SmvApp

def SmvDQM():
    """Factory method for Scala SmvDQM"""
    return SmvApp.getInstance()._jvm.SmvDQM.apply()

# Factory methods for DQM policies
def FailParserCountPolicy(threshold):
    """If the total time of parser fails >= threshold, fail the DF

        Args:
            threshold (int): the threshold after which the DF fails

        Returns:
            (DQMPolicy): policy for DQM
    """
    return SmvApp.getInstance()._jvm.FailParserCountPolicy(threshold)

def FailTotalRuleCountPolicy(threshold):
    """For all the rules in a DQM, if the total number of times they are triggered is >= threshold, fail the DF

        Args:
            threshold (int): the threshold after which the DF fails

        Returns:
            (DQMPolicy): policy for DQM
    """
    return SmvApp.getInstance()._jvm.FailTotalRuleCountPolicy(threshold)

def FailTotalFixCountPolicy(threshold):
    """For all the fixes in a DQM, if the total number of times they are triggered is >= threshold, fail the DF

        Args:
            threshold (int): the threshold after which the DF fails

        Returns:
            (DQMPolicy): policy for DQM
    """
    return SmvApp.getInstance()._jvm.FailTotalFixCountPolicy(threshold)

def FailTotalRulePercentPolicy(threshold):
    """For all the rules in a DQM, if the total number of times they are triggered is >= threshold * total Records, fail the DF

        Args:
            threshold (double): the threshold after which the DF fails. value is between 0.0 and 1.0

        Returns:
            (DQMPolicy): policy for DQM
    """
    return SmvApp.getInstance()._jvm.FailTotalRulePercentPolicy(threshold * 1.0)

def FailTotalFixPercentPolicy(threshold):
    """For all the fixes in a DQM, if the total number of times they are triggered is >= threshold * total Records, fail the DF

        Args:
            threshold (double): the threshold after which the DF fails. value is between 0.0 and 1.0

        Returns:
            (DQMPolicy): policy for DQM
    """
    return SmvApp.getInstance()._jvm.FailTotalFixPercentPolicy(threshold * 1.0)

# DQM task policies
def FailNone():
    """Tasks with FailNone will not trigger any DF level policy

        Returns:
            (DQMTaskPolicy): policy for DQM Task
    """
    return SmvApp.getInstance()._jvm.DqmTaskPolicies.failNone()

def FailAny():
    """Any rule fail or fix with FailAny will cause the entire DF to fail

        Returns:
            (DQMTaskPolicy): policy for DQM Task
    """
    return SmvApp.getInstance()._jvm.DqmTaskPolicies.failAny()

def FailCount(threshold):
    """Tasks with FailCount(n) will fail the DF if the task is triggered >= n times

        Args:
            threshold (int): the threshold after which the DF fails

        Returns:
            (DQMTaskPolicy): policy for DQM Task
    """
    return SmvApp.getInstance()._jvm.FailCount(threshold)

def FailPercent(threshold):
    """Tasks with FailPercent(r) will fail the DF if the task is triggered >= r percent of the
        total number of records in the DF

        Args:
            threshold (double): the threshold after which the DF fails. value is between 0.0 and 1.0

        Returns:
            (DQMTaskPolicy): policy for DQM Task
    """
    return SmvApp.getInstance()._jvm.FailPercent(threshold * 1.0)

def DQMRule(rule, name = None, taskPolicy = None):
    """DQMRule defines a requirement on the records of a DF

        Example:

            Require the sum of "a" and "b" columns less than 100

            >>> DQMRule(col('a') + col('b') < 100.0, 'a_b_sum_lt100', FailPercent(0.01))

        Args:
            rule (Column): boolean condition that defines the requirement on the records of a DF
            name (string): optional parameter for naming the DQMRule. if not specified, defaults to the rule text
            taskPolicy (DQMTaskPolicy): optional parameter for the DQM policy. if not specified, defaults to FailNone()

        Returns:
            (DQMRule): a DQMRule object
    """
    task = taskPolicy or FailNone()
    return SmvApp.getInstance()._jvm.DQMRule(rule._jc, name, task)

def DQMFix(condition, fix, name = None, taskPolicy = None):
    """DQMFix will fix a column with a default value

        Example:

            If "age" greater than 100, make it 100

            >>> DQMFix(col('age') > 100, lit(100).alias('age'), 'age_cap100', FailNone)

        Args:
            condition (Column): boolean condition that determines when the fix should occur on the records of a DF
            fix (Column): the fix to use when replacing a value that does not pass the condition
            name (String): optional parameter for naming the DQMFix. if not specified, defaults to the condition text
            taskPolicy (DQMTaskPolicy): optional parameter for the DQM policy. if not specified, defaults to FailNone()

        Returns:
            (DQMFix): a DQMFix object
    """
    task = taskPolicy or FailNone()
    return SmvApp.getInstance()._jvm.DQMFix(condition._jc, fix._jc, name, task)
