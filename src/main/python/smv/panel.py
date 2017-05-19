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
"""SMV PartialTime and TimePanel Framework

This module defines the most used PartialTime including `Day`, `Month`, `Quarter`, and the TimePanel
"""

from smvapp import SmvApp

def Quarter(year, quarter):
    """Define an smv.panel.Quarter

        Args:
            year (int):
            quarter (int):

        Returns:
            (java object smv.panel.Quarter)
    """
    return SmvApp.getInstance()._jvm.Quarter(year, quarter)

def Month(year, month):
    """Define an smv.panel.Month

        Args:
            year (int):
            month (int):

        Returns:
            (java object smv.panel.Month)
    """
    return SmvApp.getInstance()._jvm.Month(year, month)

def Day(year, month, day):
    """Define an smv.panel.Day

        Args:
            year (int):
            month (int):
            day (int):

        Returns:
            (java object smv.panel.Day)
    """
    return SmvApp.getInstance()._jvm.Day(year, month, day)

def TimePanel(start, end):
    """Define an smv.panel.TimePanel

        Args:
            start (java object smv.PartialTime): Quarter, Month, Day etc.
            end (java object smv.PartialTime): Quarter, Month, Day etc.

        Example:

            >>> tp = TimePanel(Day(2012, 1, 1), Day(2013, 12, 31))

        Returns:
            (java object smv.panel.TimePanel)
    """
    return SmvApp.getInstance()._jvm.TimePanel(start, end)
