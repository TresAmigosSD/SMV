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
"""SMV Provider API

    This module allows user to declare/register providers.
"""

class SmvProvider(object):
    """Base class of all provider classes.

       Each provider must inherit from this class and also define `provider_type()` static
       method that returns the type as a string.

       Note: there is no version agnostic way to enforce derived classes to implement
       a static `provider_type()` so the check will be done dynamically (runtime)
    """

    IS_PROVIDER = True

    @staticmethod
    def provider_type(): return ""

