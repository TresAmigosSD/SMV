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
"""
Errors thrown by SMV
"""
import json

class SmvRuntimeError(RuntimeError):
    def __init__(self,msg):
        super(SmvRuntimeError,self).__init__(msg)

class SmvDqmValidationError(SmvRuntimeError):
    """ This class has an instance of dqmValidationResult(dict)
    """
    def __init__(self, dqmValidationResult):
        super(SmvDqmValidationError,self).__init__(json.dumps(dqmValidationResult))
        self.dqmValidationResult = dqmValidationResult

class SmvMetadataValidationError(SmvRuntimeError):
    pass