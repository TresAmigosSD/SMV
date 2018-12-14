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

import smv
import testSmvFramework

class BaseMod(smv.SmvModule):
    def requiresDS(self):
        return []

    def run(self, i):
        return self.smvApp.createDF("foo: Integer; bar: String", "2,jkl;1,mno")

class ModWithUserMeta(BaseMod):
    def metadata(self, df):
        return {'foo': 'bar'}

class ModWithFailingValidation(BaseMod):
    def validateMetadata(self, current, history):
        return "FAILURE"

class ModWithInvalidMetadata(BaseMod):
    def metadata(self, df):
        return "NOT A DICT"

class ModWithInvalidMetadataValidation(BaseMod):
    def validateMetadata(self, current, history):
        x = lambda: 1
        x.__name__ = "NOT_A_STRING"
        return x

class ModWithMetaCount(BaseMod):
    def isEphemeral(self):
        return True

    def metadata(self, df):
        # keep count of how many times `metadata` is called.
        testSmvFramework.metadata_count = testSmvFramework.metadata_count + 1
        return {'foo': 'bar'}

class DependsOnMetaCount(smv.SmvModule):
    def requiresDS(self):
        return [ModWithMetaCount]

    def run(self, i):
        return i[ModWithMetaCount]