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

from smv import SmvModel, SmvModelExec, SmvOutput, SmvModule

class Model(SmvModel, SmvOutput):
    def requiresDS(self):
        return []

    def run(self, i):
        x = [1,2,3]
        return x

class ModelExec(SmvModelExec):
    def requiresDS(self):
        return []

    def requiresModel(self):
        return Model

    def run(self, i, model):
        return self.smvApp.createDF("model: String", "\"{}\"".format(model))

class ModuleUsesModel(SmvModule):
    def requiresDS(self):
        return [Model]

    def run(self, i):
        return self.smvApp.createDF("a:String", "\"{}\"".format(i[Model]))