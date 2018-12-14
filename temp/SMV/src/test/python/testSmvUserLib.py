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

import os
import shutil

from test_support.smvbasetest import SmvBaseTest


class SmvUserLibTest(SmvBaseTest):

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage1']

    def setUp(self):
        app_dir = self.__class__.resourceTestDir()
        self.smvApp.setAppDir(app_dir)

    def test_discover_user_libs(self):
        user_libs = self.smvApp.userLibs()
        assert sorted(user_libs) == ['lib1', 'submod.lib2', 'submod.lib3']
