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

from smvbasetest import SmvBaseTest
from smv import SmvApp

from fixture.stage2.links import L,B

import unittest

class ModuleLinkTest(SmvBaseTest):
    PublishDir = 'testpub'

    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=fixture.stage1:fixture.stage2',
                '-m', 'output.A', '--publish', cls.PublishDir]

    @classmethod
    def tearDownClass(cls):
        import shutil
        import os
        shutil.rmtree(os.path.join(cls.DataDir, 'publish', cls.PublishDir), ignore_errors=True)

    def setUp(self):
        super(ModuleLinkTest, self).setUp()

    def test_module_link_can_be_resolved(self):
        self.smvApp.j_smvApp.run()
        l = self.smvApp.runModule(L.urn())
        lExpected = self.createDF("k:String;v:Integer", "a,;b,2")
        self.should_be_same(lExpected, l) # link resolution

        b = self.smvApp.runModule(B.urn())
        bExpected = self.createDF("k:String;v:Integer;v2:Integer", "a,,;b,2,3")
        self.should_be_same(bExpected, b) # link as dependency
