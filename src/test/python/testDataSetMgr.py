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

from test_support.smvbasetest import SmvBaseTest
import smv.smvshell as smvshell
import os

class SmvDataSetMgrTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return [
            '--smv-props', 
            'smv.stages=stage1:stage2:stage3:stage4'
        ]

    def assertDSListMatch(self, jds_list, urn_list):
        jds_urns = [str(x.urn().toString()) for x in jds_list]
        self.assertEqual(sorted(jds_urns), sorted(urn_list))

    def test_dsmgr_can_load_module(self):
        fqn = "stage1.modules.B"
        self.load(fqn)

    def test_dsmgr_resolves_dependencies(self):
        fqn = "stage1.modules.A"
        j_m = self.load(fqn)[0]
        self.assertDSListMatch(j_m.resolvedRequiresDS().array(), 
            ['mod:stage1.modules.B', 'mod:stage1.modules.C']
        )

    def test_dsmgr_resolves_to_the_same_j_obj(self):
        mods = self.load("stage1.modules.A", "stage1.modules.C")
        b1 = mods[0].resolvedRequiresDS().array()[0]
        b2 = mods[1].resolvedRequiresDS().array()[0]
        self.assertEqual(b1, b2)

    def test_dsmgr_finds_all_ds_in_stage(self):
        dfForStage = self.smvApp.dsm.dataSetsForStage("stage1")
        self.assertDSListMatch(dfForStage, [
            'mod:stage1.modules.A', 
            'mod:stage1.modules.B', 
            'mod:stage1.modules.C'
        ])

    def test_allDataSets(self):
        allDs = self.smvApp.dsm.allDataSets()
        self.assertDSListMatch(allDs, [
            'mod:stage1.modules.A', 
            'mod:stage1.modules.B', 
            'mod:stage1.modules.C',
            'mod:stage2.modules.X',
            'mod:stage2.modules.Z',
            'mod:stage3.modules.Y'
        ])

    def test_dsmgr_does_not_find_link_in_stage(self):
        dfForStage = self.smvApp.dsm.dataSetsForStage("stage4")
        self.assertEqual(len(dfForStage), 0)