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

    def assertDSListMatch(self, jds_list, fqn_list):
        # Scaffolding for moving resolver from Scala to Python
        try:
            jds_fqns = [str(x.fqn().toString()) for x in jds_list]
        except:
            jds_fqns = [x.fqn() for x in jds_list]

        self.assertEqual(sorted(jds_fqns), sorted(fqn_list))

    def resolvedDS(self, ds):
        # Scaffolding for moving resolver from Scala to Python
        try:
            return ds.resolvedRequiresDS().array()
        except:
            return ds.resolvedRequiresDS

    def test_dsmgr_can_load_module(self):
        fqn = "stage1.modules.B"
        self.load(fqn)

    def test_dsmgr_resolves_dependencies(self):
        fqn = "stage1.modules.A"
        j_m = self.load(fqn)[0]
        self.assertDSListMatch(self.resolvedDS(j_m),
            ['stage1.modules.B', 'stage1.modules.C']
        )

    def test_dsmgr_resolves_to_the_same_j_obj(self):
        mods = self.load("stage1.modules.A", "stage1.modules.C")
        b1 = self.resolvedDS(mods[0])[0]
        b2 = self.resolvedDS(mods[1])[0]
        self.assertEqual(b1, b2)

    def test_allDataSets(self):
        allDs = self.smvApp.dsm.allDataSets()
        self.assertDSListMatch(allDs, [
            'stage1.modules.A',
            'stage1.modules.B',
            'stage1.modules.C',
            'stage2.modules.X',
            'stage2.modules.Z',
            'stage3.modules.Y'
        ])
