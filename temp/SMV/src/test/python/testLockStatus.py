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
import threading
import time
import os

lock_exist = None

class SmvLockStatusTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return [
            "--smv-props", 
            "smv.stages=stage",
            "smv.lock=true"
        ]

    def assert_lock(self, m, status):
        lock_file = m._lock_path()
        print(lock_file)
        if (status):
            self.assertTrue(os.path.exists(lock_file))
        else:
            self.assertFalse(os.path.exists(lock_file))

    def test_no_lock_before_after_run(self):
        fqn = "stage.modules.X"
        m = self.load(fqn)[0]
        self.assert_lock(m, False)
        self.df(fqn, True)
        self.assert_lock(m, False)

    def test_locked_when_calc_meta(self):
        self.smvApp.data_cache = {}
        self.mkTmpTestDir()
        global lock_exist
        lock_exist = False
        # Lock is around run and metadata calculation, so when check 
        # lock file in metadata method will return exist
        self.df("stage.modules.X")
        self.assertTrue(lock_exist)