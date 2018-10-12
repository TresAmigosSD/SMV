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

class SmvLockStatusTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return [
            "--smv-props", 
            "smv.stages=stage"
        ]

    def test_no_lock_before_after_run(self):
        fqn = "stage.modules.X"
        j_m = self.load(fqn)[0]
        self.assertEqual(j_m.persistStgy.lockfileStatus(), self.smvApp.scalaNone())
        self.df(fqn, True)
        self.assertEqual(j_m.persistStgy.lockfileStatus(), self.smvApp.scalaNone())

    def test_should_lock_when_other_app_running(self):
        fqn = "stage.modules.Y"
        t1_running = False
        status = self.smvApp.scalaNone()
        s2 = 0
        def t1():
            global t1_running
            t1_running = True
            self.df(fqn, True)

        t1th = threading.Thread(target=t1)

        def t2():
            global t1_running
            while(not t1_running):
                time.sleep(0.01)
            time.sleep(0.2)
            self.assertNotEqual(
                self.load(fqn)[0].persistStgy.lockfileStatus(),
                self.smvApp.scalaNone()
            )

        t2th = threading.Thread(target=t2)

        t1th.start()
        t2th.start()

        t1th.join()
        t2th.join()

        self.assertEqual(
            self.load(fqn)[0].persistStgy.lockfileStatus(),
            self.smvApp.scalaNone()
        )