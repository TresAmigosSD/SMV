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
from smv import SmvApp

class SmvConfigTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-app-dir', cls.resourceTestDir(),
                '--smv-props', 'smv.test2=in_cmd_line',
                '--input-dir', 'TestInput',
                '-m', "None"]

    def test_basic_props_priority(self):
        props = self.smvApp.py_smvconf.merged_props()
        self.assertEqual(props.get('smv.test0'), 'in_app_conf')
        self.assertEqual(props.get('smv.test1'), 'in_user_conf')
        self.assertEqual(props.get('smv.test2'), 'in_cmd_line')

    def test_can_get_conn_conf(self):
        props = self.smvApp.py_smvconf.merged_props()
        self.assertEqual(props.get('smv.conn.myhdfs.type'), 'hdfs')

    def test_conf_stages(self):
        self.assertEqual(self.smvApp.py_smvconf.stage_names(), ['s1', 's2', 's3'])

    def test_input_dir_override(self):
        data_dirs = self.smvApp.py_smvconf.all_data_dirs()
        self.assertEqual(data_dirs.get('inputDir'), 'TestInput')
        self.assertEqual(data_dirs.get('outputDir'), self.tmpDataDir() + '/output')
