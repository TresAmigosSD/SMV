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

import shutil
import os

class HdfsBridgeTest(SmvBaseTest):
    BinFile = 'bitcoin128.png'
    TextFile = 'UTF-8-demo.txt'

    def copy_and_compare(self, fn):
        """Helper method to copy a test resource to hdfs"""

        # invoke the hdfs copy method
        srcpath = os.path.join(self.resourceTestDir(), fn)
        destpath = os.path.join(self.tmpTestDir(), fn)
        with open(srcpath, 'rb') as f: self.smvApp.copyToHdfs(f, destpath)

        # read the original content
        with open(srcpath, 'rb') as f:
            original = f.read()

        # read the copy content
        with open(destpath, 'rb') as f:
            hdfscopy = f.read()

        self.assertEqual(original, hdfscopy)

    def test_binary_file_can_be_copied(self):
        self.copy_and_compare(self.BinFile)

    def test_text_file_can_be_copied(self):
        self.copy_and_compare(self.TextFile)
