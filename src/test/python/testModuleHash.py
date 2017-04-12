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

import sys

from smvbasetest import SmvBaseTest
from smv import SmvApp
from smv.smvapp import DataSetRepo

class ModuleHashTest(SmvBaseTest):
    ResourcePath1 = 'src/test/python/dsh1'
    ResourcePath2 = 'src/test/python/dsh2'

    class Resource(object):
        def __init__(self, smvApp, path, fqn):
            self.dsr = DataSetRepo(smvApp)
            self.path = path
            self.fqn = fqn

        def __enter__(self):
            sys.path.insert(1,self.path)
            return self.dsr.loadDataSet(self.fqn)

        def __exit__(self, type, value, traceback):
            sys.path.remove(self.path)

    def compare_resource_hash(self, fqn, assertion):
        with self.Resource(self.smvApp,self.ResourcePath1,fqn) as ds:
            hash1 = ds.datasetHash()
        with self.Resource(self.smvApp,self.ResourcePath2,fqn) as ds:
            hash2 = ds.datasetHash()
        assertion(hash1, hash2)

    def assert_hash_should_change(self, fqn):
        self.compare_resource_hash(fqn,self.assertNotEqual)

    def assert_hash_should_not_change(self, fqn):
        self.compare_resource_hash(fqn,self.assertEqual)

    def test_add_comment_should_not_change_hash(self):
        """hash will not change if we add a comment to its code"""
        self.assert_hash_should_not_change("modules.AddComment")

    def test_change_code_should_change_hash(self):
        """hash will change if we change module's code"""
        self.assert_hash_should_change("modules.ChangeCode")

    def test_change_dependency_should_change_hash(self):
        """hash will change if we change module's requiresDS"""
        self.assert_hash_should_change("modules.Dependent")

    def test_change_baseclass_should_change_hash(self):
        """hash will change if we change code for class that module inherits from"""
        self.assert_hash_should_change("modules.Child")

    def test_change_upstream_module_should_not_change_datasethash(self):
        """hash will not change if we change module that is listed in module's requiresDS"""
        self.assert_hash_should_not_change("modules.Downstream")
