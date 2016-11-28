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
from smv import SmvPyDataSet, SmvApp, for_name

import imp
import sys

class BaseModule(SmvPyDataSet):
    """Base class for modules written for testing"""
    def requiresDS(self):
        return []
    def run(self, i):
        sqlcontext = self.smvapp.sqlContext
        from pyspark.sql.types import StructType
        return sqlContext.createDataFrame(sqlContext._sc.emptyRDD(), StructType([]))
    def doRun(self, validator, known):
        return self.run(known)
    @classmethod
    def hashsource(cls, src, fname='inline'):
        return hash(compile(src, fname, 'exec'))

class ModuleHashTest(SmvBaseTest):
    def test_add_comment_should_not_change_hash(self):
        a = """
class A(BaseModule):
    def add(this, a, b):
        return a+b
"""
        b = """
class A(BaseModule):
    # comments should not change hash
    def add(this, a, b):
        return a+b
"""
        self.assertEquals(BaseModule.hashsource(a), BaseModule.hashsource(b))

    def test_change_code_should_change_hash(self):
        a = """
class A(BaseModule):
    def add(this, a, b):
        return a+b
"""
        b = """
class A(BaseModule):
    def add(this, a, b, c=1):
        return a+b+c
"""
        self.assertFalse(BaseModule.hashsource(a) == BaseModule.hashsource(b))

    def test_change_dependency_should_change_hash(self):
        a = """class A(BaseModule):
        pass
"""
        b = """class B(BaseModule):
        pass
"""
        c1 = """class C(BaseModule):
        def requiresDS(self): return [A]
"""
        c2 = """class C(BaseModule):
        def requiresDS(self): return [B]
"""

        exec(a+b+c1, globals())
        h1 = for_name(self.__module__ + ".C")(SmvApp).datasetHash()

        exec(a+b+c2, globals())
        h2 = for_name(self.__module__ + ".C")(SmvApp).datasetHash()

        self.assertFalse(h1 == h2)

    def test_change_baseclass_should_change_hash(self):
        p1 = """class Parent(BaseModule):
    def test(self):
        return True
"""
        p2 = """class Parent(BaseModule):
    def test(self):
        return False
"""
        a = """class A(Parent):
    def m(self,a):
        return a
"""
        exec(p1 + a, globals())
        h1 = for_name(self.__module__ + ".A")(SmvApp).datasetHash()

        exec(p2 + a, globals())
        h2 = for_name(self.__module__ + ".A")(SmvApp).datasetHash()

        self.assertFalse(h1 == h2)

    def test_change_upstream_module_should_not_change_datasethash(self):
        p1 = """class Upstream(BaseModule):
    def f(self): return True
"""
        p2 = """class Upstream(BaseModule):
    def f(self): return False
"""
        a = """class A(BaseModule):
    def requiresDS(self): return [Upstream]
    def m(self, a): return a
"""
        exec(p1 + a, globals())
        d1 = for_name(self.__module__ + ".A")(SmvApp).datasetHash()

        exec(p2 + a, globals())
        d2 = for_name(self.__module__ + ".A")(SmvApp).datasetHash()

        self.assertTrue(d1 == d2, "datasetHash should be the same")
