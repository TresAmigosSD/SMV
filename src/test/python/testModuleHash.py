from smvbasetest import SmvBaseTest
from smv import SmvPyDataSet, for_name

import imp
import sys

class BaseModule(SmvPyDataSet):
    """Base class for modules written for testing"""
    def requiresDS(self):
        return []
    def run(self, i):
        sqlcontext = self.smv.sqlContext
        from pyspark.sql.types import StructType
        return sqlContext.createDataFrame(sqlContext._sc.emptyRDD(), StructType([]))

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
        h1 = for_name(self.__module__ + ".C").datasetHash()

        exec(a+b+c2, globals())
        h2 = for_name(self.__module__ + ".C").datasetHash()

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
        h1 = for_name(self.__module__ + ".A").datasetHash()

        exec(p2 + a, globals())
        h2 = for_name(self.__module__ + ".A").datasetHash()

        self.assertFalse(h1 == h2)