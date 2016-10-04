from smvbasetest import SmvBaseTest
from smv import SmvPyDataSet

class BaseModule(SmvPyDataSet):
    def description(self):
        return """Base class for modules written for testing"""
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
        pass
