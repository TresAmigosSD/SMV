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

import datetime
import pyspark.sql.types as sql_types
from smv.smvapp import SmvApp

class SmvSchema(object):
    """The Python representation of SmvSchema scala class.

    Most of the work is still being done on the scala side and this is just a pass through.
    """
    def __init__(self, j_smv_schema):
        self.j_smv_schema = j_smv_schema
        self.spark_schema = self._toStructType()

    @staticmethod
    def discover(csv_path, csvAttributes, n=100000):
        """Discover schema from CSV file with given csv attributes"""
        smvApp = SmvApp.getInstance()
        j_smv_schema = smvApp.discoverSchemaAsSmvSchema(csv_path, csvAttributes, n)
        return SmvSchema(j_smv_schema)

    @staticmethod
    def fromFile(schema_file):
        smvApp = SmvApp.getInstance()
        j_smv_schema = smvApp.smvSchemaObj.fromFile(smvApp.j_smvApp.sc(), schema_file)
        return SmvSchema(j_smv_schema)

    @staticmethod
    def fromString(schema_str):
        smvApp = SmvApp.getInstance()
        j_smv_schema = smvApp.smvSchemaObj.fromString(schema_str)
        return SmvSchema(j_smv_schema)

    def toValue(self, i, str_val):
        """convert the string value to native value based on type defined in schema.

           For example, if first column was of type Int, then call of `toValue(0, "55")`
           would return integer 55.
        """
        val = self.j_smv_schema.toValue(i, str_val)
        j_type = self.spark_schema.fields[i].dataType.typeName()
        if j_type == sql_types.DateType.typeName():
            val = datetime.date(1900 + val.getYear(), val.getMonth()+1, val.getDate())
        return val

    def saveToLocalFile(self, schema_file):
        """Save schema to local (driver) file"""
        self.j_smv_schema.saveToLocalFile(schema_file)

    def _scala_to_python_field_type(self, scala_field_type):
        """create a python FieldType from the scala field type"""
        col_name = str(scala_field_type.name())
        col_type_name = str(scala_field_type.dataType())
        # map string "IntegerType" to actual class IntegerType
        col_type_class = getattr(sql_types, col_type_name)
        return sql_types.StructField(col_name, col_type_class())

    def _toStructType(self):
        """return equivalent Spark schema (StructType) from this smv schema"""
        # ss is the raw scala spark schema (Scala StructType).  This has no
        # iterator defined on the python side, so we use old school for loop.
        ss = self.j_smv_schema.toStructType()
        spark_schema = sql_types.StructType()
        for i in range(ss.length()):
            # use "apply" to get the nth StructField item in StructType
            ft = self._scala_to_python_field_type(ss.apply(i))
            spark_schema = spark_schema.add(ft)
        return spark_schema
