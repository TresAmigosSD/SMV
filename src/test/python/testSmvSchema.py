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
from smv import SmvSchema
import pyspark.sql.types as st
import datetime
import os

class SmvSchemaTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage']

    def test_fromString(self):
        s = SmvSchema.fromString("a:string; b:double")
        fields = s.spark_schema.fields
        assert(len(fields) == 2)
        assert(fields[0] == st.StructField('a', st.StringType()))
        assert(fields[1] == st.StructField('b', st.DoubleType()))

    def test_fromFile(self):
        f = os.path.join(SmvSchemaTest.resourceTestDir(), "data/a.schema")
        s = SmvSchema.fromFile(f)
        fields = s.spark_schema.fields
        assert(len(fields) == 2)
        assert(fields[0] == st.StructField('a', st.StringType()))
        assert(fields[1] == st.StructField('b', st.FloatType()))

    def test_toValue(self):
        s = SmvSchema.fromString("a:string; b:double; c:Date[yyyy-MM-dd]")

        str_v = s.toValue(0, "abc")
        assert(str_v == "abc")

        double_v = s.toValue(1, "5.3")
        assert(abs(double_v - 5.3) < 0.001)

        date_v = s.toValue(2, "2018-01-31")
        assert(date_v == datetime.date(2018,1,31))
