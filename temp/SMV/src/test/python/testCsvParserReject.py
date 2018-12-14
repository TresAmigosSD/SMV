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
from smv import *
from smv.error import *

class CsvParserRejectTest(SmvBaseTest):
    @classmethod
    def smvAppInitArgs(cls):
        return ['--smv-props', 'smv.stages=stage']

    def _schema_str(self):
        return """id: String;
            val: double;
            val2: timestamp[yyyyMMddHHmmss];
            val3: String"""

    def _create_csv_data(self):
        schema = self._schema_str().replace(";", "")
    
        data = """123,12.50  ,20130109130619,12102012
123,  ,20130109130619,12102012
123,12.50  ,20130109130619,12102012
123,12.50  ,130109130619,12102012
123,12.50  ,109130619,12102012
123,12.50  ,201309130619,12102012
123,12.50  ,20133109130619,12102012
123,12.50  ,12102012
123,001x  ,20130109130619,12102012
231,67.21  ,20121009101621,02122011
123,,20140817010156,22052014"""
        
        self.createTempInputFile('csv1.csv', data)
        self.createTempInputFile('csv1.schema', schema)

    def test_drop_parsing_error(self):
        self._create_csv_data()
        fqn = "stage.modules.CsvIgnoreError"
        df = self.df(fqn)
        expect = self.createDF(self._schema_str(),
            "123,,20130109130619,12102012;"
        + "123,12.5,20130109130619,12102012;"
        + "123,12.5,20150709130619,12102012;"
        + "231,67.21,20121009101621,02122011;"
        + "123,,20140817010156,22052014")
        self.should_be_same(expect, df)

    def test_rejection_message(self):
        self._create_csv_data()
        fqn = "stage.modules.CsvWithError"
        with self.assertRaises(SmvDqmValidationError) as cm:
            df = self.df(fqn)
            df.smvDumpDF()
        e = cm.exception
        self.assertEqual(
            e.dqmValidationResult['errorMessages'],
            [{"FailParserCountPolicy(1)": "false"}]
        )
    
        self.assertEqual(
            sorted(e.dqmValidationResult["checkLog"]),
            sorted([
                "java.text.ParseException: Unparseable date: \"130109130619\" @RECORD: 123,12.50  ,130109130619,12102012",
                "java.text.ParseException: Unparseable date: \"109130619\" @RECORD: 123,12.50  ,109130619,12102012",
                "java.text.ParseException: Unparseable date: \"201309130619\" @RECORD: 123,12.50  ,201309130619,12102012",
                "java.lang.IllegalArgumentException: requirement failed @RECORD: 123,12.50  ,12102012",
                "java.lang.NumberFormatException: For input string: \"001x\" @RECORD: 123,001x  ,20130109130619,12102012"
            ])
        )