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

import unittest

from test_support.smvbasetest import SmvBaseTest

from smv.functions import normlevenshtein
from smv.matcher import *
from pyspark.sql.functions import *

class SmvEntityMatcherTest(SmvBaseTest):
    def test_MainFeatures(self):
        df1 = self.createDF("id:String; first_name:String; last_name:String; address:String; city:String; state:String; zip:String; full_name:String",
            "1,George,Jetson,100 Skyway Drive,Metropolis,CA,90210,George Jetson;" +
            "2,Fred,Flintsone,900 Rockaway Road,Pebbleton,CA,90210,Fred Flintstone;" +
            "3,George,Washington,1600 Pennsylvania Avenue,Washington,DC,20006,George Washington"
        )
        df2 = self.createDF("_id:String; _first_name:String; _last_name:String; _address:String; _city:String; _state:String; _zip:String; _full_name:String",
            "1,Fred,Flintsone,900 Rockaway Road,Pebbleton,CA,90210,Fred Flintstone;" +
            "2,Alice,Kramden,328 Chauncey Street,Brooklyn,NY,11233,Alice Kramden;" +
            "3,Georje,Jetson,101 Skyway Drive,Metropolis,CA,90120,Georje Jetson"
        )

        res = SmvEntityMatcher("id", "_id",
            ExactMatchPreFilter("Full_Name_Match", col("full_name") == col("_full_name")),
            GroupCondition(soundex("first_name") == soundex("_first_name")),
            [
                ExactLogic("First_Name_Match", col("first_name") == col("_first_name")),
                FuzzyLogic("Levenshtein_City", lit(True), normlevenshtein(col("city"),col("_city")), 0.9)
            ]
        ).doMatch(df1, df2, False)
        expect = self.createDF("id: String; _id: String; Full_Name_Match: Boolean; First_Name_Match: Boolean; Levenshtein_City: Boolean; Levenshtein_City_Value: Float; MatchBitmap: String",
              """2,1,true,,,,100;
                 1,3,false,false,true,1.0,001"""
        )
        self.should_be_same(expect, res)

    def test_NoOp(self):
        df1 = self.createDF("id:String; first_name:String; last_name:String; address:String; city:String; state:String; zip:String; full_name:String",
            "1,George,Jetson,100 Skyway Drive,Metropolis,CA,90210,George Jetson;" +
            "2,Fred,Flintsone,900 Rockaway Road,Pebbleton,CA,90210,Fred Flintstone;" +
            "3,George,Washington,1600 Pennsylvania Avenue,Washington,DC,20006,George Washington"
        )
        df2 = self.createDF("_id:String; _first_name:String; _last_name:String; _address:String; _city:String; _state:String; _zip:String; _full_name:String",
            "1,Fred,Flintsone,900 Rockaway Road,Pebbleton,CA,90210,Fred Flintstone;" +
            "2,Alice,Kramden,328 Chauncey Street,Brooklyn,NY,11233,Alice Kramden;" +
            "3,Georje,Jetson,101 Skyway Drive,Metropolis,CA,90120,Georje Jetson"
        )

        res = SmvEntityMatcher("id", "_id",
            NoOpPreFilter(),
            NoOpGroupCondition(),
            [
                ExactLogic("First_Name_Match", col("first_name") == col("_first_name")),
                FuzzyLogic("Levenshtein_City", lit(True), normlevenshtein(col("city"),col("_city")), 0.9)
            ]
        ).doMatch(df1, df2, False)
        expect = self.createDF("id: String;_id: String;First_Name_Match: Boolean;Levenshtein_City: Boolean;Levenshtein_City_Value: Float;MatchBitmap: String",
              """2,1,true,true,1.0,11;
                 1,3,false,true,1.0,01"""
        )
        self.should_be_same(expect, res)
