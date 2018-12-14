/*
 * This file is licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tresamigos.smv.matcher

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.tresamigos.smv._, smvfuncs._

trait NameMatcherTestFixture extends SmvTestUtil {
  def createDF1 = dfFrom(
    "id:String; first_name:String; last_name:String; address:String; city:String; state:String; zip:String; full_name:String",
    "1,George,Jetson,100 Skyway Drive,Metropolis,CA,90210,George Jetson;" +
      "2,Fred,Flintsone,900 Rockaway Road,Pebbleton,CA,90210,Fred Flintstone;" +
      "3,George,Washington,1600 Pennsylvania Avenue,Washington,DC,20006,George Washington"
  )

  def createDF2 =
    dfFrom(
      "id:String; first_name:String; last_name:String; address:String; city:String; state:String; zip:String; full_name:String",
      "1,Fred,Flintsone,900 Rockaway Road,Pebbleton,CA,90210,Fred Flintstone;" +
        "2,Alice,Kramden,328 Chauncey Street,Brooklyn,NY,11233,Alice Kramden;" +
        "3,Georje,Jetson,101 Skyway Drive,Metropolis,CA,90120,Georje Jetson"
    ).smvPrefixFieldNames("_")

  def dfPair: (DataFrame, DataFrame) = (createDF1, createDF2.smvPrefixFieldNames("_"))
}

class SmvEntityMatcherTest extends NameMatcherTestFixture {

  test("Main name matcher: all parameters") {
    val ssc = sqlContext
    import ssc.implicits._

    val resultDF = SmvEntityMatcher(
      "id",
      "_id",
      ExactMatchPreFilter("Full_Name_Match", $"full_name" === $"_full_name"),
      GroupCondition(soundex($"first_name") === soundex($"_first_name")),
      List(
        ExactLogic("First_Name_Match", $"first_name" === $"_first_name"),
        FuzzyLogic("Levenshtein_City", lit(true), normlevenshtein($"city", $"_city"), 0.9f)
      )
    ).doMatch(createDF1, createDF2, false)

    assertSrddSchemaEqual(
      resultDF,
      "id: String; _id: String; Full_Name_Match: Boolean; First_Name_Match: Boolean; Levenshtein_City: Boolean; Levenshtein_City_Value: Float; MatchBitmap: String"
    )

    assertUnorderedSeqEqual(resultDF.collect.map(_.toString),
                            Seq("[2,1,true,null,null,null,100]", "[1,3,false,false,true,1.0,001]"))
  }

  test("GroupCondition only takes EqualTo expressions") {
    val e = intercept[SmvUnsupportedType] {
      GroupCondition(col("first_name") < col("full_name"))
    }

    assert(e.getMessage === "Expression should be in left === right form")
  }

  test("NoOpPreFilter test") {
    val resultDF = SmvEntityMatcher(
      "id",
      "_id",
      NoOpPreFilter,
      GroupCondition(soundex(col("first_name")) === soundex(col("_first_name"))),
      List(
        FuzzyLogic("Zip_And_Levenshtein_City",
                   col("zip") === col("_zip"),
                   normlevenshtein(col("city"), col("_city")),
                   0.9f),
        ExactLogic("Zip_Not_Match", col("zip") =!= col("_zip"))
      )
    ).doMatch(createDF1, createDF2, false)

    assertSrddSchemaEqual(
      resultDF,
      "id: String;_id: String;Zip_And_Levenshtein_City: Boolean;Zip_Not_Match: Boolean;Zip_And_Levenshtein_City_Value: Float;MatchBitmap: String")
    assertUnorderedSeqEqual(resultDF.collect.map(_.toString),
                            Seq(
                              "[1,3,false,true,1.0,01]",
                              "[2,1,true,false,1.0,10]",
                              "[3,3,false,true,0.0,01]"
                            ))
  }

  test("NoOpGroupCondition test") {
    val ssc = sqlContext
    import ssc.implicits._

    val resultDF = SmvEntityMatcher(
      "id",
      "_id",
      ExactMatchPreFilter("Full_Name_Match", $"full_name" === $"_full_name"),
      NoOpGroupCondition,
      Seq(
        FuzzyLogic("Zip_And_Levenshtein_City",
                   $"zip" === $"_zip",
                   normlevenshtein($"city", $"_city"),
                   0.9f),
        ExactLogic("Zip_Not_Match", $"zip" =!= $"_zip")
      )
    ).doMatch(createDF1, createDF2, false)

    assertSrddSchemaEqual(
      resultDF,
      "id: String;_id: String;Full_Name_Match: Boolean;Zip_And_Levenshtein_City: Boolean;Zip_Not_Match: Boolean;Zip_And_Levenshtein_City_Value: Float;MatchBitmap: String"
    )
    assertUnorderedSeqEqual(
      resultDF.collect.map(_.toString),
      Seq(
        "[2,1,true,null,null,null,100]",
        "[3,3,false,false,true,0.0,001]",
        "[3,2,false,false,true,0.100000024,001]",
        "[1,3,false,false,true,1.0,001]",
        "[1,2,false,false,true,0.3,001]"
      )
    )

  }
  //TODO: NoOpGroupCondition test, and all python side interface
}
