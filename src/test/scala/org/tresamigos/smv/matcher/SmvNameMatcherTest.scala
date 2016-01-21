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

import org.tresamigos.smv._

trait NameMatcherTestFixture extends SmvTestUtil {
  def createDF1 = createSchemaRdd(
    "id:String; first_name:String; last_name:String; address:String; city:String; state:String; zip:String; full_name:String",
    "1,George,Jetson,100 Skyway Drive,Metropolis,CA,90210,George Jetson;" +
      "2,Fred,Flintsone,900 Rockaway Road,Pebbleton,CA,90210,Fred Flintstone;" +
      "3,George,Washington,1600 Pennsylvania Avenue,Washington,DC,20006,George Washington"
  )

  def createDF2 = createSchemaRdd(
    "id:String; first_name:String; last_name:String; address:String; city:String; state:String; zip:String; full_name:String",
    "1,Fred,Flintsone,900 Rockaway Road,Pebbleton,CA,90210,Fred Flintstone;" +
      "2,Alice,Kramden,328 Chauncey Street,Brooklyn,NY,11233,Alice Kramden;" +
      "3,Georje,Jetson,101 Skyway Drive,Metropolis,CA,90120,Georje Jetson"
  )


}

class SmvNameMatcherTest extends NameMatcherTestFixture {

  test("Test all parameters") {
    val ssc = sqlContext
    import ssc.implicits._


    val resultDF = SmvNameMatcher(
      ExactMatchFilter("Full_Name_Match", $"full_name" === $"_full_name"),
      CommonLevelMatcherExpression(StringMetricUDFs.soundexMatch($"first_name", $"_first_name")),
      List(
        ExactLevelMatcher("First_Name_Match", $"first_name" === $"_first_name"),
        FuzzyLevelMatcher("Levenshtein_City", null, StringMetricUDFs.levenshtein($"city",$"_city"), 0.9f)
      )
    ).doMatch(createDF1, createDF2)


    assertSrddSchemaEqual(resultDF, "id:String; _id:String; Full_Name_Match:Boolean; First_Name_Match:Boolean; Levenshtein_City:Boolean; Levenshtein_City_Value:Float")
    assertUnorderedSeqEqual(resultDF.collect.map(_.toString), Seq(
      "[2,1,true,null,null,null]",
      "[1,3,false,false,true,1.0]"))
  }

}

class ExactMatchFilterTest extends NameMatcherTestFixture {
  test("Test the filter") {
    val ssc = sqlContext
    import ssc.implicits._

    val df1 = createDF1
    val df2 = createDF2.prefixFieldNames("_")

    val emf = ExactMatchFilter("Full_Name_Match", $"full_name" === $"_full_name")
    val result = emf.extract(df1, df2)

    assertSrddSchemaEqual(result.remainingDF1, "id:String; first_name:String; last_name:String; address:String; city:String; state:String; zip:String; full_name:String")
    assertUnorderedSeqEqual(result.remainingDF1.collect.map(_.toString), Seq(
      "[1,George,Jetson,100 Skyway Drive,Metropolis,CA,90210,George Jetson]",
      "[3,George,Washington,1600 Pennsylvania Avenue,Washington,DC,20006,George Washington]"
    ) )

    assertSrddSchemaEqual(result.remainingDF2, "id:String; first_name:String; last_name:String; address:String; city:String; state:String; zip:String; full_name:String")
    assertUnorderedSeqEqual(result.remainingDF2.collect.map(_.toString), Seq(
      "[2,Alice,Kramden,328 Chauncey Street,Brooklyn,NY,11233,Alice Kramden]",
      "[3,Georje,Jetson,101 Skyway Drive,Metropolis,CA,90120,Georje Jetson]"
    ) )

    assertSrddSchemaEqual(result.extracted, "id:String; _id:String")
    assertUnorderedSeqEqual(result.extracted.collect.map(_.toString), Seq(
      "[2,1]"
    ) )


  }
}