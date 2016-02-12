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

  def dfPair: (DataFrame, DataFrame) = (createDF1, createDF2.prefixFieldNames("_"))
}

class SmvEntityMatcherTest extends NameMatcherTestFixture {

  test("Main name matcher: all parameters") {
    val ssc = sqlContext
    import ssc.implicits._

    val resultDF = SmvEntityMatcher(
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

  test("Main name matcher: only fuzzy level") {
    val ssc = sqlContext
    import ssc.implicits._

    val resultDF = SmvEntityMatcher(
      null,
      null,
      List(
        FuzzyLevelMatcher("Zip_And_Levenshtein_City", $"zip" === $"_zip", StringMetricUDFs.levenshtein($"city",$"_city"), 0.9f)
      )
    ).doMatch(createDF1, createDF2)

    assertSrddSchemaEqual(resultDF, "id:String; _id:String; Zip_And_Levenshtein_City:Boolean; Zip_And_Levenshtein_City_Value: Float")

    assertUnorderedSeqEqual(resultDF.collect.map(_.toString), Seq(
      "[2,1,true,1.0]"
    ))
  }

  test("Main name matcher: only exact level") {
    val ssc = sqlContext
    import ssc.implicits._

    val resultDF = SmvEntityMatcher(
      null,
      null,
      List(
        ExactLevelMatcher("Zip_Match", $"zip" === $"_zip")
      )
    ).doMatch(createDF1, createDF2)

    assertSrddSchemaEqual(resultDF, "id:String; _id:String; Zip_Match:Boolean")

    assertUnorderedSeqEqual(resultDF.collect.map(_.toString), Seq(
      "[1,1,true]",
      "[2,1,true]"
    ))
  }

  test("Main name matcher: only exact and fuzzy levels") {
    val ssc = sqlContext
    import ssc.implicits._

    val resultDF = SmvEntityMatcher(
      null,
      null,
      List(
        FuzzyLevelMatcher("Zip_And_Levenshtein_City", $"zip" === $"_zip", StringMetricUDFs.levenshtein($"city",$"_city"), 0.9f),
        ExactLevelMatcher("Zip_Not_Match", $"zip" !== $"_zip")
      )
    ).doMatch(createDF1, createDF2)

    assertSrddSchemaEqual(resultDF, "id:String; _id:String; Zip_And_Levenshtein_City:Boolean; Zip_And_Levenshtein_City_Value: Float; Zip_Not_Match:Boolean")

    assertUnorderedSeqEqual(resultDF.collect.map(_.toString), Seq(
      "[1,2,false,0.3,true]",
      "[1,3,false,1.0,true]",
      "[2,1,true,1.0,false]",
      "[3,1,false,0.3,true]",
      "[2,2,false,0.111111104,true]",
      "[2,3,false,0.100000024,true]",
      "[3,2,false,0.100000024,true]",
      "[3,3,false,0.0,true]"
    ))
  }

  test("Main name matcher: common + exact & fuzzy") {
    val ssc = sqlContext
    import ssc.implicits._

    val resultDF = SmvEntityMatcher(
      null,
      CommonLevelMatcherExpression(StringMetricUDFs.soundexMatch($"first_name", $"_first_name")),
      List(
        FuzzyLevelMatcher("Zip_And_Levenshtein_City", $"zip" === $"_zip", StringMetricUDFs.levenshtein($"city",$"_city"), 0.9f),
        ExactLevelMatcher("Zip_Not_Match", $"zip" !== $"_zip")
      )
    ).doMatch(createDF1, createDF2)

    assertSrddSchemaEqual(resultDF, "id:String; _id:String; Zip_And_Levenshtein_City:Boolean; Zip_And_Levenshtein_City_Value: Float; Zip_Not_Match:Boolean")

    assertUnorderedSeqEqual(resultDF.collect.map(_.toString), Seq(
      "[1,3,false,1.0,true]",
      "[2,1,true,1.0,false]",
      "[3,3,false,0.0,true]"
    ))
  }

  test("Main name matcher: filter + exact & fuzzy") {
    val ssc = sqlContext
    import ssc.implicits._

    val resultDF = SmvEntityMatcher(
      ExactMatchFilter("Full_Name_Match", $"full_name" === $"_full_name"),
      null,
      List(
        FuzzyLevelMatcher("Zip_And_Levenshtein_City", $"zip" === $"_zip", StringMetricUDFs.levenshtein($"city",$"_city"), 0.9f),
        ExactLevelMatcher("Zip_Not_Match", $"zip" !== $"_zip")
      )
    ).doMatch(createDF1, createDF2)

    assertSrddSchemaEqual(resultDF, "id:String; _id:String; Full_Name_Match:Boolean; Zip_And_Levenshtein_City:Boolean; Zip_And_Levenshtein_City_Value: Float; " +
      "Zip_Not_Match:Boolean")

    assertUnorderedSeqEqual(resultDF.collect.map(_.toString), Seq(
      "[2,1,true,null,null,null]",
      "[1,2,false,false,0.3,true]",
      "[1,3,false,false,1.0,true]",
      "[3,2,false,false,0.100000024,true]",
      "[3,3,false,false,0.0,true]"
    ))
  }

}

class ExactMatchFilterTest extends NameMatcherTestFixture {
  test("Test the filter") {
    val ssc = sqlContext
    import ssc.implicits._

    val (df1, df2) = dfPair

    val emf = ExactMatchFilter("Full_Name_Match", $"full_name" === $"_full_name")
    val result = emf.extract(df1, df2)

    assertSrddSchemaEqual(result.remainingDF1, "id:String; first_name:String; last_name:String; address:String; city:String; state:String; zip:String; full_name:String")
    assertUnorderedSeqEqual(result.remainingDF1.collect.map(_.toString), Seq(
      "[1,George,Jetson,100 Skyway Drive,Metropolis,CA,90210,George Jetson]",
      "[3,George,Washington,1600 Pennsylvania Avenue,Washington,DC,20006,George Washington]"
    ) )

    assertSrddSchemaEqual(result.remainingDF2, "_id:String; _first_name:String; _last_name:String; _address:String; _city:String; _state:String; _zip:String; _full_name:String")
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

class FuzzyLevelMatcherTest extends NameMatcherTestFixture {
  test("Fuzzy level matcher: column name functions") {
    val df1 = createDF1
    import df1.sqlContext.implicits._

    val colName = "Result"
    val target = FuzzyLevelMatcher(colName, null, StringMetricUDFs.levenshtein($"first_name", $"full_name"), 0.5f)

    // verify column name functions
    target.getMatchColName shouldBe colName
    target.valueColName shouldBe colName + "_Value"
  }

  test("Fuzzy level matcher parameters: null exact, fuzzy") {
    val df1 = createDF1
    import df1.sqlContext.implicits._

    val colName = "Result"
    val target = FuzzyLevelMatcher(colName, null, StringMetricUDFs.levenshtein($"first_name", $"full_name"), 0.5f)
    val res = target.addCols(df1)

    assertSrddSchemaEqual(res, "id:String; first_name:String; last_name:String; address:String; city:String; state:String; zip:String; full_name:String; " +
      target.getMatchColName + ":Boolean; " + target.valueColName + ":Float" )

    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[1,George,Jetson,100 Skyway Drive,Metropolis,CA,90210,George Jetson,false,0.46153843]",
      "[2,Fred,Flintsone,900 Rockaway Road,Pebbleton,CA,90210,Fred Flintstone,false,0.26666665]",
      "[3,George,Washington,1600 Pennsylvania Avenue,Washington,DC,20006,George Washington,false,0.35294116]"
    ) )

  }

  test("Fuzzy level matcher parameters: exact, null fuzzy") {
    val df1 = createDF1
    import df1.sqlContext.implicits._

    val colName = "Result"

    val e = intercept[IllegalArgumentException] {
      FuzzyLevelMatcher(colName, StringMetricUDFs.soundexMatch($"first_name", $"full_name"), null, 0f)
    }

    assert(e.getMessage === "requirement failed")
  }

  test("Fuzzy level matcher parameters: exact and fuzzy") {
    val (df1, df2) = dfPair
    import df1.sqlContext.implicits._

    val colName = "Result"
    val target = FuzzyLevelMatcher(colName, StringMetricUDFs.soundexMatch($"first_name", $"full_name"), StringMetricUDFs.levenshtein($"first_name", $"full_name"), 0.5f)
    val res = target.addCols(df1)

    assertSrddSchemaEqual(res, "id:String; first_name:String; last_name:String; address:String; city:String; state:String; zip:String; full_name:String; " +
      target.getMatchColName + ":Boolean; " + target.valueColName + ":Float" )

    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[1,George,Jetson,100 Skyway Drive,Metropolis,CA,90210,George Jetson,false,0.46153843]",
      "[2,Fred,Flintsone,900 Rockaway Road,Pebbleton,CA,90210,Fred Flintstone,false,0.26666665]",
      "[3,George,Washington,1600 Pennsylvania Avenue,Washington,DC,20006,George Washington,false,0.35294116]"
    ) )

  }

}

class NoOpExactMatchFilterTest extends NameMatcherTestFixture {
  test("No-op filter") {
    val (df1, df2) = dfPair
    import df1.sqlContext.implicits._

    val ExactMatchFilterResult(r1, r2, ex) = NoOpExactMatchFilter.extract(df1, df2)

    assert(r1 == df1)
    assert(r2 == df2)

    assertSrddSchemaEqual(ex, "id:String; _id:String")

    assertUnorderedSeqEqual(ex.collect.map(_.toString), Seq() )

  }
}

class CommonLevelMatcherNoneTest extends NameMatcherTestFixture {
  test("No-op filter") {
    val df1 = createDF1
    val df2 = createDF2

    import df1.sqlContext.implicits._

    val res = CommonLevelMatcherNone.join(df1, df2)

    assertSrddSchemaEqual(res, "id:String; first_name:String; last_name:String; address:String; city:String; state:String; zip:String; full_name:String; id:String; first_name:String; last_name:String; address:String; city:String; state:String; zip:String; full_name:String")

    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[1,George,Jetson,100 Skyway Drive,Metropolis,CA,90210,George Jetson,1,Fred,Flintsone,900 Rockaway Road,Pebbleton,CA,90210,Fred Flintstone]",
      "[1,George,Jetson,100 Skyway Drive,Metropolis,CA,90210,George Jetson,2,Alice,Kramden,328 Chauncey Street,Brooklyn,NY,11233,Alice Kramden]",
      "[1,George,Jetson,100 Skyway Drive,Metropolis,CA,90210,George Jetson,3,Georje,Jetson,101 Skyway Drive,Metropolis,CA,90120,Georje Jetson]",
      "[2,Fred,Flintsone,900 Rockaway Road,Pebbleton,CA,90210,Fred Flintstone,1,Fred,Flintsone,900 Rockaway Road,Pebbleton,CA,90210,Fred Flintstone]",
      "[2,Fred,Flintsone,900 Rockaway Road,Pebbleton,CA,90210,Fred Flintstone,2,Alice,Kramden,328 Chauncey Street,Brooklyn,NY,11233,Alice Kramden]",
      "[2,Fred,Flintsone,900 Rockaway Road,Pebbleton,CA,90210,Fred Flintstone,3,Georje,Jetson,101 Skyway Drive,Metropolis,CA,90120,Georje Jetson]",
      "[3,George,Washington,1600 Pennsylvania Avenue,Washington,DC,20006,George Washington,1,Fred,Flintsone,900 Rockaway Road,Pebbleton,CA,90210,Fred Flintstone]",
      "[3,George,Washington,1600 Pennsylvania Avenue,Washington,DC,20006,George Washington,2,Alice,Kramden,328 Chauncey Street,Brooklyn,NY,11233,Alice Kramden]",
      "[3,George,Washington,1600 Pennsylvania Avenue,Washington,DC,20006,George Washington,3,Georje,Jetson,101 Skyway Drive,Metropolis,CA,90120,Georje Jetson]"
    ) )

  }
}

class ExactLevelMatcherTest extends NameMatcherTestFixture {
  test("Exact level matcher: all parameters") {
    val df1 = createDF1
    import df1.sqlContext.implicits._

    val colName = "Result"
    val target = ExactLevelMatcher(colName, $"id" % 2 cast BooleanType as "c")

    val res = target.addCols(df1)

    assertSrddSchemaEqual(res, "id:String; first_name:String; last_name:String; address:String; city:String; state:String; zip:String; full_name:String; " +
      target.getMatchColName + ":Boolean")

    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[1,George,Jetson,100 Skyway Drive,Metropolis,CA,90210,George Jetson,true]",
      "[2,Fred,Flintsone,900 Rockaway Road,Pebbleton,CA,90210,Fred Flintstone,false]",
      "[3,George,Washington,1600 Pennsylvania Avenue,Washington,DC,20006,George Washington,true]"
    ) )

  }

  test("Exact level matcher: null column name") {
    val df1 = createDF1
    import df1.sqlContext.implicits._

    val colName = "Result"

    val e = intercept[IllegalArgumentException] {
      ExactLevelMatcher(null, $"id" % 2 cast BooleanType as "c")
    }

    assert(e.getMessage === "requirement failed")
  }
}
