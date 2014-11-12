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

package org.tresamigos.smv

import org.apache.spark.sql.catalyst.types.{DoubleType, StringType}

class NullSubTest extends SparkTestUtil {
  sparkTest("test NullSub with String and Numeric values") {
    val ssc = sqlContext; import ssc._
    val srdd = createSchemaRdd("a:String; b:Double", "A1,;,5") // B1,A2 are null!

    val res = srdd.select(NullSub('a, "NA") as 'a2, NullSub('b, 6.0) as 'b2)
    val fields = res.schema.fields
    assert(res.collect.map(_(0)) === List("A1", "NA"))
    assert(fields(0).dataType === StringType)

    assert(res.collect.map(_(1)) === List(6.0, 5.0))
    assert(fields(1).dataType === DoubleType)
  }
}

class SmvStrCatTest extends SparkTestUtil {
  sparkTest("test SmvStrCat function") {
    val ssc = sqlContext; import ssc._
    val srdd = createSchemaRdd("a:String; b:String; c:String; d:Integer",
      "a,b,c,1;x,y,z,2")
    val res = srdd.select(SmvStrCat('a, "_", 'b, "+", 'd) as 'cat)
    assertSrddDataEqual(res, "a_b+1;x_y+2")
  }
}

class LEFTTest extends SparkTestUtil {
  sparkTest("test LEFT function") {
    val ssc = sqlContext; import ssc._
    val r = sqlContext.csvFileWithSchema(testDataDir + "NonAggTest/LEFT.data.csv")
    val res = r.select(LEFT('id, 3)).collect.map(_(0))
    assert(res === Array("hoh", "foo", "hob" ))
  }
}


class TimeFuncsTest extends SparkTestUtil {
  sparkTest("test YEAR, MONTH, DAYOFMOUNTH, DAYOFWEEK, HOUR") {
    val ssc = sqlContext; import ssc.symbolToUnresolvedAttribute
    val r = sqlContext.csvFileWithSchema(testDataDir + "NonAggTest/test2")
    val res = r.select('val2, YEAR('val2), MONTH('val2), DAYOFMONTH('val2), DAYOFWEEK('val2), HOUR('val2)).collect()(0).mkString(",")
    assert(res === "2013-01-09 13:06:19.0,2013,01,09,04,13")
  }

  sparkTest("test SmvYear, SmvMonth, SmvDayOfWeek, SmvDayOfMonth, SmvQuarter") {
    val ssc = sqlContext;
    import ssc._
    import ssc.symbolToUnresolvedAttribute

    val srdd = createSchemaRdd(
      // Schema
      "time1:Timestamp[yyyy/dd/MM]; time2:Timestamp[yyyy-MM]",
      // Values
      """2014/05/03,2013-11;
         2010/30/12,2012-01
      """.stripMargin
    )

    val result = srdd.
      select(SmvYear('time1) as 'year1, SmvMonth('time1) as 'month1, SmvDayOfMonth('time1) as 'dayOfMonth1,
        SmvYear('time2) as 'year2, SmvMonth('time2) as 'month2, SmvDayOfMonth('time2) as 'dayOfMonth2,
        SmvQuarter('time1) as 'quarter1)

    assertUnorderedSeqEqual(result.collect.map(_.toString), Seq("[2014,3,5,2013,11,1,2014_Q1]", "[2010,12,30,2012,1,1,2010_Q4]"))
  }
}
