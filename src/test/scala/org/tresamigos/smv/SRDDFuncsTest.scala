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

import org.apache.spark.sql._, types._

class SelectWithReplaceTest extends SparkTestUtil {
  val fields = Seq("name:String", "friends:Integer")
  val schema = fields.mkString(";")
  val data = Seq("Adam,1", "Beth,2", "Caleb,3", "David,4")

  def testDf(sqlContext: SQLContext): DataFrame =
    createSchemaRdd(schema, data.mkString(";"))

  sparkTest("should add new columns without modification") {
    val input = testDf(sqlContext)
    val res = input.selectWithReplace(input("friends") + 1 as "newfriends")
    assertSrddSchemaEqual(res, schema + ";newfriends:Integer")
    assertSrddDataEqual(res,
      "Adam,1,2;Beth,2,3;Caleb,3,4;David,4,5")
  }

  sparkTest("should overwrite existing column with the same name") {
    val input = testDf(sqlContext)
    val res = input.selectWithReplace(input("friends") + 1 as "friends")
    assertSrddSchemaEqual(res, "name:String;friends:Integer")
    assertSrddDataEqual(res,
      "Adam,2;Beth,3;Caleb,4;David,5")
  }

  sparkTest("should accept a column aliased multiple times") {
    val input = testDf(sqlContext)
    val res = input.selectWithReplace(input("friends") as "friends" as "friends")
    assertSrddSchemaEqual(res, schema)
    assertSrddDataEqual(res, data.mkString(";"))
  }
}

class SelectPlusMinusTest extends SparkTestUtil {
  sparkTest("test SelectPlus") {
    val ssc = sqlContext; import ssc.implicits._
    val df = open(testDataDir +  "EddTest/test1.csv")
    val res = df.selectPlus('b + 2.0 as 'bplus2)
    assertSrddDataEqual(res,
      "1.0,10.0,12.0;" +
      "2.0,20.0,22.0;" +
      "3.0,30.0,32.0")
  }

  sparkTest("test SelectPlusPrefix") {
    val ssc = sqlContext; import ssc.implicits._
    val df = open(testDataDir +  "EddTest/test1.csv")
    val res = df.selectPlusPrefix('b + 2.0 as 'bplus2)
    assertSrddDataEqual(res,
      "12.0,1.0,10.0;" +
      "22.0,2.0,20.0;" +
      "32.0,3.0,30.0")
  }

  sparkTest("test SelectMinus") {
    val ssc = sqlContext; import ssc.implicits._
    val df = open(testDataDir +  "EddTest/test1.csv")
    val res = df.selectMinus('b)
    assertSrddDataEqual(res,
      "1.0;" +
      "2.0;" +
      "3.0")
  }
}

class renameFieldTest extends SparkTestUtil {
  sparkTest("test rename fields") {
    val df = createSchemaRdd("a:Integer; b:Double; c:String",
      "1,2.0,hello")

    val result = df.renameField('a -> 'aa, 'c -> 'cc)

    val fieldNames = result.schema.fieldNames
    assert(fieldNames === Seq("aa", "b", "cc"))
    assert(result.collect.map(_.toString) === Seq("[1,2.0,hello]") )
  }

  // TODO: what are these tests commented out?  Are they still valid?
  /*
  sparkTest("test prefixing field names") {
    val df = createSchemaRdd("a:Integer; b:Double; c:String",
      "1,2.0,hello")

    val result = df.prefixFieldNames("xx_")

    val fieldNames = result.schema.fieldNames
    assert(fieldNames === Seq("xx_a", "xx_b", "xx_c"))
    assert(result.collect.map(_.toString) === Seq("[1,2.0,hello]") )
  }

  sparkTest("test postfixing field names") {
    val df = createSchemaRdd("a:Integer; b:Double; c:String",
      "1,2.0,hello")

    val result = df.postfixFieldNames("_xx")

    val fieldNames = result.schema.fieldNames
    assert(fieldNames === Seq("a_xx", "b_xx", "c_xx"))
    assert(result.collect.map(_.toString) === Seq("[1,2.0,hello]") )
  }
  */
}

class JoinHelperTest extends SparkTestUtil {
  sparkTest("test joinUniqFieldNames") {
    val ssc = sqlContext; import ssc.implicits._
    val srdd1 = createSchemaRdd("a:Integer; b:Double; c:String",
      """1,2.0,hello;
         1,3.0,hello;
         2,10.0,hello2;
         2,11.0,hello3"""
    )

    val srdd2 = createSchemaRdd("a2:Integer; c:String",
      """1,asdf;
         2,asdfg"""
    )

    val result = srdd1.joinUniqFieldNames(srdd2, $"a" === $"a2", "inner")
    val fieldNames = result.columns
    assert(fieldNames === Seq("a", "b", "c", "a2", "_c"))
    assertUnorderedSeqEqual(result.collect.map(_.toString), Seq(
    "[1,2.0,hello,1,asdf]",
    "[1,3.0,hello,1,asdf]",
    "[2,10.0,hello2,2,asdfg]",
    "[2,11.0,hello3,2,asdfg]"))
  }

  sparkTest("test joinByKey") {
    val ssc = sqlContext; import ssc.implicits._
    val srdd1 = createSchemaRdd("a:Integer; b:Double; c:String",
      """1,2.0,hello;
         1,3.0,hello;
         2,10.0,hello2;
         2,11.0,hello3"""
    )

    val srdd2 = createSchemaRdd("a:Integer; c:String",
      """1,asdf;
         2,asdfg"""
    )

    val result = srdd1.joinByKey(srdd2, Seq("a"), "inner")
    val fieldNames = result.columns
    assert(fieldNames === Seq("a", "b", "c", "_c"))
    assertUnorderedSeqEqual(result.collect.map(_.toString), Seq(
    "[1,2.0,hello,asdf]",
    "[1,3.0,hello,asdf]",
    "[2,10.0,hello2,asdfg]",
    "[2,11.0,hello3,asdfg]"))
  }
}

class dedupByKeyTest extends SparkTestUtil {
  sparkTest("test dedupByKey") {
    val df = createSchemaRdd("a:Integer; b:Double; c:String",
      """1,2.0,hello;
         1,3.0,hello;
         2,10.0,hello2;
         2,11.0,hello3"""
    )

    val result1 = df.dedupByKey('a)
    assertUnorderedSeqEqual(result1.collect.map(_.toString), Seq(
      "[1,2.0,hello]",
      "[2,10.0,hello2]" ))

    val fieldNames1 = result1.schema.fieldNames
    assert(fieldNames1 === Seq("a", "b", "c"))

    val result2 = df.dedupByKey('a, 'c)
    assertUnorderedSeqEqual(result2.collect.map(_.toString), Seq(
    "[1,2.0,hello]",
    "[2,10.0,hello2]",
    "[2,11.0,hello3]" ))

    val fieldNames2 = result2.schema.fieldNames
    assert(fieldNames2 === Seq("a", "b", "c"))

  }
}

class smvOverlapCheckTest extends SparkTestUtil {
  sparkTest("test smvOverlapCheck") {
    val s1 = createSchemaRdd("k: String", "a;b;c")
    val s2 = createSchemaRdd("k: String", "a;b;c;d")
    val s3 = createSchemaRdd("k: String", "c;d")

    val res = s1.smvOverlapCheck("k")(s2, s3)
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a,110]",
      "[b,110]",
      "[c,111]",
      "[d,011]"))

  }
}

class smvHashSampleTest extends SparkTestUtil {
  sparkTest("test smvHashSample") {
    val ssc = sqlContext; import ssc.implicits._
    val a = createSchemaRdd("key:String", "a;b;c;d;e;f;g;h;i;j;k")
    val res = a.unionAll(a).smvHashSample($"key", 0.3)
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a]",
      "[g]",
      "[i]",
      "[a]",
      "[g]",
      "[i]"))
  }
}

class smvCoalesceTest extends SparkTestUtil {
  sparkTest("Test smvCoalesce") {
    val ssc = sqlContext; import ssc.implicits._
    val a = createSchemaRdd("key:String", "a;b;c;d;e;f;g;h;i;j;k")
    val res = a.smvCoalesce(1)
    assertUnorderedSeqEqual(res.collect.map(_.toString), Seq(
      "[a]",
      "[b]",
      "[c]",
      "[d]",
      "[e]",
      "[f]",
      "[g]",
      "[h]",
      "[i]",
      "[j]",
      "[k]"))
    assert(res.rdd.partitions.size === 1)
  }
}


class smvPipeCount extends SparkTestUtil {
  sparkTest("Test smvPipeCount") {
    val ssc = sqlContext; import ssc.implicits._
    val a = createSchemaRdd("key:String", "a;b;c;d;e;f;g;h;i;j;k")
    val counter = sc.accumulator(0l)

    val n1 = a.smvPipeCount(counter).count
    val n2 = counter.value

    assert(n1 === n2)
  }
}
