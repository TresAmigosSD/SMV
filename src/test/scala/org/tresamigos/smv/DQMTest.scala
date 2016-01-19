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
import org.tresamigos.smv.dqm._
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

class DQMTest extends SmvTestUtil {
  test("test DQMState functions") {
    val state = new DQMState(sc, Seq("rule1", "rule2"), Seq("fix1"))

    (0 to 5).foreach{i =>
      state.addRec()
      state.addRuleRec("rule1", s"record: $i")
    }

    state.addRec()
    state.addRuleRec("rule2", "rule2 record")

    (0 to 2).foreach{i =>
      state.addRec()
      state.addFixRec("fix1")
    }

    state.addRec()

    intercept[IllegalArgumentException] {
      println(state.getRecCount())
    }

    state.snapshot()
    assert(state.getRecCount() === 11)
    assert(state.getRuleCount("rule1") === 6)
    assert(state.getTotalRuleCount() === 7)
    assert(state.getTotalFixCount() === 3)
    assertUnorderedSeqEqual(state.getRuleLog("rule2"), Seq(
      "org.tresamigos.smv.dqm.DQMRuleError: rule2 @FIELDS: rule2 record"))
  }

  test("test DQMRule") {
    val df = createSchemaRdd("a:Integer;b:Double", "1,0.3;0,0.2")
    val state = new DQMState(sc, Seq("rule1"), Nil)
    val dqmRule1 = DQMRule((new Column("a")) + (new Column("b")) > 0.3, "rule1")
    val (c1, c2, c3) = dqmRule1.createCheckCol(state)
    val res = df.selectPlus(c1).where(c2).selectMinus(c3)

    assertSrddDataEqual(res, "1,0.3")
    state.snapshot()

    assertUnorderedSeqEqual(state.getRuleLog("rule1"), Seq(
      "org.tresamigos.smv.dqm.DQMRuleError: rule1 @FIELDS: a=0,b=0.2"
    ))
  }

  test("test DQMFix") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("a:Integer;b:Double", "1,0.3;0,0.2")
    val state = new DQMState(sc, Nil, Seq("fix1"))
    val dqmFix = DQMFix($"a" > 0, lit(0) as "a", "fix1")
    val c = dqmFix.createFixCol(state)
    val res = df.selectWithReplace(c)

    assertSrddDataEqual(res, "0,0.3;0,0.2")
  }

  test("test SmvDQM with FailAny (so FailCount)") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("a:Integer;b:Double", "1,0.3;0,0.2")

    val dqm = new DQMValidator(SmvDQM().add(DQMRule($"a" <= 0, "a_le_0", FailAny)))

    val res = dqm.attachTasks(df)
    assert(res.count === 1)

    val dqmRes = dqm.validate(res)
    assert(dqmRes.toJSON() === """{
  "passed":false,
  "errorMessages": [
    {"a_le_0":"false"}
  ],
  "checkLog": [
    "Rule: a_le_0, total count: 1",
    "org.tresamigos.smv.dqm.DQMRuleError: a_le_0 @FIELDS: a=1"
  ]
}""")
  }

  test("test FailPercent") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("a:Integer;b:Double", "1,0.3;0,0.2;3,0.5")

    val dqm = new DQMValidator(SmvDQM().
      add(DQMRule($"b" < 0.4 , "b_lt_03", FailPercent(0.5))).
      add(DQMFix($"a" < 1, lit(1) as "a", "a_lt_1_fix", FailPercent(0.3))))

    val res = dqm.attachTasks(df)
    assertSrddDataEqual(res, "1,0.3;1,0.2")

    val dqmRes = dqm.validate(res)
    assert(dqmRes.toJSON() === """{
  "passed":false,
  "errorMessages": [
    {"b_lt_03":"true"},
    {"a_lt_1_fix":"false"}
  ],
  "checkLog": [
    "Rule: b_lt_03, total count: 1",
    "org.tresamigos.smv.dqm.DQMRuleError: b_lt_03 @FIELDS: b=0.5",
    "Fix: a_lt_1_fix, total count: 1"
  ]
}""")
  }

  test("test Total Policies") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("a:Integer;b:Double", "1,0.3;0,0.2;3,0.5")

    val dqm = new DQMValidator(SmvDQM().
      add(DQMRule($"b" < 0.4 , "b_lt_03")).
      add(DQMFix($"a" < 1, lit(1) as "a", "a_lt_1_fix")).
      add(FailTotalRuleCountPolicy(2)).
      add(FailTotalFixCountPolicy(1)).
      add(FailTotalRulePercentPolicy(0.3)).
      add(FailTotalFixPercentPolicy(0.3)))

    val res = dqm.attachTasks(df)

    /** Action count will be executed with optimization which will not trigger the fixes */
    //res.foreach(r => Unit)
    res.rdd.count

    val dqmRes = dqm.validate(res)
    assertUnorderedSeqEqual(dqmRes.errorMessages, Seq(
      ("FailTotalRuleCountPolicy(2)","true"),
      ("FailTotalFixCountPolicy(1)","false"),
      ("FailTotalRulePercentPolicy(0.3)","false"),
      ("FailTotalFixPercentPolicy(0.3)","false")
    ))
  }

  test("test dqm method in SmvDataSet") {
    val ssc = sqlContext; import ssc.implicits._
    object file extends SmvCsvStringData("a:Integer;b:Double", "1,0.3;0,0.2;3,0.5") {
      override def dqm() = SmvDQM().
        add(DQMRule($"b" < 0.4 , "b_lt_03")).
        add(DQMFix($"a" < 1, lit(1) as "a", "a_lt_1_fix")).
        add(FailTotalRuleCountPolicy(2)).
        add(FailTotalFixCountPolicy(1))
    }
    intercept[ValidationError] {
      file.rdd.show
    }
  }

  test("test additional DQMRules") {
    val ssc = sqlContext; import ssc.implicits._
    object file extends SmvCsvStringData("a:Integer;b:String;c:String", "1,m,a;0,f,c;2,m,z;1,o,x;1,m,zz") {
      override def dqm() = SmvDQM().
        add(BoundRule($"a", 0, 2)).
        add(SetRule($"b", Set("m", "f"))).
        add(FormatRule($"c", ".")).
        add(FailTotalRuleCountPolicy(3))
    }
    intercept[ValidationError] {
      file.rdd.show
    }
  }

  test("test additional DQMFixes") {
    val ssc = sqlContext; import ssc.implicits._
    object file extends SmvCsvStringData("a:Integer;b:String;c:String", "1,m,a;0,f,c;2,m,z;1,x,x;1,m,zz") {
      override def dqm() = SmvDQM().
        add(SetFix($"b", Set("m", "f", "o"), "o")).
        add(FormatFix($"c", ".", "_")).
        add(FailTotalFixCountPolicy(5))
    }
    assertSrddDataEqual(file.rdd, "1,m,a;0,f,c;2,m,z;1,o,x;1,m,_")
  }

  test("test user defined policy") {
    val ssc = sqlContext; import ssc.implicits._
    object file extends SmvCsvStringData("a:Integer;b:Double", "1,0.3;0,0.2;3,0.5") {
      val policy: (DataFrame, DQMState) => Boolean = {(df, state) =>
        state.getRuleCount("rule1") + state.getFixCount("fix2") == 3
      }
      override def dqm() = SmvDQM().
        add(DQMRule($"b" < 0.4 , "rule1")).
        add(DQMFix($"a" < 1, lit(1) as "a", "fix2")).
        add(DQMPolicy(policy, "udp"))
    }

    intercept[ValidationError] {
      file.rdd
    }
  }
}
