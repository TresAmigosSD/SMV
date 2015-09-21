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
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

class DQMTest extends SparkTestUtil {
  sparkTest("test DQMState functions") {
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
      "org.tresamigos.smv.dqm.DQMRuleError: rule2 @RECORD: rule2 record"))
  }

  sparkTest("test DQMRule") {
    val df = createSchemaRdd("a:Integer;b:Double", "1,0.3;0,0.2")
    val state = new DQMState(sc, Seq("rule1"), Nil)
    val dqmRule1 = DQMRule((new Column("a")) + (new Column("b")) > 0.3, "rule1")
    val (c1, c2, c3) = dqmRule1.createCheckCol(state)
    val res = df.selectPlus(c1).where(c2).selectMinus(c3)

    assertSrddDataEqual(res, "1,0.3")
    state.snapshot()

    assertUnorderedSeqEqual(state.getRuleLog("rule1"), Seq(
      "org.tresamigos.smv.dqm.DQMRuleError: rule1 @RECORD: a=0,b=0.2"
    ))
  }

  sparkTest("test DQMFix") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("a:Integer;b:Double", "1,0.3;0,0.2")
    val state = new DQMState(sc, Nil, Seq("fix1"))
    val dqmFix = DQMFix($"a" > 0, lit(0) as "a", "fix1")
    val c = dqmFix.createFixCol(state)
    val res = df.selectWithReplace(c)

    assertSrddDataEqual(res, "0,0.3;0,0.2")
  }

  sparkTest("test SmvDQM with FailAny (so FailCount)") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("a:Integer;b:Double", "1,0.3;0,0.2")

    val dqm = SmvDQM().add(DQMRule($"a" <= 0, "a_le_0", FailAny))

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
    "org.tresamigos.smv.dqm.DQMRuleError: a_le_0 @RECORD: a=1"
  ]
}""")
  }

  sparkTest("test FailPercent") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("a:Integer;b:Double", "1,0.3;0,0.2;3,0.5")

    val dqm = SmvDQM().
      add(DQMRule($"b" < 0.4 , "b_lt_03", FailPercent(0.5))).
      add(DQMFix($"a" < 1, lit(1) as "a", "a_lt_1_fix", FailPercent(0.3)))

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
    "org.tresamigos.smv.dqm.DQMRuleError: b_lt_03 @RECORD: b=0.5",
    "Fix: a_lt_1_fix, total count: 1"
  ]
}""")
  }

  sparkTest("test Total Policies") {
    val ssc = sqlContext; import ssc.implicits._
    val df = createSchemaRdd("a:Integer;b:Double", "1,0.3;0,0.2;3,0.5")

    val dqm = SmvDQM().
      add(DQMRule($"b" < 0.4 , "b_lt_03")).
      add(DQMFix($"a" < 1, lit(1) as "a", "a_lt_1_fix")).
      add(FailTotalRuleCountPolicy(2)).
      add(FailTotalFixCountPolicy(1)).
      add(FailTotalRulePercentPolicy(0.3)).
      add(FailTotalFixPercentPolicy(0.3))

    val res = dqm.attachTasks(df)

    /** Action count will be executed with optimization which will not trigger the fixes */
    res.foreach(r => Unit)

    val dqmRes = dqm.validate(res)
    assertUnorderedSeqEqual(dqmRes.errorMessages, Seq(
      ("NoOpDQMPolicy","true"),
      ("NoOpDQMPolicy","true"),
      ("FailTotalRuleCountPolicy(2)","true"),
      ("FailTotalFixCountPolicy(1)","false"),
      ("FailTotalRulePercentPolicy(0.3)","false"),
      ("FailTotalFixPercentPolicy(0.3)","false")
    ))
  }

  sparkTest("test dqm method in SmvDataSet") {
    val ssc = sqlContext; import ssc.implicits._
    object file extends SmvCsvData("a:Integer;b:Double", "1,0.3;0,0.2;3,0.5") {
      override def dqm() = SmvDQM().
        add(DQMRule($"b" < 0.4 , "b_lt_03")).
        add(DQMFix($"a" < 1, lit(1) as "a", "a_lt_1_fix")).
        add(FailTotalRuleCountPolicy(2)).
        add(FailTotalFixCountPolicy(1))
    }
    file.injectApp(app)
    intercept[ValidationError] {
      file.rdd.show
    }
  }
}
