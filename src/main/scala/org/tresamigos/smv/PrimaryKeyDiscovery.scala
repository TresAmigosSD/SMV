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

import org.apache.spark.sql.DataFrame
import smvfuncs._

private[smv] class PrimaryKeyDiscovery(val debug: Boolean) {

  private def debugMessage(s: String) = if (debug) println(s)

  /**
   * Find PKs one by one through recursive call itself.
   *
   * @param df DataFrame to discover on
   * @param pool candidate pool of column names
   * @param preKeys key columns already identified from previous iteration
   * @param preKeyCnt unique-count on "preKeys"
   *
   * @return (newKeys, newKeyCnt) new set of keys and the unique-count of them
   **/
  private def oneByOnePK(df: DataFrame, pool: Seq[String], preKeys: Seq[String], preKeyCnt: Long):
    (Seq[String], Long)= {

    if (!preKeys.isEmpty) debugMessage(s"current keys: ${preKeys};    unique-count: $preKeyCnt")
    /* adding each element from "pool" to "preKeys" one by one and unique-count */
    val exprs = pool.map{p => smvCountDistinctWithNull(p, preKeys:_*) as p}
    val res = df.agg(exprs.head, exprs.tail:_*)
    val resMap = res.columns.zip(res.collect.head.toSeq.map{n => n.asInstanceOf[Long]})

    /* If adding a candidate does not increase unique-count, the candidate is rejected.*/
    val filteredKeys = resMap.filter{case (k, v)=> v > preKeyCnt}
    if (preKeyCnt > 0) debugMessage("Rejected: " + resMap.filter{case (k, v) => v == preKeyCnt}.map{_._1}.mkString(",") + "\n")

    if (filteredKeys.isEmpty) (preKeys, preKeyCnt)
    else {
      /* For the candidate left, add the one with largest contribution to the unique-count*/
      val newKeyNCnt = filteredKeys.sortWith(_._2 > _._2).head
      val newKeys = preKeys :+ newKeyNCnt._1
      val newCnt = newKeyNCnt._2

      /* Keep left candidates and send to next round iteration */
      val newPool = filteredKeys.diff(Seq(newKeyNCnt)).map{_._1}
      if (newPool.isEmpty) (newKeys, newCnt)
      else oneByOnePK(df, newPool, newKeys, newCnt)
    }
  }

  def discoverPK(df: DataFrame, n: Integer) = {
    val workingSet = df.limit(n).cache

    val pool = workingSet.columns
    val res = oneByOnePK(workingSet, pool, Nil, 0l)

    workingSet.unpersist
    res
  }
}
