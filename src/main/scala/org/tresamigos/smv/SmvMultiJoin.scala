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
import org.apache.spark.sql.contrib.smv._
import org.apache.spark.sql.types._
import scala.util.Try


private[smv] case class SmvJoinDF(df: DataFrame, postfix: String, jType: String)

private[smv] case class SmvMultiJoinConf(leftDf: DataFrame, keys: Seq[String], defaultJoinType: String)

class SmvMultiJoin(val dfChain: Seq[SmvJoinDF], val conf: SmvMultiJoinConf) {

  /**
   * Append SmvMultiJoin Chain
   * {{{
   * df.smvJoinMultipleByKey(Seq("k1", "k2"), Inner).
   *    joinWith(df2, "_df2").
   *    joinWith(df3, "_df3", LeftOuter).
   *    doJoin()
   * }}}
   **/
  def joinWith(df: DataFrame, postfix: String, joinType: String = null) = {
    val dfJT = if(joinType == null) conf.defaultJoinType else joinType
    val newChain = dfChain :+ SmvJoinDF(df, postfix, dfJT)
    new SmvMultiJoin(newChain, conf)
  }

  /**
   * Really triger the join operation
   *
   * @param dropExtra default false, which will keep all duplicated name columns with the postfix.
   *                   when it's true, the duplicated columns will be dropped. 
   **/
  def doJoin(dropExtra: Boolean = false): DataFrame = {
    val res = dfChain.foldLeft(conf.leftDf){case (acc: DataFrame, SmvJoinDF(df, postfix, jType)) =>
      acc.joinByKey(df, conf.keys, jType, postfix, false)
    }

    val colsWithPostfix = dfChain.map{_.postfix}.flatMap{p =>
      res.columns.filter{c => c.endsWith(p)}
    }.distinct

    if (dropExtra && (! colsWithPostfix.isEmpty))
      res.selectMinus(colsWithPostfix.head, colsWithPostfix.tail: _*)
    else
      res
  }
}
