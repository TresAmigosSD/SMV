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

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

import scala.language.implicitConversions

/**
 * Provides Extended Data Dictionary functions for ad hoc data analysis
 *
 * {{{
 * scala> val res1 = df.edd.summary($"amt", $"time")
 * scala> res1.eddShow
 *
 * scala> val res2 = df.edd.histogram(AmtHist($"amt"), $"county", Hist($"pop", binSize=1000))
 * scala> res2.eddShow
 * scala> res2.saveReport("file/path")
 * }}}
 *
 * Depends on the data types of the columns, Edd summary method will perform different
 * statistics.
 *
 * The `histogram` method takes a group of `HistColumn` as parameters.
 * Or when a group of `String` as the column names are given, it will use the default `HistColumn`
 * parameters.
 * Two types of `HistColumn`s are supported
 *  - [[org.tresamigos.smv.edd.Hist]]
 *  - [[org.tresamigos.smv.edd.AmtHist]]
 *
 * The `eddShow` method will print report to the console, `saveReport` will save report as `RDD[String]`,
 * The strings are JSON strings.
 **/
package object edd {
  implicit def makeEddDFCvrt(erf: EddResultFunctions) = erf.toDF
}
