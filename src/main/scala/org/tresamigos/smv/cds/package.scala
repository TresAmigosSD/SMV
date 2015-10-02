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

/**
 * A Custom Data Selector (CDS) defines a sub-set of a group of records within a GroupedData,
 * and user can define aggregations on this sub-set of data.
 *
 * NOTE: since Spark 1.4 introduced the `window` concept, which has functional overlap with
 * cds, when migrating to Spark 1.5, SMV cds interface might be totally re-designed.
 *
 * As in the following data, for each transaction record, we want to calculate the sum of
 * the dollar spend on the passed 7 days.
 *
 * {{{
 * Id, time, amt
 *  1, 20140102, 100.0
 *  1, 20140201, 30.0
 *  1, 20140202, 10.0
 * }}}
 *
 * The expected output should be
 * {{{
 * Id, time, amt, runamt
 * 1, 20140102, 100.0, 100.0
 * 1, 20140201, 30.0, 30.0
 * 1, 20140202, 10.0, 40.0
 * }}}
 *
 * Here is how to calculate above running aggregate with CDS:
 * {{{
 *   val inLast7d = TimeInLastNDays("time", 7)
 *   df.smvGroupBy("Id").runAgg($"time")(
 *      $"Id", $"time", $"amt",
 *      sum($"amt") from inLast7d as "runamt"
 *   )
 * }}}
 *
 * In this exmple, `inLast7d` is a `CDS`. Please note that we extends the column syntax with
 * a `from` keyword, which is followed by a `CDS`.
 *
 * The `runAgg` and `oneAgg` methods in the [[org.tresamigos.smv.SmvGroupedDataFunc]] class is
 * the main entry to the CDS.
 *
 **/
package object cds {
}
