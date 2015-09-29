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

package org.tresamigos.smv.edd

import org.tresamigos.smv._
import org.apache.spark.sql.Column

private[smv] abstract class HistColumn

/**
 * Define histogram parameters for specified the column
 *
 * @param colName column name as a String
 * @param binSize bin size for numeric column, default 100.0
 * @param sortByFreq histogram result sort be frequency or not, default false (sort by key)
 **/
case class Hist(colName: String, binSize: Double = 100.0, sortByFreq: Boolean = false) extends HistColumn

/**
 * Specify an '''Amount''' binned histogram
 * Pre-defined bins for amount-like column to best report on log-normal distributed amount fields
 *
 * Bins:
 *  - "<0.0" => bin by 1000
 *  - "0.0" => keep 0.0
 *  - (0.0, 10.0) => 0.01
 *  - [10.0, 200.0) => bin by 10, floor
 *  - [200.0, 1000.0) => bin by 50, floor
 *  - [1000.0, 10000.0) => bin by 500, floor
 *  - [10000.0, 1000000.0) => bin by 5000, floor
 *  - [1000000.0, Inf) => bin by 1000000, floor
 **/
case class AmtHist(colName: String) extends HistColumn
