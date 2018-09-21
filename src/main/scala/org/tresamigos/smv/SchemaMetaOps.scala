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
import org.apache.spark.sql.types.Metadata

private[smv] class SchemaMetaOps(df: DataFrame) {
  /**
   * Set the metadata of the specified columns.
   *
   * @param colMeta a list of (colName, meta) tuples, where colName is the column name for which
   *                to set the metadata. And meta is a json string of the metadata.
   *
   * This helper exists since we can't select columns with meta in pyspark 2.1 or lower versions.
   *
   * TODO: once we decide to completely move to pyspark 2.2 or higher, remove this function and
   *       re-implement it in python side, using df.select(col("a").alias("a", metadata = {...}))
   */
  def setColMeta(colMeta: Seq[(String, String)]): DataFrame = {
    require(!colMeta.isEmpty)
    val colMap = colMeta.toMap

    val columns = df.schema.fields map { f =>
      val c = f.name
      if (colMap.contains(c)) {
        df(c).as(c, Metadata.fromJson(colMap.getOrElse(c, "")))
      } else df(c)
    }

    df.select(columns: _*)
  }
}
