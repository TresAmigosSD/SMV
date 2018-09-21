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
import org.tresamigos.smv.SmvKeys._

import org.json4s._
import org.json4s.jackson.JsonMethods.{parse}

private[smv] class SchemaMetaOps(df: DataFrame) {

  /**
   * Adds labels to the specified columns.
   *
   * A column may have multiple labels.  Adding the same label twice
   * to a column has the same effect as adding that label once.
   *
   * For multiple colNames, the same set of labels will be added to all of them.
   * When colNames is empty, the set of labels will be added to all columns of the df.
   * labels parameters must be non-empty.
   */
  def addLabel(colNames: Seq[String], labels: Seq[String]): DataFrame = {
    require(!labels.isEmpty)
    val allCol = colNames.isEmpty

    val columns = df.schema.fields map { f =>
      val c = f.name

      // if new label should be added to this column. Add to all columns, if colNames is empty
      if (allCol || colNames.contains(c)) {
        val meta = addLabelsToMeta(f.metadata, labels)
        df(c).as(c, meta)
      } else df(f.name)
    }

    df.select(columns: _*)
  }

  def addDesc(colDescs: Seq[(String, String)]): DataFrame = {
    require(!colDescs.isEmpty)
    val colMap = colDescs.toMap

    val columns = df.schema.fields map { f =>
      val c = f.name
      if (colMap.contains(c)) {
        val meta = addDescToMeta(f.metadata, colMap.getOrElse(c, ""))
        df(c).as(c, meta)
      } else df(c)
    }

    df.select(columns: _*)
  }

  def setColMeta(colMeta: Seq[(String, String)]): DataFrame = {
    require(!colMeta.isEmpty)
    val colMap = colMeta.toMap
    println(colMap)

    val columns = df.schema.fields map { f =>
      val c = f.name
      if (colMap.contains(c)) {
        df(c).as(c, Metadata.fromJson(colMap.getOrElse(c, "")))
      } else df(c)
    }

    df.select(columns: _*)
  }

  def getLabel(colName: String): Seq[String] = {
    getMetaLabels(df.schema.apply(colName).metadata)
  }

  def getDesc(colName: String): String = {
    getMetaDesc(df.schema.apply(colName).metadata)
  }

  def removeLabel(colNames: Seq[String], labels: Seq[String]): DataFrame = {
    val allCol    = colNames.isEmpty

    val columns = df.schema.fields map { f =>
      val c = f.name
      if (allCol || colNames.contains(c)) {
        val meta = removeLabelsFromMeta(f.metadata, labels)
        df(c) as (c, meta)
      } else df(c)
    }
    df.select(columns: _*)
  }

  def removeDesc(colNames: Seq[String]): DataFrame = {
    val allCol = colNames.isEmpty

    val columns = df.schema.fields map { f =>
      val c = f.name
      if (allCol || colNames.contains(c)) {
        val meta    = removeDescFromMeta(f.metadata)
        df(c) as (c, meta)
      } else df(c)
    }
    df.select(columns: _*)
  }

  def colWithLabel(labels: Seq[String]): Seq[String] = {
    val filterFn: Metadata => Boolean = { meta =>
      if (labels.isEmpty)
        !meta.contains(SmvLabel) || meta.getStringArray(SmvLabel).isEmpty
      else
        meta.contains(SmvLabel) && labels.toSet.subsetOf(meta.getStringArray(SmvLabel).toSet)
    }

    val ret = for {
      f <- df.schema.fields if (filterFn(f.metadata))
    } yield f.name

    require(
      !ret.isEmpty,
      if (labels.isEmpty)
        s"""there are no unlabeled columns in the data frame [${df.columns.mkString(",")}]"""
      else
        s"""there are no columns labeled with ${labels} in the data frame [${df.columns.mkString(
          ",")}]"""
    )

    ret
  }
}
