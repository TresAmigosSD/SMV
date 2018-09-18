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

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructField}

private[smv] class SmvKeys {
  val SmvLabel = "smvLabel"
  val SmvDesc  = "smvDesc"

  def getMetaDesc(m: Metadata): String = {
    if (m.contains(SmvDesc)) m.getString(SmvDesc) else ""
  }

  def getMetaLabels(m: Metadata): Seq[String] = {
    if (m.contains(SmvLabel)) m.getStringArray(SmvLabel).toSeq else Seq.empty
  }

  def createMetaWithDesc(desc: String): Metadata = {
    val builder = new MetadataBuilder()
    builder.putString(SmvDesc, desc).build
  }

  def addDescToMeta(m: Metadata, desc: String): Metadata = {
    val builder = new MetadataBuilder().withMetadata(m)
    builder.putString(SmvDesc, desc).build
  }

  def addLabelsToMeta(m: Metadata, labels: Seq[String]): Metadata = {
    val builder = new MetadataBuilder().withMetadata(m)
    // preserve the current meta data
    builder.putStringArray(SmvLabel, (getMetaLabels(m) ++ labels).distinct.toArray).build
  }

  def removeLabelsFromMeta(m: Metadata, labels: Seq[String]): Metadata = {
    val newLabels = if (labels.isEmpty) Seq.empty else (getMetaLabels(m) diff labels).distinct
    val builder   = new MetadataBuilder().withMetadata(m)
    builder.putStringArray(SmvLabel, newLabels.toArray).build
  }

  def removeDescFromMeta(m: Metadata): Metadata = {
    val builder = new MetadataBuilder().withMetadata(m)
    builder.putString(SmvDesc, "").build
  }
}

private[smv] class SchemaMetaOps(df: DataFrame) extends SmvKeys {

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
