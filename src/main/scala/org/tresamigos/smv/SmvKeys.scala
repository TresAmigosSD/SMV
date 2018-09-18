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

import org.apache.spark.sql.types.{Metadata, MetadataBuilder, StructField}

/** Utility for using the metadata part of StructField to store Smv keys 
 */
private[smv] object SmvKeys {
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