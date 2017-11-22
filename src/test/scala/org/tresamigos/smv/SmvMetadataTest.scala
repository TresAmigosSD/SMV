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

class SmvMetadataTest extends SmvTestUtil {
  override def appArgs = super.appArgs ++ Seq(
    "--smv-props",
    "smv.stages=org.tresamigos.smv.modules1:org.tresamigos.smv.modules2:org.tresamigos.smv.modules3," +
    "smv.stages.modules3.version=1"
  )

  test("SmvMetadata extracts schema from DataFrames") {
    val metadata = new SmvMetadata
    val df       = app.createDF("a:String;b:Integer", "x,10")
    metadata.addSchemaMetadata(df)
    // Note that type comes before name due to implementation details; the order
    // of dictionary pairs is arbitrary (under the hood they are in an unordered map)
    assert(
      metadata.toJson === "{\"_columns\":[{\"type\":\"String\",\"name\":\"a\"},{\"type\":\"Integer\",\"name\":\"b\"}]}")
  }

  test("SmvDataSet getMetadata includes dependencies") {
    val expectedSubstring =
      "\"_inputs\":[\"" +
      modules1.Y.moduleMetaPath() + "\",\"" +
      modules2.Z.moduleMetaPath() + "\",\"" +
      modules3.W.publishMetaPath("1") + "\"]"
    val x = app.dsm.load(modules1.X.urn).head
    assert( x.getMetadata.toJson.contains(expectedSubstring) )
  }

  test("Persisted SmvDataSet's metadata include schema") {
    app.runModule(modules1.V.urn)
    // Need to run the module first to persist its metadata (otherwise won't get the full metadata)
    val expectedSubstring =
      "\"_columns\":[{\"type\":\"String\",\"name\":\"a\"},{\"type\":\"Integer\",\"name\":\"b\"}]"
    val x = app.dsm.load(modules1.V.urn).head
    assert( x.getMetadata.toJson.contains(expectedSubstring) )
  }

  test("Persisted SmvDataSet's metadata include timestamp") {
    app.runModule(modules1.V.urn)
    // Need to run the module first to persist its metadata (otherwise won't get the full metadata)
    val expectedPattern = "\"_timestamp\":\".+\"".r
    val x = app.dsm.load(modules1.V.urn).head
    assert( expectedPattern.findAllIn(x.getMetadata.toJson).hasNext )
  }

  test("SmvDataSet's metadata includes custom metadata") {
    app.runModule(modules1.A.urn)
    val expectedPattern = "\"foo\":\"bar\"".r
    val a = app.dsm.load(modules1.A.urn).head
    assert( expectedPattern.findAllIn(a.getMetadata.toJson).hasNext )
  }

  test("SmvDataSet's metadata history is logged up to history size") {
    for (i <- 1 to modules1.B.metadataHistorySize + 1){
      app.runModule(modules1.B.urn, true)
    }

    val b = app.dsm.load(modules1.B.urn).head
    val curSparkMeta = b.getMetadata.builder.build
    val histMeta = b.getMetadataHistory
    val histSparkMeta = histMeta(0).builder.build

    assert(histMeta.length === modules1.B.metadataHistorySize)
    assert(curSparkMeta.getString("foo") == histSparkMeta.getString("foo"))
  }

  test("metadata validation fails iff user-defined validation fails") {
    val succeeds = modules1.MetadataValidationSucceeds
    // Validation should succeed because history is empty
    app.runModule(succeeds.urn)
    // Validation should succeed because metadata matches
    app.runModule(succeeds.urn, true)

    val fails = modules1.MetadataValidationFails
    // Validation should succeed because history is empty
    app.runModule(fails.urn)
    // Validation should fail because metadata doesn't match
    val dqmError = intercept[SmvDqmValidationError] {
      app.runModule(fails.urn, true)
    }

    val expectedPattern = fails.validationFailureMessage.r

    assert(expectedPattern.findAllIn(dqmError.getMessage).hasNext)
  }
}

package modules1 {
  object A extends SmvModule("") {
    def requiresDS = Seq(Y)
    def run(i: runParams) = {
      i(Y)
    }
    override def metadata(df: DataFrame) = SmvMetadata.fromJson("{\"foo\": \"bar\"}")
  }

  object B extends SmvModule("") {
    def requiresDS =
      Seq(Y)
    def run(i: runParams) = {
      i(Y)
    }
    override def metadata(df: DataFrame) =
      SmvMetadata.fromJson("{\"foo\": \"bar\"}")
    override def metadataHistorySize() =
      2
  }

  object MetadataValidationSucceeds extends SmvModule("") {
    def requiresDS =
      Seq(Y)
    def run(i: runParams) = {
      i(Y)
    }
    override def metadata(df: DataFrame) =
      SmvMetadata.fromJson("{\"foo\": \"bar\"}")
    override def validateMetadata(current: SmvMetadata, history: Seq[SmvMetadata]) = {
      if (history.isEmpty || current.builder.build.getString("foo") == history.head.builder.build.getString("foo"))
        None
      else
        Some(validationFailureMessage)
    }
    def validationFailureMessage = "FOOS DON'T MATCH"
    override def metadataHistorySize() =
      1
  }

  object MetadataValidationFails extends SmvModule("") {
    def requiresDS =
      Seq(Y)
    def run(i: runParams) = {
      i(Y)
    }
    override def validateMetadata(current: SmvMetadata, history: Seq[SmvMetadata]) = {
      if (history.isEmpty || current.builder.build.getString("_timestamp") == history.head.builder.build.getString("_timestamp"))
        None
      else
        Some(validationFailureMessage)
    }
    def validationFailureMessage = "TIMESTAMPS DON'T MATCH"
    override def metadataHistorySize() =
      1
  }

  object X extends SmvModule("") {
    def requiresDS = Seq(Y, ZL, WL)
    def run(i: runParams) = {
      i(Y)
    }
  }

  object V extends SmvModule("") {
    def requiresDS = Seq(Y)
    def run(i: runParams) = {
      i(Y)
    }
  }

  object Y extends SmvModule("") {
    def requiresDS = Seq()
    def run(i: runParams) = {
      app.createDF("a:String;b:Integer")
    }
  }

  object ZL extends SmvModuleLink(modules2.Z)
  object WL extends SmvModuleLink(modules3.W)
}

// unpublished
package modules2 {
  object Z extends SmvModule("") with SmvOutput {
    def requiresDS = Seq()
    def run(i: runParams) = {
      app.createDF("a:String;b:Integer")
    }
  }
}

// published
package modules3 {
  object W extends SmvModule("") with SmvOutput {
    def requiresDS = Seq()
    def run(i: runParams) = {
      app.createDF("a:String;b:Integer")
    }
  }
}
