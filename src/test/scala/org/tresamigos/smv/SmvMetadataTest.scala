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
      metadata.toJson === "{\"columns\":[{\"type\":\"String\",\"name\":\"a\"},{\"type\":\"Integer\",\"name\":\"b\"}]}")
  }

  test("SmvDataSet getMetadata includes dependencies") {
    val expectedSubstring =
      "\"inputs\":[\"" +
      modules1.Y.moduleMetaPath() + "\",\"" +
      modules2.Z.moduleMetaPath() + "\",\"" +
      modules3.W.publishMetaPath("1") + "\"]"
    val x = app.dsm.load(modules1.X.urn).head
    assert( x.getMetadata.toJson.contains(expectedSubstring) )
  }

  test("Persisted SmvDataSet's metadata include schema") {
    // Need to run the module first to persist its metadata (otherwise won't get the full metadata)
    app.runModule(modules1.V.urn)
    val expectedSubstring =
      "\"columns\":[{\"type\":\"String\",\"name\":\"a\"},{\"type\":\"Integer\",\"name\":\"b\"}]"
    val x = app.dsm.load(modules1.V.urn).head
    assert( x.getMetadata.toJson.contains(expectedSubstring) )
  }
}

package modules1 {
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
