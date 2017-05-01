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
  test("SmvMetaData extracts schema from DataFrames") {
    val metadata = new SmvMetadata
    val df = app.createDF("a:String;b:Integer", "x,10")
    metadata.addSchemaMetadata(df)
    // Note that type comes before name due to implementation details; the order
    // of dictionary pairs is arbitrary (under the hood they are in an unordered map)
    assert(metadata.toString === "{\"columns\":[{\"type\":\"String\",\"name\":\"a\"},{\"type\":\"Integer\",\"name\":\"b\"}]}")
  }

  test("Persisted SmvDataSet's metadata include schema") {
    // Need to run the module first to persist its metadata (otherwise won't get the full metadata)
    app.runModule(modules.X.urn)
    val expected = "{\"fqn\":\"org.tresamigos.smv.modules.X\",\"columns\":[{\"type\":\"String\",\"name\":\"a\"},{\"type\":\"Integer\",\"name\":\"b\"}]}"
    assert(modules.X.getMetadata.toString === expected)
  }
}

package modules {
  object X extends SmvModule("") {
    def requiresDS = Seq()
    def run(i: runParams) = {
      app.createDF("a:String;b:Integer")
    }
  }
}
