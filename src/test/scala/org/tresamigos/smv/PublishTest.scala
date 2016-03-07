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

package org.tresamigos.smv {

class PublishTest extends SmvTestUtil {
  override def appArgs = Seq(
    "--smv-props",
    "smv.stages=org.tresamigos.smv.publish.stage1",
    "smv.stages.stage1.version=v1",   // version when reading.
    "--publish", "v1",                // version when publishing.
    "--data-dir", testcaseTempDir,
    "--input-dir", testcaseTempDir,
    "--publish-dir", s"${testcaseTempDir}/publish",
    "-s", "stage1")

  test("Test module publish") {
    // the publish step below should generate csv/schema files in the publish directory.
    org.tresamigos.smv.publish.stage1.M1.publish()

    // Verify that the published file has same data/schema as the source module.
    val df = SmvCsvFile("publish/v1/org.tresamigos.smv.publish.stage1.M1.csv").rdd
    assertSrddSchemaEqual(df, "x:Integer")
    assertSrddDataEqual(df, "1;2;3")

    // Read published data using standard interface.
    val dfpub = org.tresamigos.smv.publish.stage1.M1.readPublishedData().get
    assertSrddSchemaEqual(dfpub, "x:Integer")
    assertSrddDataEqual(dfpub, "1;2;3")
  }
}
}

/** package for testing publish functionality */
package org.tresamigos.smv.publish.stage1 {

import org.tresamigos.smv.SmvModule

object M1 extends SmvModule("module M1") {
  override val isEphemeral = true
  override def requiresDS() = Seq.empty
  override def run(inputs: runParams) = {
    app.createDF("x:Integer", "1;2;3")
  }
}
}
