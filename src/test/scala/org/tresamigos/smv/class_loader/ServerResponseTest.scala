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

package org.tresamigos.smv.class_loader

import java.io.ByteArrayOutputStream

import org.tresamigos.smv.SparkTestUtil

class ServerResponseTest extends SparkTestUtil {
  private val apiVersionBytes = Array[Byte](0, 0,0,ServerResponse.apiVersion.toByte)

  test("test OK Server Response Marshall") {
    val okResp = new ServerResponse(20, Array[Byte](1,2,3))

    val baos = new ByteArrayOutputStream()
    okResp.send(baos)

    val expectedRawBytes = apiVersionBytes ++ Array[Byte](
      0,0,0,ServerResponse.STATUS_OK.toByte,  // status
      0,0,0,0,0,0,0,20,                       // class version
      0,0,0,3,                                // num of bytes
      1,2,3                                   // class bytes
    )
    assert(baos.toByteArray === expectedRawBytes)
  }

  test("test OK Server Response De-Marshall") {
    val rawBytes = apiVersionBytes ++ Array[Byte](
      0,0,0,ServerResponse.STATUS_OK.toByte,  // status
      0,0,0,0,0,0,0,32,                       // class version
      0,0,0,4,                                // num of bytes
      1,2,3,4                                 // class bytes
    )
    val resp = ServerResponse(rawBytes)

    assert(resp.status === ServerResponse.STATUS_OK)
    assert(resp.version === 32L)
    assert(resp.bytes === Array[Byte](1,2,3,4))
  }

  test("test ERROR Server Response Marshall") {
    val errResp = new ServerResponse(ServerResponse.STATUS_ERR_CLASS_NOT_FOUND)

    val baos = new ByteArrayOutputStream()
    errResp.send(baos)

    val expectedRawBytes = apiVersionBytes ++ Array[Byte](
      0,0,0,ServerResponse.STATUS_ERR_CLASS_NOT_FOUND.toByte
    )
    assert(baos.toByteArray === expectedRawBytes)

  }

  test("test ERROR server Response De-Marshall") {
    val rawBytes = apiVersionBytes ++ Array[Byte](
      0,0,0,ServerResponse.STATUS_ERR_CLASS_NOT_FOUND.toByte)
    val resp = ServerResponse(rawBytes)

    assert(resp.status === ServerResponse.STATUS_ERR_CLASS_NOT_FOUND)
    assert(resp.version === 0L)
    assert(resp.bytes === null)
  }
}
