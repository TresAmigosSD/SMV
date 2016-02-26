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

import java.io._

/**
 * Encapsulate the marshall/de-marshall of server response in this class.
 *
 * Wire protocol:
 * <pre>
 * if status == STATUS_OK:
 *   0. api version : int (4 bytes)
 *   1. status : int
 *   2. version : long (8 bytes)
 *   3. num_of_bytes : int
 *   4. bytes : byte[num_of_bytes]
 *
 * if status != STATUS_OK
 *   0. api version : int
 *   1. status : int
 * </pre>
 */
private[smv]
class ServerResponse(
  val status: Int,            // one of STATUS_OK, STATUS_ERR_*
  val version: Long,     // the version of the class (timestamp?)
  val bytes: Array[Byte] // the actual class code bytes (can be null)
  ) {

  /** helper for creating a status ok response. */
  def this(classVersion: Long, classBytes: Array[Byte]) = {
    this(ServerResponse.STATUS_OK, classVersion, classBytes)
  }

  /** helper for creating a status error response. */
  def this(status: Int) = {
    this(status, 0L, null)
  }

  /**
   * Sends the current server response as an encoded message to given output stream.
   */
  def send(output: OutputStream) = {
    val classBytesLength = if (bytes == null) 0 else bytes.length
    val baos = new ByteArrayOutputStream(classBytesLength + 100)
    val dos = new DataOutputStream(baos)
    dos.writeInt(ServerResponse.apiVersion)
    dos.writeInt(status)
    if (status == ServerResponse.STATUS_OK) {
      dos.writeLong(version)
      dos.writeInt(classBytesLength)
      dos.write(bytes)
    }
    dos.close()

    // TODO: add compression to the byte stream as well (using GZipOutputStream)

    baos.writeTo(output)
  }
}

private[smv]
object ServerResponse {
  // This should be bumped up if the api changes to make sure client/server are using the same api version.
  val apiVersion: Int = 1

  val STATUS_OK = 0
  val STATUS_ERR_CLASS_NOT_FOUND = 1
  val STATUS_ERR = 2

  /**
   * Create a ServerResponse object from an encoded message as byte array.
   */
  def apply(encoded: Array[Byte]) = {
    val dis = new DataInputStream(new ByteArrayInputStream(encoded))
    val _apiVersion = dis.readInt()

    assert(_apiVersion == apiVersion)

    val _status = dis.readInt()
    val (_classVersion, _classBytes) = if (_status == STATUS_OK) {
      val v = dis.readLong()
      val numBytes = dis.readInt()
      val rawBytes = new Array[Byte](numBytes)
      dis.readFully(rawBytes)
      (v, rawBytes)
    } else {
      (0L, null)
    }

    new ServerResponse(_status, _classVersion, _classBytes)
  }
}