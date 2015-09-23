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

import java.io.InputStream
import java.util.zip.CRC32

import scala.util.control.NonFatal

/**
 * computes the CRC32 checksum for the code of the given class name.
 * The class must be reachable through the configured java class path.
 */
private[smv] case class ClassCRC(className: String) {
  // convert com.foo.bar to "/com/foo/bar.class"
  private val classResourcePath = "/" + className.replace('.', '/') + ".class"

  lazy val crc = {
    val is: InputStream = getClass.getResourceAsStream(classResourcePath)
    val crc = new CRC32()
    val ba: Array[Byte] = new Array[Byte](10000)

    try {
      var count = is.read(ba)
      while (count > 0) {
        crc.update(ba, 0, count)
        count = is.read(ba)
      }
    } catch {
      case NonFatal(t) => throw new IllegalArgumentException("invalid class name for crc: " + className, t)
    } finally {
      if (is != null) is.close()
    }

    crc.getValue
  }
}
