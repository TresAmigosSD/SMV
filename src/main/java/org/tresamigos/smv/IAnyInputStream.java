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

package org.tresamigos.smv;

/**
 * An input stream interface to bridge Python's file object and Java's InputStream.
 */
public interface IAnyInputStream {
	/**
	 * Attempts to read from this input stream at most max number of bytes.
	 *
	 * @param max the most number of bytes to read
	 *
	 * @return a tuple containing the number of bytes read and the content as a byte array
	 */
	scala.Tuple2<Integer, byte[]> read(int max);

	void close();
}
