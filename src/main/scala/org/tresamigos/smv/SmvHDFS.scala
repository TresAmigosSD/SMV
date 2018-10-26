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

import java.io.ByteArrayInputStream
import java.io.File
import java.io.FileNotFoundException
import java.io.InputStream
import java.io.OutputStream
import java.io.{BufferedWriter, StringWriter, OutputStreamWriter}
import java.nio.charset.StandardCharsets

import org.apache.hadoop.fs.{FileSystem, Path, FileUtil, FileStatus, FSDataOutputStream}
import org.apache.commons.io.IOUtils

import scala.util.Try

/**
 * Some helper HDFS functions.
 */
private[smv] object SmvHDFS {

  /** default hadoop configuration. */
  private val hadoopConf = new org.apache.hadoop.conf.Configuration()

  /** Return the HDFS FileSystem object than manages the given path. */
  private def getFileSystem(path: String) = {
    val uri = java.net.URI.create(path)
    FileSystem.get(uri, hadoopConf)
  }

  def exists(fileName: String): Boolean = getFileSystem(fileName).exists(new Path(fileName))

  /** Atomically creates a file in the hadoop fs, useful for creating a lockfile */
  def createFileAtomic(fileName: String): Unit =
    getFileSystem(fileName).create(new Path(fileName), false).close()

  /** Returns the file status, may throw FileNotFoundException */
  @throws(classOf[FileNotFoundException])
  def getFileStatus(fileName: String): FileStatus =
    getFileSystem(fileName).getFileStatus(new Path(fileName))

  /**
   * delete an HDFS file by given path.
   *
   * We can not use the spark config "spark.hadoop.skipOutputChecks" as it will only overwrite partitions
   * and not the entire directory.  So if you overwrite an existing file with less partitions, some old
   * partitions will linger around (BAD!!!!)
   */
  def deleteFile(fileName: String): Boolean = {
    val path = new org.apache.hadoop.fs.Path(fileName)
    val hdfs = getFileSystem(fileName)

    hdfs.delete(path, true)
  }

  def readFromFile(fileName: String): String = {
    val path = new org.apache.hadoop.fs.Path(fileName)
    val hdfs = getFileSystem(fileName)

    val stream = hdfs.open(path)
    val writer = new StringWriter()
    IOUtils.copy(stream, writer, "UTF-8");
    writer.toString()
  }

  def writeToFile(contents: String, fileName: String): Unit = {
    val from = InputStreamAdapter(contents)
    try {
      writeToFile(from, fileName)
    } finally {
      from.close()
    }
  }

  def writeToFile(from: IAnyInputStream, fileName: String): Unit = {
    val path = new org.apache.hadoop.fs.Path(fileName)
    val hdfs = getFileSystem(fileName)

    if (hdfs.exists(path)) hdfs.delete(path, true)
    val out = hdfs.create(path)

    try {
      var buf = from.read(8192)
      while (!buf.isEmpty) {
        out.write(buf, 0, buf.size)
        buf = from.read(8192)
      }
    } finally {
      out.close()
    }
  }

  def openForWrite(fileName: String): FSDataOutputStream = {
    val path = new org.apache.hadoop.fs.Path(fileName)
    val hdfs = getFileSystem(fileName)

    if (hdfs.exists(path)) hdfs.delete(path, true)
    hdfs.create(path)
  }

  /**
   * Copy and merge file in HDFS to a single file in local file system
   **/
  def copyMerge(hdfsPath: String, localPath: String) = {
    val hdfs = getFileSystem(hdfsPath)
    val lfs = FileSystem.getLocal(hadoopConf)

    val pathHdfsPath = new Path(hdfsPath)
    val pathLocalPath = new Path(localPath)
    FileUtil.copyMerge(hdfs, pathHdfsPath, lfs, pathLocalPath, false, hadoopConf, "")
  }

  /**
   * get modification time of a HDFS file.
   * If the file path contains a "*" glob pattern, 0 is returned.
   * TODO: should come up with a hash of all modification times for which the
   * glob patten matchs (and rename this method)
   **/
  def modificationTime(fileName: String): Long = {
    if (fileName contains "*") {
      0
    } else {
      val path = new org.apache.hadoop.fs.Path(fileName)
      val hdfs = getFileSystem(fileName)

      hdfs.getFileStatus(path).getModificationTime()
    }
  }

  /**
   * Return a list of files in the given directory.
   * Note that we don't use hdfs.listFiles as it was not available in earlier
   * version of hadoop and we don't want to force our users to upgrade.
   * The only exception would be if we needed to implement recursive directory
   * walk, then we should switch to listFiles method instead.
   */
  def dirList(dirName: String): Seq[String] = {
    Try {
      val path = new org.apache.hadoop.fs.Path(dirName)
      val hdfs = getFileSystem(dirName)
      hdfs.listStatus(path).map(_.getPath.getName).toSeq
    }.getOrElse(Seq.empty[String])
  }

  /**
   * Returns the basename of a given file path (the last part of the full path)
   */
  def baseName(pathName: String): String = new File(pathName).getName

  /**
   * Purge the contents of the given directory that are not in the keep list.
   * This is a shallow purge, subdirs in dirName are not inspected.
   *
   * @param dirName directory to purge.
   * @param keepFiles base names of files in above directory to keep
   * @return a sequence of to-be-deleted filenames and whether the deletion is successful
   */
  def purgeDirectory(dirName: String, keepFiles: Seq[String]): Seq[(String, Boolean)] =
    for {
      file <- (dirList(dirName).toSet -- keepFiles).toSeq.sorted
      filename = s"${dirName}/${file}"
      r = deleteFile(filename)
    } yield (filename, r)
}

/**
 * Adapts a java InputStream object to the IAnyInputStream interface,
 * so it can be used in I/O methods that can work with input streams
 * from both Java and Python sources.
 */
class InputStreamAdapter(in: InputStream) extends IAnyInputStream {
  @Override def read(max: Int) = {
    val buf = new Array[Byte](max)
    var size = 0
    while (0 == size)
      size = in.read(buf)
    if (-1 == size) Array.empty else buf.slice(0, size)
  }
  @Override def close(): Unit = in.close()
}

/** Factory object to create InputStreamAdapters from input sources */
object InputStreamAdapter {
  def apply(in: InputStream): InputStreamAdapter = new InputStreamAdapter(in)
  def apply(content: String): InputStreamAdapter = new InputStreamAdapter(
    new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)))
}
