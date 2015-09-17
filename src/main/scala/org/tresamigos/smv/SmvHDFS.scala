package org.tresamigos.smv

import java.io.File
import java.io.{BufferedWriter, StringWriter, OutputStreamWriter}

import org.apache.hadoop.fs.FileSystem
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

  /**
   * delete an HDFS file by given path.
   *
   * We can not use the spark config "spark.hadoop.skipOutputChecks" as it will only overwrite partitions
   * and not the entire directory.  So if you overwrite an existing file with less partitions, some old
   * partitions will linger around (BAD!!!!)
   */
  def deleteFile(fileName: String) : Boolean = {
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

  def writeToFile(contents: String, fileName: String) = {
    val path = new org.apache.hadoop.fs.Path(fileName)
    val hdfs = getFileSystem(fileName)

    if ( hdfs.exists(path)) hdfs.delete(path, true)
    val stream = hdfs.create(path)
    val writer = new BufferedWriter( new OutputStreamWriter(stream, "UTF-8") )
    writer.write(contents);
    writer.close()
    stream.close()
  }

  /**
   * get modification time of a HDFS file
   **/

  def modificationTime(fileName: String) : Long = {
    val path = new org.apache.hadoop.fs.Path(fileName)
    val hdfs = getFileSystem(fileName)

    hdfs.getFileStatus(path).getModificationTime()
  }
  /**
   * Return a list of files in the given directory.
   * Note that we don't use hdfs.listFiles as it was not available in earlier
   * version of hadoop and we don't want to force our users to upgrade.
   * The only exception would be if we needed to implement recursive directory
   * walk, then we should switch to listFiles method instead.
   */
  def dirList(dirName: String) : Seq[String] = {
    Try {
      val path = new org.apache.hadoop.fs.Path(dirName)
      val hdfs = getFileSystem(dirName)
      hdfs.listStatus(path).map(_.getPath.getName).toSeq
    }.getOrElse(Seq.empty[String])
  }

  /**
   * Returns the basename of a given file path (the last part of the full path)
   */
  def baseName(pathName: String) : String = new File(pathName).getName

  /**
   * Purge the contents of the given directory that are not in the keep list.
   * This is a shallow purge, subdirs in dirName are not inspected.
   * @param dirName directory to purge.
   * @param keepFiles base names of files in above directory to keep
   */
  def purgeDirectory(dirName: String, keepFiles: Seq[String]) = {
    val dirFiles = dirList(dirName)

    (dirFiles.toSet -- keepFiles).foreach { f =>
      deleteFile(s"${dirName}/${f}")
    }
  }
}
