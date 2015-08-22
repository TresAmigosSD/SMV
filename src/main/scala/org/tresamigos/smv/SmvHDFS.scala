package org.tresamigos.smv

import org.apache.hadoop.fs.FileSystem

import scala.util.Try

/**
 * Some helper HDFS functions.
 */
object SmvHDFS {

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
}
