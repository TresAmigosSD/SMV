package org.tresamigos.smv

import org.apache.hadoop.fs.FileSystem

/**
 * Some helper HDFS functions.
 */
object SmvHDFS {

  /** default hadoop configuration. */
  private val hadoopConf = new org.apache.hadoop.conf.Configuration()

  /**
   * delete an HDFS file by given path.
   *
   * We can not use the spark config "spark.hadoop.skipOutputChecks" as it will only overwrite partitions
   * and not the entire directory.  So if you overwrite an existing file with less partitions, some old
   * partitions will linger around (BAD!!!!)
   */
  def deleteFile(fileName: String) : Boolean = {
    val path = new org.apache.hadoop.fs.Path(fileName)
    val uri = java.net.URI.create(fileName)
    val hdfs = FileSystem.get(uri, hadoopConf)

    hdfs.delete(path, true)
  }
}
