package org.tresamigos.smv

import java.io.{PrintWriter, File}


class SmvHDFSTest extends SparkTestUtil {

  /** root of all test files in this suite. May or may not already exist! */
  private val hdfsTestDir = new File(s"${testDataDir}SmvHDFSTest")

  /** wiope out the test directory and recreate an empty instance. */
  private def resetTestDir = {
    SmvHDFS.deleteFile(hdfsTestDir.getAbsolutePath)
    hdfsTestDir.mkdir()
  }

  /** create dummy file in test dir and return File path */
  private def createDummyFile(baseName: String): File = {
    val outFile = new File(hdfsTestDir, baseName)
    val pw = new PrintWriter(outFile)
    pw.write("Hello, world")
    pw.close
    outFile
  }

  test("Test HDFS delete file operation") {
    val outFilePath = createDummyFile("out.csv")

    // confirm csv file now exists.
    assert( outFilePath.canRead() )

    // delete the generated file and confirm we get true from delete op
    val res1 = SmvHDFS.deleteFile(outFilePath.getAbsolutePath())
    assert(res1)

    // confirm csv file no longer exists.
    assert( ! outFilePath.canRead() )

    // second delete should just return false silently.
    val res2 = SmvHDFS.deleteFile(outFilePath.getAbsolutePath())
    assert(!res2)
  }

  test("Test HDFS list directory operation") {
    resetTestDir

    createDummyFile("F3.csv")
    createDummyFile("F1.csv")
    createDummyFile("F2.csv")

    val files = SmvHDFS.dirList(hdfsTestDir.getAbsolutePath)
    assertUnorderedSeqEqual(files, Seq("F1.csv", "F2.csv", "F3.csv"))
  }

  test("Test HDFS list non-existant directory") {
    resetTestDir
    val files = SmvHDFS.dirList("/Some_Directory_That_Does_Not_Exist")
    assert(files === Seq.empty[String])
  }
}
