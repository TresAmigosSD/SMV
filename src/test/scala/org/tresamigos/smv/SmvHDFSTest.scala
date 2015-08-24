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

  test("Test HDFS list non-existent directory") {
    resetTestDir
    val files = SmvHDFS.dirList("/Some_Directory_That_Does_Not_Exist")
    assert(files === Seq.empty[String])
  }

  test("Test the basename function in HDFS") {
    assert(SmvHDFS.baseName("/tmp/a/x.csv") === "x.csv")
    assert(SmvHDFS.baseName("file://tmp/a/y.csv") === "y.csv")
    assert(SmvHDFS.baseName("hdfs:localhost:4040/tmp/a/z.csv") === "z.csv")
  }

  test("Test purging old files from directory") {
    resetTestDir
    createDummyFile("F1.csv")
    createDummyFile("F2.csv")
    createDummyFile("F3.csv")

    SmvHDFS.purgeDirectory(hdfsTestDir.getAbsolutePath, Seq("F3.csv"))

    // only F3.csv should remain in the dir now.
    val files = SmvHDFS.dirList(hdfsTestDir.getAbsolutePath)
    assertUnorderedSeqEqual(files, Seq("F3.csv"))

  }
}
