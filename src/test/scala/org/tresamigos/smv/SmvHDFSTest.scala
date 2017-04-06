package org.tresamigos.smv

class SmvHDFSTest extends SmvTestUtil {

  test("Test HDFS delete file operation") {
    resetTestcaseTempDir()
    val outFilePath = createTempFile("out.csv")

    // confirm csv file now exists.
    assert(outFilePath.canRead())

    // delete the generated file and confirm we get true from delete op
    val res1 = SmvHDFS.deleteFile(outFilePath.getAbsolutePath())
    assert(res1)

    // confirm csv file no longer exists.
    assert(!outFilePath.canRead())

    // second delete should just return false silently.
    val res2 = SmvHDFS.deleteFile(outFilePath.getAbsolutePath())
    assert(!res2)
  }

  test("Test HDFS list directory operation") {
    resetTestcaseTempDir()

    createTempFile("F3.csv")
    createTempFile("F1.csv")
    createTempFile("F2.csv")

    val files = SmvHDFS.dirList(testcaseTempDir)
    assertUnorderedSeqEqual(files, Seq("F1.csv", "F2.csv", "F3.csv"))
  }

  test("Test HDFS list non-existent directory") {
    resetTestcaseTempDir()
    val files = SmvHDFS.dirList("/Some_Directory_That_Does_Not_Exist")
    assert(files === Seq.empty[String])
  }

  test("Test the basename function in HDFS") {
    assert(SmvHDFS.baseName("/tmp/a/x.csv") === "x.csv")
    assert(SmvHDFS.baseName("file://tmp/a/y.csv") === "y.csv")
    assert(SmvHDFS.baseName("hdfs:localhost:4040/tmp/a/z.csv") === "z.csv")
  }

  test("Test purging old files from directory") {
    resetTestcaseTempDir
    createTempFile("F1.csv")
    createTempFile("F2.csv")
    createTempFile("F3.csv")

    SmvHDFS.purgeDirectory(testcaseTempDir, Seq("F3.csv"))

    // only F3.csv should remain in the dir now.
    val files = SmvHDFS.dirList(testcaseTempDir)
    assertUnorderedSeqEqual(files, Seq("F3.csv"))
  }

  test("Test file modificationTime") {
    resetTestcaseTempDir
    createTempFile("F1.csv")

    val mt0 = SmvHDFS.modificationTime("target")
    val mt1 = SmvHDFS.modificationTime(s"${testcaseTempDir}/F1.csv")
    assert(mt0 < mt1)
  }

  test("Test HDFS file read") {
    resetTestcaseTempDir()

    createTempFile("F1", "hahahaha")
    val res = SmvHDFS.readFromFile(testcaseTempDir + "/F1")
    assert(res === "hahahaha")
  }

  test("Test HDFS file write") {
    resetTestcaseTempDir()

    SmvHDFS.writeToFile("test contents", testcaseTempDir + "/F2")
    val res = SmvHDFS.readFromFile(testcaseTempDir + "/F2")
    assert(res === "test contents")
  }

}
