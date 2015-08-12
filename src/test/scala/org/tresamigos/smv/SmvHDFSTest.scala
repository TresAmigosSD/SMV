package org.tresamigos.smv

import java.io.File


class SmvHDFSTest extends SparkTestUtil {
  sparkTest("Test HDFS delete file operation") {
    val outFilePath = new File(testDataDir + "SmvHDFSTest/out.csv")

    // create a dummy DF and save it to a scratch file.
    val df = createSchemaRdd("k:String;", "a;b;c")
    df.rdd.saveAsTextFile(outFilePath.getAbsolutePath)

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
}
