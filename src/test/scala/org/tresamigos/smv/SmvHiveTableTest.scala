package org.tresamigos.smv

import java.io.File

class SmvHiveTableTest extends SmvTestUtil {
  test("test SmvHiveTable read") {
    // point hive metastore to local file in test dir (instead of default "/user/hive/...")
    val warehouseDir = new File(testcaseTempDir).getAbsolutePath
    sqlContext.setConf("hive.metastore.warehouse.dir", "file://" + warehouseDir)

    // create a temporary hive table with two columns.
    val t = app.createDF("k:String; v:Integer",
      """k1,15; k2,20; k3,25""")
    t.write.mode("append").saveAsTable("bar")

    val ht = new SmvHiveTable("bar")
    val df = ht.rdd()
    assertSrddDataEqual(df, """k1,15; k2,20; k3,25""")
    assertSrddSchemaEqual(df, "k:String; v:Integer")
  }

}
