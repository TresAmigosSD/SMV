package org.tresamigos.smv

import java.io.File

class SmvHiveTableTest extends SmvTestUtil {
  test("test SmvHiveTable read/write") {
    val be = app.dsm.load(HiveModules.BarExport.urn).head
    val bt = app.dsm.load(HiveModules.BarTable.urn).head

    // point hive metastore to local file in test dir (instead of default "/user/hive/...")
    val warehouseDir = new File(testcaseTempDir).getAbsolutePath
    sqlContext.setConf("hive.metastore.warehouse.dir", "file://" + warehouseDir)
    be.exportToHive

    assertDataFramesEqual(be.rdd(), bt.rdd())
  }

}

package HiveModules {
  object BarExport extends SmvModule("") with SmvOutput {
    def requiresDS = Seq()
    override def tableName = "bar"
    def run(i: runParams) = app.createDF("k:String; v:Integer", """k1,15; k2,20; k3,25""")
  }

  object BarTable extends SmvHiveTable("bar")
}
