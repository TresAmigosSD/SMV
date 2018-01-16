package org.tresamigos.smv

import java.io.File

class SmvHiveTableTest extends SmvTestUtil {
  override def appArgs =
    Seq(
      "-m",
      "C",
      "--data-dir",
      testcaseTempDir,
      "--input-dir",
      testcaseTempDir,
      "--smv-props",
      "smv.stages=org.tresamigos.smv"
    )

  test("test SmvHiveTable read/write") {
    val be = app.dsm.load(HiveModules.BarExport.urn).head
    val bt = app.dsm.load(HiveModules.BarTable.urn).head

    // point hive metastore to local file in test dir (instead of default "/user/hive/...")
    val warehouseDir = new File(testcaseTempDir).getAbsolutePath
    sqlContext.setConf("hive.metastore.warehouse.dir", "file://" + warehouseDir)
    be.exportToHive(collector=new SmvRunInfoCollector)

    assertDataFramesEqual(be.rdd(collector=new SmvRunInfoCollector), bt.rdd(collector=new SmvRunInfoCollector))
  }

  test("test SmvHiveTable publishHiveSql") {
    val be = app.dsm.load(HiveModules.BarSqlExport.urn).head
    val bt = app.dsm.load(HiveModules.BarTable.urn).head

    // point hive metastore to local file in test dir (instead of default "/user/hive/...")
    val warehouseDir = new File(testcaseTempDir).getAbsolutePath
    sqlContext.setConf("hive.metastore.warehouse.dir", "file://" + warehouseDir)
    be.exportToHive(collector=new SmvRunInfoCollector)

    assertDataFramesEqual(be.rdd(collector=new SmvRunInfoCollector), bt.rdd(collector=new SmvRunInfoCollector))
  }
}

package HiveModules {
  object BarExport extends SmvModule("") with SmvOutput {
    def requiresDS = Seq()
    override def tableName = "bar"
    def run(i: runParams) = app.createDF("k:String; v:Integer", """k1,15; k2,20; k3,25""")
  }

  /* test module that used the publishHiveSql override to provide a multi-statement hql to publish */
  object BarSqlExport extends SmvModule("") with SmvOutput {
    def requiresDS = Seq()
    override def publishHiveSql = Some("""
      drop table if exists bar;
      create table bar as select * from dftable
      """)
    def run(i: runParams) = app.createDF("k:String; v:Integer", """k1,15; k2,20; k3,25""")
  }

  object BarTable extends SmvHiveTable("bar")
}
