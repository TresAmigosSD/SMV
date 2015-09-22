package org.tresamigos.smv

import org.apache.spark.sql.DataFrame

class CsvTest extends SmvTestUtil {

  test("Test loading of csv file with header") {
    val file = SmvCsvFile("./" + testDataDir +  "CsvTest/test1", CsvAttributes.defaultCsvWithHeader)
    val df = file.rdd
    val res = df.map(r => (r(0), r(1))).collect.mkString(",")
    // TODO: should probably add a assertSRDDEqual() with expected rows instead of convert to string
    assert(res === "(Bob,1),(Fred,2)")
  }

  test("Test column with pure blanks converts to null as Integer or Double") {
    val df = createSchemaRdd("a:Integer;b:Double", "1 , 0.2 ; 2, 1 ;3, ; , ;5, 3.")
    assertSrddDataEqual(df,
      "1,0.2;" +
      "2,1.0;" +
      "3,null;" +
      "null,null;" +
      "5,3.0")
  }

  test("Test run method in SmvFile") {
    object TestFile extends SmvCsvFile("./" + testDataDir +  "CsvTest/test1", CsvAttributes.defaultCsvWithHeader) {
      override def run(df: DataFrame) = {
        import df.sqlContext.implicits._
        df.selectPlus(smvStrCat($"name", $"id") as "name_id")
      }
    }
    val df = TestFile.rdd
    assertSrddDataEqual(df,
      "Bob,1,Bob1;" +
      "Fred,2,Fred2")
  }
}
