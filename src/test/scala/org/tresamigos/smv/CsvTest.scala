package org.tresamigos.smv

class CsvTest extends SparkTestUtil {

  sparkTest("Test loading of csv file with header") {
    // without the implict csv attribute with header, we would get a format error
    implicit val ca = CsvAttributes.defaultCsvWithHeader
    val srdd = sqlContext.csvFileWithSchema(testDataDir +  "CsvTest/test1")
    val res = srdd.map(r => (r(0), r(1))).collect.mkString(",")
    // TODO: should probably add a assertSRDDEqual() with expected rows instead of convert to string
    assert(res === "(Bob,1),(Fred,2)")
  }

  sparkTest("Test column with pure blanks converts to null as Integer or Double") {
    val df = createSchemaRdd("a:Integer;b:Double", "1 , 0.2 ; 2, 1 ;3, ; , ;5, 3.")
    assertSrddDataEqual(df,
      "1,0.2;" +
      "2,1.0;" +
      "3,null;" +
      "null,null;" +
      "5,3.0")
  }
}
