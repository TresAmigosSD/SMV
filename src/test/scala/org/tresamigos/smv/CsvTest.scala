package org.tresamigos.smv

class CsvTest extends SparkTestUtil {

  sparkTest("Test loading of csv file with header") {
    // without the implict csv attribute with header, we would get a format error
    implicit val ca = CsvAttributes.defaultCsvWithHeader
    val df = sqlContext.csvFileWithSchema(testDataDir +  "CsvTest/test1")
    val res = df.map(r => (r(0), r(1))).collect.mkString(",")
    // TODO: should probably add a assertSRDDEqual() with expected rows instead of convert to string
    assert(res === "(Bob,1),(Fred,2)")
  }

}