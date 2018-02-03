package org.tresamigos.smv

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

class CsvTest extends SmvTestUtil {

  test("Test loading of csv file with header") {
    val file = SmvCsvFile("./" + testDataDir + "CsvTest/test1", CsvAttributes.defaultCsvWithHeader)
    val df   = file.rdd(collector=new SmvRunInfoCollector)
    val res  = df.map(r => (r(0), r(1))).collect.mkString(",")
    // TODO: should probably add a assertSRDDEqual() with expected rows instead of convert to string
    assert(res === "(Bob,1),(Fred,2)")
  }

  test("Test column with pure blanks converts to null as Integer or Double") {
    val df = dfFrom("a:Integer;b:Double", "1 , 0.2 ; 2, 1 ;3, ; , ;5, 3.")
    assertSrddDataEqual(df,
                        "1,0.2;" +
                          "2,1.0;" +
                          "3,null;" +
                          "null,null;" +
                          "5,3.0")
  }

  test("Test run method in SmvFile") {
    object TestFile
        extends SmvCsvFile("./" + testDataDir + "CsvTest/test1",
                           CsvAttributes.defaultCsvWithHeader) {
      override def run(df: DataFrame) = {
        import df.sqlContext.implicits._
        df.smvSelectPlus(smvfuncs.smvStrCat($"name", $"id") as "name_id")
      }
    }
    val df = TestFile.rdd(collector=new SmvRunInfoCollector)
    assertSrddDataEqual(df,
                        "Bob,1,Bob1;" +
                          "Fred,2,Fred2")
  }

  test("Test change csv attribute modifies hash") {
    val file1 =
      SmvCsvFile(
        "./" + testDataDir + "CsvTest/test1",
        CsvAttributes.defaultCsvWithHeader,
        null,
        false,
        Some("@quote-char=|;eman:String;di:integer")
      )

    val file2 =
      SmvCsvFile(
        "./" + testDataDir + "CsvTest/test1",
        CsvAttributes.defaultCsvWithHeader,
        null,
        false,
        Some("@quote-char=';eman:String;di:integer")
      )

    val file3 =
      SmvCsvFile(
        "./" + testDataDir + "CsvTest/test1",
        CsvAttributes.defaultCsvWithHeader,
        null,
        false,
        Some("@quote-char=';eman:String;di:integer")
      )

    assert(file1.instanceValHash != file2.instanceValHash)
    assert(file2.instanceValHash == file3.instanceValHash)
  }

  test("Test reading CSV file with user-defined schema") {
    val file =
      SmvCsvFile(
        "./" + testDataDir + "CsvTest/test1",
        CsvAttributes.defaultCsvWithHeader,
        null,
        false,
        Some("eman:String;di:integer")
      )

    val res = file.rdd(collector=new SmvRunInfoCollector)

    assertSrddDataEqual(res, "Bob,1;Fred,2")
    assertSrddSchemaEqual(res, "eman:String;di:integer")
  }

  test("Test reading CSV file with attributes in schema file.") {
    val file = SmvCsvFile("./" + testDataDir + "CsvTest/test2.csv")
    val df   = file.rdd(collector=new SmvRunInfoCollector)

    // take the sum of second column (age) to make sure csv was interpreted correctly.
    val res = df.agg(sum(df("age")))

    assertSrddDataEqual(res, "46")
  }

  test("Test writing CSV file with attributes in schema file.") {
    val df         = dfFrom("f1:String;f2:String", "x,y;a,b").repartition(1)
    val ca         = CsvAttributes('|', '^', true)
    val csvPath    = testcaseTempDir + "/test_attr.csv"
    val schemaPath = testcaseTempDir + "/test_attr.schema"
    df.saveAsCsvWithSchema(csvPath, ca)

    // verify header partition in csv file
    assertFileEqual(csvPath + "/part-00000", "^f1^|^f2^\n")

    // verify data partition in csv file
    assertFileEqual(csvPath + "/part-00001", "^x^|^y^\n^a^|^b^\n")

    // verify schema file output
    assertFileEqual(
      schemaPath + "/part-00000",
      """@delimiter = |
        |@has-header = true
        |@quote-char = ^
        |f1: String
        |f2: String
        |""".stripMargin
    )
  }

  test("Test persisting file with null strings") {
    object A extends SmvModule("A Module") {
      override def requiresDS() = Seq()
      override def run(inputs: runParams) = {
        app.createDF("a:String", "1;;3").smvSelectPlus(lit("") as "b").repartition(1)
      }
    }

    val res = A.rdd(collector=new SmvRunInfoCollector)
    assertSrddDataEqual(res, """1,;null,;3,""")
    assertFileEqual(
      A.moduleCsvPath() + "/part-00000",
      """"1",""
        |"_SmvStrNull_",""
        |"3",""
        |""".stripMargin
    )
    assertFileEqual(
      A.moduleSchemaPath() + "/part-00000",
      """@delimiter = ,
        |@has-header = false
        |@quote-char = "
        |a: String[,_SmvStrNull_]
        |b: String[,_SmvStrNull_]
        |""".stripMargin
    )

  }

  test("Read file with quote-char null") {
    val file = SmvCsvFile("./" + testDataDir + "CsvTest/test3.csv")
    val exp = dfFrom("@quote-char = \\0;name: String;age: Integer",
                    """Bob,22;
                    |Fred,24;
                    |"Joe,50""".stripMargin
              )
    assertDataFramesEqual(file.rdd(collector=new SmvRunInfoCollector), exp)
  }

  test("Write file with \" as content instead of quote-char") {
    /* In this case res will be saved with @quote-char = ", but data will use Excel escape
       style as """b".
       */
    val res = dfFrom("name:String", "a").withColumn("name2", lit("\"b"))
    val csvPath = testcaseTempDir + "/test_write_data_with_quote"

    res.saveAsCsvWithSchema(csvPath)

    val fileOut = SmvCsvFile("./" + csvPath)
    assertDataFramesEqual(fileOut.rdd(collector=new SmvRunInfoCollector), res)
  }

  test("Test escaping quotes in strings") {

    /** Test for CSV Excel format  **/
    var df = dfFrom("f1:String;f2:String",
                    "\"left\"\"right comma,\",\"escape char \\\";\"a\",\"b\"").repartition(1)
    var ca      = CsvAttributes()
    val csvPath = testcaseTempDir + "/test_escape_quotes.csv"

    df.saveAsCsvWithSchema(csvPath, ca)

    // verify that input "" is converted to " internally in DF
    assert(df.collect()(0)(0) == """left"right comma,""")

    // verify that output uses Excel's CSV format
    assertFileEqual(csvPath + "/part-00000",
                    """"left""right comma,","escape char \"
                      |"a","b"
                      |""".stripMargin)

    var dfOut = open(csvPath)

    // verify that serialize/desrialize results in the same output
    assertDataFramesEqual(df, dfOut)

    /** Test for other formats.  This one uses ^ as a quote char and \ as an escape char **/
    df = dfFrom("@delimiter = ,;@has-header = false;@quote-char = ^;f1:String;f2:String",
                "^left\\^right quote\\\" comma\\, ^,^escape char\\\\^;a,b").repartition(1)
    ca = CsvAttributes(',', '^', false)
    val csvPathCaret = testcaseTempDir + "/test_escape_caret.csv"

    df.saveAsCsvWithSchema(csvPathCaret, ca)
    val file = SmvCsvFile("./" + csvPathCaret, ca)
    dfOut = file.rdd(collector=new SmvRunInfoCollector)

    assertDataFramesEqual(df, dfOut)

    /** Test for arrays **/
    df = dfFrom("a:String;b:Array[String]", """ "a1", "b""1|b2" """).repartition(1)
    ca = CsvAttributes()
    val csvPathArray = testcaseTempDir + "/test_escape_array.csv"

    df.saveAsCsvWithSchema(csvPathArray, ca)

    // verify that output uses Excel's CSV format
    assertFileEqual(csvPathArray + "/part-00000",
                    """"a1","b""1|b2"
                      |""".stripMargin)

    dfOut = open(csvPathArray)

    // verify that serialize/desrialize results in the same output
    assertDataFramesEqual(df, dfOut)

  }

}
