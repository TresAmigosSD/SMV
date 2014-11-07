import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftOuter}
import org.apache.spark.sql.catalyst.expressions._
import org.tresamigos.smv._

val sqlContext = new SQLContext(sc)
import sqlContext._

object i {
  def smvSchema(srdd: SchemaRDD) = Schema.fromSchemaRDD(srdd)
  def open(fullPath: String) = {
    implicit val ca = CsvAttributes.defaultCsvWithHeader
    sqlContext.csvFileWithSchema(fullPath)
  }
  def save(srdd: SchemaRDD, fullPath: String) = {
    srdd.saveAsCsvWithSchema(fullPath)(CsvAttributes.defaultCsvWithHeader)
  }

  def createSchemaRdd(schemaStr: String, data: String) = {
    val schema = Schema.fromString(schemaStr)
    val dataArray = data.split(";").map(_.trim)
    val rowRDD = sc.makeRDD(dataArray).csvToSeqStringRDD.seqStringRDDToRowRDD(schema)
    sqlContext.applySchemaToRowRDD(rowRDD, schema)
  }
}
