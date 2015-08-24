import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.{FullOuter, Inner, LeftOuter}
import org.apache.spark.sql.catalyst.expressions._
import org.tresamigos.smv._

val sqlContext = new SQLContext(sc)
import sqlContext._

object i {
  def smvSchema(df: DataFrame) = SmvSchema.fromDataFrame(df)
  def open(fullPath: String) = {
    implicit val ca = CsvAttributes.defaultCsvWithHeader
    sqlContext.csvFileWithSchema(fullPath)
  }
  def save(df: DataFrame, fullPath: String) = {
    df.saveAsCsvWithSchema(fullPath)(CsvAttributes.defaultCsvWithHeader)
  }
}
