/*
 * This file is licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tresamigos.smv

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.{Literal, Row, AttributeReference}
import org.apache.spark.sql.DataFrame

import org.apache.spark.sql.catalyst.plans.logical._

import java.text.{DateFormat, SimpleDateFormat}

import scala.annotation.switch

private[smv] abstract class SchemaEntry extends java.io.Serializable {
  val name: String
  val dataType: DataType
  def strToVal(s: String) : Any
  def valToStr(v: Any) : String = if (v==null) "" else v.toString
  val typeName: String
  private[smv] var meta: String = ""
  override def toString = name + ": " + typeName
}

private[smv] abstract class NumericSchemaEntry extends SchemaEntry {
  private[smv] def trim(s: String) = s.replaceAll("""(^ +| +$)""", "")
}

private[smv] case class DoubleSchemaEntry(name: String) extends NumericSchemaEntry {
  override def strToVal(s:String) : Any = {
    val trimedS = trim(s)
    if (trimedS.isEmpty) null else trimedS.toDouble
  }
  override val typeName = "Double"
  val dataType = DoubleType
}

private[smv] case class FloatSchemaEntry(name: String) extends NumericSchemaEntry {
  override def strToVal(s:String) : Any = {
    val trimedS = trim(s)
    if (trimedS.isEmpty) null else trimedS.toFloat
  }
  override val typeName = "Float"
  val dataType = FloatType
}

private[smv] case class IntegerSchemaEntry(name: String) extends NumericSchemaEntry {
  override def strToVal(s:String) : Any = {
    val trimedS = trim(s)
    if (trimedS.isEmpty) null else trimedS.toInt
  }
  override val typeName = "Integer"
  val dataType = IntegerType
}

private[smv] case class LongSchemaEntry(name: String) extends NumericSchemaEntry {
  override def strToVal(s:String) : Any = {
    val trimedS = trim(s)
    if (trimedS.isEmpty) null else trimedS.toLong
  }
  override val typeName = "Long"
  val dataType = LongType
}

private[smv] case class BooleanSchemaEntry(name: String) extends SchemaEntry {
  override def strToVal(s:String) : Any = if (s.isEmpty) null else s.toBoolean
  override val typeName = "Boolean"
  val dataType = BooleanType
}

private[smv] case class StringSchemaEntry(name: String) extends SchemaEntry {
  override def strToVal(s:String) : Any = if (s.isEmpty) null else s
  override val typeName = "String"
  val dataType = StringType
}

private[smv] case class TimestampSchemaEntry(name: String, fmt: String = "yyyy-MM-dd hh:mm:ss.S") extends SchemaEntry {
  // `SimpleDateFormat` is not thread-safe.
  val fmtObj = SmvSchema.threadLocalDateFormat(fmt).get()
  //val fmtObj = new java.text.SimpleDateFormat(fmt)
  override def strToVal(s:String) : Any = {
    if(s.isEmpty) null
    else new java.sql.Timestamp(fmtObj.parse(s).getTime())
  }
  override val typeName = "Timestamp"
  val dataType = TimestampType
  override def toString = s"$name: $typeName[$fmt]"
}

private[smv] case class DateSchemaEntry(name: String, fmt: String = "yyyy-MM-dd") extends SchemaEntry {

  val fmtObj = SmvSchema.threadLocalDateFormat(fmt).get()

  override def strToVal(s:String) : Any = {
    if (s.isEmpty) null
    else DateUtils.millisToDays(fmtObj.parse(s).getTime())
  }

  override def valToStr(v: Any) : String = {
    if (v==null) ""
    else fmtObj.format(DateUtils.toJavaDate(v.asInstanceOf[Int]))
  }

  override val typeName = "Date"
  val dataType = DateType
  override def toString = s"$name: $typeName[$fmt]"
}
// TODO: map entries delimiter hardcoded to "|" for now.
// TODO: not worrying about key/val values containing the delimiter for now.
// TODO: only allow basic types to avoid creating a full parser for the sub-types.
private[smv] case class MapSchemaEntry(name: String,
      keySchemaEntry: SchemaEntry, valSchemaEntry: SchemaEntry) extends SchemaEntry {
  override val typeName = "Map"

  override def toString = s"$name: Map[${keySchemaEntry.typeName},${valSchemaEntry.typeName}]"

  val dataType = MapType(keySchemaEntry.dataType, valSchemaEntry.dataType)
  override def strToVal(s: String) : Any = {
    if (s.isEmpty)
      null
    else
      // TODO: should probably use openCSV to parse the map to array
      s.split("\\|").sliding(2,2).map { a =>
        (keySchemaEntry.strToVal(a(0)), valSchemaEntry.strToVal(a(1)))
      }.toMap
  }
  override def valToStr(v: Any) : String = {
    if (v==null) return ""
    val keyNativeType = keySchemaEntry.dataType.asInstanceOf[NativeType]
    val valNativeType = valSchemaEntry.dataType.asInstanceOf[NativeType]
    val m = v.asInstanceOf[Map[Any,Any]]
    m.map{ case (k,v) =>
      val keyAsStr = keySchemaEntry.valToStr(k)
      val valAsStr = valSchemaEntry.valToStr(v)
      s"${keyAsStr}|${valAsStr}"
    }.mkString("|")
  }
}

private[smv] case class ArraySchemaEntry(name: String, valSchemaEntry: SchemaEntry) extends SchemaEntry {
  override val typeName = "Array"

  override def toString = s"$name: Array[${valSchemaEntry.typeName}]"

  val dataType = ArrayType(valSchemaEntry.dataType)
  override def strToVal(s: String) : Any = {
    if (s.isEmpty)
      null
    else
      // TODO: should probably use openCSV to parse the map to array
      s.split("\\|").map { a => valSchemaEntry.strToVal(a) }
  }
  override def valToStr(v: Any) : String = {
    if (v==null) return ""
    val m = v match {
      case a: Seq[Any] => a
      case a: Array[_] => a.toSeq
    }
    m.map{r => valSchemaEntry.valToStr(r)}.mkString("|")
  }
}

private[smv] object NumericSchemaEntry {
  def apply(name: String, dataType: DataType) : NumericSchemaEntry = {
    val trimName = name.trim
    dataType match {
      case DoubleType => DoubleSchemaEntry(trimName)
      case FloatType => FloatSchemaEntry(trimName)
      case LongType => LongSchemaEntry(trimName)
      case IntegerType => IntegerSchemaEntry(trimName)
      case _ => throw new IllegalArgumentException(s"Type: ${dataType.toString} is not numeric")
    }
  }
}

private[smv] object SchemaEntry {
  private final val StringPattern = "[sS]tring".r
  private final val DoublePattern = "[dD]ouble".r
  private final val FloatPattern = "[fF]loat".r
  private final val LongPattern = "[lL]ong".r
  private final val IntegerPattern = "[iI]nteger".r
  private final val BooleanPattern = "[bB]oolean".r
  private final val TimestampPatternFmt = "[tT]imestamp\\[(.+)\\]".r
  private final val TimestampPattern = "[tT]imestamp".r
  private final val DatePatternFmt = "[dD]ate\\[(.+)\\]".r
  private final val DatePattern = "[dD]ate".r
  private final val MapPattern = "[mM]ap\\[(.+),(.+)\\]".r
  private final val ArrayPattern = "[aA]rray\\[(.+)\\]".r

  def apply(name: String, typeStr: String) : SchemaEntry = {
    val trimName = name.trim
    typeStr.trim match {
      case StringPattern() => StringSchemaEntry(trimName)
      case DoublePattern() => DoubleSchemaEntry(trimName)
      case FloatPattern() => FloatSchemaEntry(trimName)
      case LongPattern() => LongSchemaEntry(trimName)
      case IntegerPattern() => IntegerSchemaEntry(trimName)
      case BooleanPattern() => BooleanSchemaEntry(trimName)
      case TimestampPattern() => TimestampSchemaEntry(trimName)
      case TimestampPatternFmt(fmt) => TimestampSchemaEntry(trimName, fmt)
      case DatePattern() => DateSchemaEntry(trimName)
      case DatePatternFmt(fmt) => DateSchemaEntry(trimName, fmt)
      case MapPattern(keyTypeStr, valTypeStr) =>
        MapSchemaEntry(trimName,
          SchemaEntry("keyType", keyTypeStr),
          SchemaEntry("valType", valTypeStr))
      case ArrayPattern(valTypeStr) =>
        ArraySchemaEntry(trimName,
          SchemaEntry("valType", valTypeStr))
      case _ => throw new IllegalArgumentException(s"unknown type: $typeStr")
    }
  }

  def apply(name: String, dataType: DataType) : SchemaEntry = {
    val trimName = name.trim
    dataType match {
      case StringType => StringSchemaEntry(trimName)
      case DoubleType => DoubleSchemaEntry(trimName)
      case FloatType => FloatSchemaEntry(trimName)
      case LongType => LongSchemaEntry(trimName)
      case IntegerType => IntegerSchemaEntry(trimName)
      case BooleanType => BooleanSchemaEntry(trimName)
      case TimestampType => TimestampSchemaEntry(trimName)
      case DateType => DateSchemaEntry(trimName)
      case MapType(keyType, valType, _) =>
        MapSchemaEntry(trimName,
          SchemaEntry("keyType", keyType),
          SchemaEntry("valType", valType))
      case ArrayType(valType, _) =>
        ArraySchemaEntry(trimName,
          SchemaEntry("valType", valType))
      case _ => throw new IllegalArgumentException(s"unknown type: ${dataType.toString}")
    }
  }

  def apply(structField: StructField) : SchemaEntry = {
    SchemaEntry(structField.name, structField.dataType)
  }

  def apply(nameAndType: String) : SchemaEntry = {
    val parseNT = """([^:]*):(.*)""".r
    nameAndType match {
      case parseNT(n, t) => SchemaEntry(n, t)
      case _ => throw new IllegalArgumentException(s"Illegal SchemaEmtry string: $nameAndType")
    }
  }

  /**
   * maps a given value of any type into a valid column name by transforming invalid
   * characters to "_" and reducing multiple "_" into a single "_" and remove leading/trailing "_"
   * For example:
   * "5/14/2014" --> "5_14_2014"
   * "Hello There !" --> "Hello_There"
   * "Hi, bye" --> "Hi_bye"
   */
  def valueToColumnName(value: Any) : String = {
    "[^a-zA-Z0-9]+".r.replaceAllIn(value.toString, "_").stripPrefix("_").stripSuffix("_")
  }
}

class SmvSchema (val entries: Seq[SchemaEntry]) extends java.io.Serializable {
  private[smv] def getSize = entries.size

  /* Since Spark-1.3.0, Catalyst started to maintain its own type, we always need to call
     ScalaReflection.convertToCatalyst when we construct a Row from Scala.
     However in Spark-1.3.1, the createDataFrame method of SQLContext always call
     a private version with "needsConversion = true", so it always do the conversion on
     Row constructed on Scala. Therefore, the following code with the conversion is only
     need for Spark-1.3.0 but not in Spark-1.3.1 (althogh other than repeate the work
     no real harm either).
   */
  private[smv] def toValue(ordinal: Int, sVal: String) = {
    val entry = entries(ordinal)
    val elem = entry.strToVal(sVal)
    val dataType = entry.dataType
    ScalaReflection.convertToCatalyst(elem, dataType)
  }

  override def toString = "Schema: " + entries.mkString("; ")

  private[smv] def toStructType : StructType = StructType(entries.map(se => StructField(se.name, se.dataType, true)))

  def toStringWithMeta = entries.map{e =>
    e.toString + (if (e.meta.isEmpty) "" else ("\t\t# " + e.meta))
  }

  def saveToFile(sc: SparkContext, path: String) {
    sc.makeRDD(toStringWithMeta, 1).saveAsTextFile(path)
  }

  def saveToLocalFile(path: String) {
    import java.io.{File, PrintWriter}
    val pw = new PrintWriter(new File(path))
    pw.println(toStringWithMeta.mkString("\n"))
    pw.close()
  }

  /**
   * convert a data row to a delimited csv string.
   * Handles delimiter and quote character appearing anywhere in string value.
   */
  private[smv] def rowToCsvString(row: Row)(implicit ca: CsvAttributes) = {
    require(row.size == entries.size)
    val sb = new StringBuilder

    // TODO: should be done using zip,map and mkstring rather than index.
    for (idx <- 0 until row.length) {
      // prepend delimiter except for first item.
      if (idx > 0)
        sb.append(ca.delimiter)

      val se = entries(idx)
      (se.dataType: @switch) match {
        // TODO: handle timestamp here to convert to desired format
        case StringType =>
          // TODO: need to handle this better!
          sb.append("\"")
          sb.append(se.valToStr(row(idx)))
          sb.append("\"")
        case _ => sb.append(se.valToStr(row(idx)))
      }
    }

    sb.toString
  }
}

object SmvSchema {

  /**
   * creates a schema object from an array of raw schema entry strings.
   * Remove comments, empty lines and create the schema entry object
   * from each line.
   */
  private def schemaFromEntryStrings(strEntries : Array[String]) = {
    new SmvSchema(
      strEntries.map(_.replaceFirst("(//|#).*", "")).
      filterNot(_.matches("^[ \t]*$")).
      map(_.replaceFirst(";[ \t]*$", "")).
      map(SchemaEntry(_)).
      toList
    )
  }

  /**
   * convert a single line schema definition string into a schema object.
   * The schema entries in the input line must be separated by ";"
   * example:
   * "id:String; value:Double"
   */
  def fromString(schemaString: String) = {
    schemaFromEntryStrings(schemaString.split(';'))
  }

  /**
   * read a schema file as an RDD of schema entries.  One entry per line.
   * Each entry format is "name : type"
   */
  def fromFile(sc: SparkContext, path: String) = {
    schemaFromEntryStrings(sc.textFile(path).collect)
  }


  def fromDataFrame(df: DataFrame) = {
    new SmvSchema(
      df.schema.fields.map{a =>
        SchemaEntry(a.name, a.dataType)
      }
    )
  }

  /**
   * read a schema file and extract field length from schema file entries for
   * Fixed Record Length data
   *
   * Assume the schema file have part of the comment as "$12" to indicate this
   * field has fixed length 12. Assume all the schema entries in the schema
   * file have the field length number.
   * Example Schema Entry:
   *
   * Customer_ID: String  #customer id: $16
   *
   **/
  private[smv] def slicesFromFile(sc: SparkContext, path: String) = {
    sc.textFile(path).collect.map{s =>
      try{
        Some(s.replaceFirst(".*#.*\\$([0-9]+)[ \t]*$", "$1").toInt)
      } catch {
        case _ : Throwable => None
      }
    }.collect{case Some(c) => c}
  }

  /**
   * map the data file path to schema path.
   * Ignores ".gz", ".csv", ".tsv" extensions when constructions schema file path.
   * For example: "/a/b/foo.csv" --> "/a/b/foo.schema".  Makes for cleaner mapping.
   */
  private[smv] def dataPathToSchemaPath(dataPath: String): String = {
    // remove all known data file extensions from path.
    val exts = List("gz", "csv", "tsv").map("\\."+_+"$")
    val dataPathNoExt = exts.foldLeft(dataPath)((s,e) => s.replaceFirst(e,""))

    dataPathNoExt + ".schema"
  }

  // `SimpleDateFormat` is not thread-safe.
  private[smv] val threadLocalDateFormat = {fmt: String => new ThreadLocal[DateFormat] {
    override def initialValue() = {
      new SimpleDateFormat(fmt)
    }
  }}
}
