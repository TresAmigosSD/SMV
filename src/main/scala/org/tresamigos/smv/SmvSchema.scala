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
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame

import java.sql.Date
import java.text.{DateFormat, SimpleDateFormat}

import scala.util.Try

private[smv] abstract class TypeFormat extends Serializable {
  val dataType: DataType
  val typeName: String
  val format: String = null
  def strToVal(s: String): Any
  def valToStr(v: Any): String = if (v == null) "" else v.toString
  override def toString        = if (format == null) typeName else s"${typeName}[${format}]"
}

private[smv] case class SchemaEntry(field: StructField, typeFormat: TypeFormat)
    extends Serializable {
  override def toString = field.name + ": " + typeFormat.toString

  def toStringWithMeta = {
    val metaStr =
      if (field.metadata == Metadata.empty) ""
      else " @metadata=" + field.metadata.toString

    toString + metaStr
  }
}

private[smv] abstract class NumericTypeFormat extends TypeFormat {
  private[smv] def trim(s: String) = s.replaceAll("""(^ +| +$)""", "")
}

private[smv] case class DoubleTypeFormat(override val format: String = null)
    extends NumericTypeFormat {
  override def strToVal(s: String): Any = {
    val trimedS = trim(s)
    if (trimedS.isEmpty) null else trimedS.toDouble
  }
  override val typeName = "Double"
  val dataType          = DoubleType
}

private[smv] case class FloatTypeFormat(override val format: String = null)
    extends NumericTypeFormat {
  override def strToVal(s: String): Any = {
    val trimedS = trim(s)
    if (trimedS.isEmpty) null else trimedS.toFloat
  }
  override val typeName = "Float"
  val dataType          = FloatType
}

private[smv] case class IntegerTypeFormat(override val format: String = null)
    extends NumericTypeFormat {
  override def strToVal(s: String): Any = {
    val trimedS = trim(s)
    if (trimedS.isEmpty) null else trimedS.toInt
  }
  override val typeName = "Integer"
  val dataType          = IntegerType
}

private[smv] case class LongTypeFormat(override val format: String = null)
    extends NumericTypeFormat {
  override def strToVal(s: String): Any = {
    val trimedS = trim(s)
    if (trimedS.isEmpty) null else trimedS.toLong
  }
  override val typeName = "Long"
  val dataType          = LongType
}

private[smv] case class BooleanTypeFormat(override val format: String = null) extends TypeFormat {
  override def strToVal(s: String): Any = if (s.isEmpty) null else s.toBoolean
  override val typeName                 = "Boolean"
  val dataType                          = BooleanType
}

private[smv] case class StringTypeFormat(override val format: String = null,
                                         val nullValue: String = "")
    extends TypeFormat {
  override def strToVal(s: String): Any = if (s == nullValue) null else s
  override def valToStr(v: Any): String = if (v == null) nullValue else v.toString
  override def toString =
    if (format == null && nullValue == "") typeName
    else if (format == null) s"${typeName}[,${nullValue}]"
    else if (nullValue == "") s"${typeName}[${format}]"
    else s"${typeName}[${format}, ${nullValue}]"

  override val typeName = "String"
  val dataType          = StringType
}

private[smv] case class TimestampTypeFormat(override val format: String = "yyyy-MM-dd hh:mm:ss.S")
    extends TypeFormat {
  // `SimpleDateFormat` is not thread-safe.
  val fmtObj = SmvSchema.threadLocalDateFormat(format).get()
  //val fmtObj = new java.text.SimpleDateFormat(fmt)
  override def strToVal(s: String): Any = {
    if (s.isEmpty) null
    else new java.sql.Timestamp(fmtObj.parse(s).getTime())
  }
  override val typeName = "Timestamp"
  val dataType          = TimestampType
}

private[smv] case class DateTypeFormat(override val format: String = "yyyy-MM-dd")
    extends TypeFormat {

  val fmtObj = SmvSchema.threadLocalDateFormat(format).get()

  override def strToVal(s: String): Any = {
    if (s.isEmpty) null
    else new Date(fmtObj.parse(s).getTime())
  }

  override def valToStr(v: Any): String = {
    if (v == null) ""
    else fmtObj.format(v)
  }

  override val typeName = "Date"
  val dataType          = DateType
}

private[smv] case class DecimalTypeFormat(val precision: Integer,
                                          val scale: Integer,
                                          override val format: String = null)
    extends NumericTypeFormat {
  override val typeName = "Decimal"
  override def toString = s"Decimal[$precision,$scale]"
  val dataType          = DecimalType(precision, scale)

  override def strToVal(s: String): Any = {
    val trimedS = trim(s)
    if (trimedS.isEmpty) null else Decimal(trimedS)
  }
}

// TODO: map entries delimiter hardcoded to "|" for now.
// TODO: not worrying about key/val values containing the delimiter for now.
// TODO: only allow basic types to avoid creating a full parser for the sub-types.
private[smv] case class MapTypeFormat(keyTypeFormat: TypeFormat, valTypeFormat: TypeFormat)
    extends TypeFormat {
  override val typeName = "Map"

  override def toString = s"Map[${keyTypeFormat.toString},${valTypeFormat.toString}]"

  val dataType = MapType(keyTypeFormat.dataType, valTypeFormat.dataType)
  override def strToVal(s: String): Any = {
    if (s.isEmpty)
      null
    else
      // TODO: should probably use openCSV to parse the map to array
      s.split("\\|", -1)
        .sliding(2, 2)
        .map { a =>
          (keyTypeFormat.strToVal(a(0)), valTypeFormat.strToVal(a(1)))
        }
        .toMap
  }
  override def valToStr(v: Any): String = {
    if (v == null) return ""
    val m = v.asInstanceOf[Map[Any, Any]]
    m.map {
        case (k, v) =>
          val keyAsStr = keyTypeFormat.valToStr(k)
          val valAsStr = valTypeFormat.valToStr(v)
          s"${keyAsStr}|${valAsStr}"
      }
      .mkString("|")
  }
}

private[smv] case class ArrayTypeFormat(valTypeFormat: TypeFormat) extends TypeFormat {
  override val typeName = "Array"

  override def toString = s"Array[${valTypeFormat.toString}]"

  val dataType = ArrayType(valTypeFormat.dataType)
  override def strToVal(s: String): Any = {
    if (s.isEmpty)
      null
    else
      // TODO: should probably use openCSV to parse the map to array
      s.split("\\|", -1).map { a =>
        valTypeFormat.strToVal(a)
      }
  }
  override def valToStr(v: Any): String = {
    if (v == null) return ""
    val m = v match {
      case a: Seq[Any] => a
      case a: Array[_] => a.toSeq
    }
    m.map { r =>
        valTypeFormat.valToStr(r)
      }
      .mkString("|")
  }
}

private[smv] object TypeFormat {
  private final val StringPattern2Arg   = "[sS]tring\\[ *(\\d*) *, *(\\w+) *\\]".r
  private final val StringPattern1Arg   = "[sS]tring\\[ *(\\d+) *\\]".r
  private final val StringPattern0Arg   = "[sS]tring".r
  private final val DoublePattern       = "[dD]ouble".r
  private final val FloatPattern        = "[fF]loat".r
  private final val LongPattern         = "[lL]ong".r
  private final val IntegerPattern      = "[iI]nteger".r
  private final val BooleanPattern      = "[bB]oolean".r
  private final val TimestampPatternFmt = "[tT]imestamp\\[(.+)\\]".r
  private final val TimestampPattern    = "[tT]imestamp".r
  private final val DatePatternFmt      = "[dD]ate\\[(.+)\\]".r
  private final val DatePattern         = "[dD]ate".r
  private final val DecimalPattern2Arg  = "[dD]ecimal\\[ *(\\d+) *, *(\\d+) *\\]".r
  private final val DecimalPattern1Arg  = "[dD]ecimal\\[ *(\\d+) *\\]".r
  private final val DecimalPattern0Arg  = "[dD]ecimal".r
  private final val MapPattern          = "[mM]ap\\[(.+),(.+)\\]".r
  private final val ArrayPattern        = "[aA]rray\\[(.+)\\]".r

  def apply(typeStr: String): TypeFormat = {
    typeStr.trim match {
      case StringPattern2Arg(fStr, nStr) => StringTypeFormat(fStr, nStr)
      case StringPattern1Arg(fStr)       => StringTypeFormat(fStr)
      case StringPattern0Arg()           => StringTypeFormat()

      case DoublePattern()          => DoubleTypeFormat()
      case FloatPattern()           => FloatTypeFormat()
      case LongPattern()            => LongTypeFormat()
      case IntegerPattern()         => IntegerTypeFormat()
      case BooleanPattern()         => BooleanTypeFormat()
      case TimestampPattern()       => TimestampTypeFormat()
      case TimestampPatternFmt(fmt) => TimestampTypeFormat(fmt)
      case DatePattern()            => DateTypeFormat()
      case DatePatternFmt(fmt)      => DateTypeFormat(fmt)

      case DecimalPattern2Arg(pStr, sStr) => DecimalTypeFormat(pStr.toInt, sStr.toInt)
      case DecimalPattern1Arg(pStr)       => DecimalTypeFormat(pStr.toInt, 0)
      case DecimalPattern0Arg()           => DecimalTypeFormat(10, 0)

      case MapPattern(keyTypeStr, valTypeStr) =>
        MapTypeFormat(TypeFormat(keyTypeStr), TypeFormat(valTypeStr))
      case ArrayPattern(valTypeStr) =>
        ArrayTypeFormat(TypeFormat(valTypeStr))
      case _ => throw new SmvUnsupportedType(s"unknown type: $typeStr")
    }
  }

  def apply(dataType: DataType, strNullValue: String): TypeFormat = {
    dataType match {
      case StringType      => StringTypeFormat(nullValue = strNullValue)
      case DoubleType      => DoubleTypeFormat()
      case FloatType       => FloatTypeFormat()
      case LongType        => LongTypeFormat()
      case IntegerType     => IntegerTypeFormat()
      case BooleanType     => BooleanTypeFormat()
      case TimestampType   => TimestampTypeFormat()
      case DateType        => DateTypeFormat()
      case dt: DecimalType => DecimalTypeFormat(dt.precision, dt.scale)
      case MapType(keyType, valType, _) =>
        MapTypeFormat(TypeFormat(keyType, strNullValue), TypeFormat(valType, strNullValue))
      case ArrayType(valType, _) =>
        ArrayTypeFormat(TypeFormat(valType, strNullValue))
      case _ => throw new SmvUnsupportedType(s"unknown type: ${dataType.toString}")
    }
  }

  def apply(structField: StructField, strNullValue: String): TypeFormat = {
    TypeFormat(structField.dataType, strNullValue)
  }

}

object SchemaEntry {
  def apply(structField: StructField, strNullValue: String): SchemaEntry = {
    val typeFmt = TypeFormat(structField, strNullValue)
    SchemaEntry(structField, typeFmt)
  }

  def apply(name: String, typeFmt: TypeFormat): SchemaEntry = {
    val field = StructField(name, typeFmt.dataType, true)
    SchemaEntry(field, typeFmt)
  }

  def apply(name: String, typeFmtStr: String, meta: String = null): SchemaEntry = {
    val typeFmt  = TypeFormat(typeFmtStr)
    val metaData = if (meta == null) Metadata.empty else Metadata.fromJson(meta)
    val field    = StructField(name, typeFmt.dataType, true, metaData)
    SchemaEntry(field, typeFmt)
  }

  def apply(nameAndType: String): SchemaEntry = {
    // *? is for non-greedy match
    val parseNT = """\s*([^:]*?)\s*:\s*([^@]*?)\s*(@metadata=(.*))?""".r
    nameAndType match {
      case parseNT(n, t, dummy, meta) => SchemaEntry(n, t, meta)
      case _                          => throw new SmvRuntimeException(s"Illegal SchemaEmtry string: $nameAndType")
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
  def valueToColumnName(value: Any): String = {
    if (value == null) "null"
    else "[^a-zA-Z0-9]+".r.replaceAllIn(value.toString, "_").stripPrefix("_").stripSuffix("_")
  }
}

/**
 * CSV file schema definition.
 * This class should only be used for parsing/persisting the schema file associated with a CSV file.
 * It is no longer needed as a general purpose schema definition as spark now has that covered.
 * @param entries sequence of SchemaEntry.  One for each field in the csv file.
 * @param attributes sequence of arbitrary key/value mappings (usually contains CSV file attributes such as headers, separator, etc.)
 */
class SmvSchema(val entries: Seq[SchemaEntry], val attributes: Map[String, String])
    extends Serializable {
  private[smv] def getSize = entries.size

  private[smv] def toValue(ordinal: Int, sVal: String) = entries(ordinal).typeFormat.strToVal(sVal)

  override def toString = "Schema: " + entries.mkString("; ")

  /**
   * convert SmvSchema to StructType
   **/
  @DeveloperApi
  def toStructType: StructType = StructType(entries.map(se => se.field))

  /** get representation of this scheam as a sequence of strings that encode attributes and entries */
  private[smv] def toStringsWithMeta: Seq[String] = {
    val attStrings = attributes.toSeq.sortBy(_._1).map { case (k, v) => s"@${k} = ${v}" }

    val entStrings = entries.map { e =>
      e.toStringWithMeta
    }

    attStrings ++ entStrings
  }

  def saveToFile(sc: SparkContext, path: String) {
    sc.makeRDD(toStringsWithMeta, 1).saveAsTextFile(path)
  }

  def saveToHDFSFile(path: String) {
    SmvHDFS.writeToFile(toStringsWithMeta.mkString("\n"), path)
  }

  def saveToLocalFile(path: String) {
    SmvReportIO.saveLocalReport(toStringsWithMeta.mkString("\n"), path)
  }

  /**
   * convert a data row to a delimited csv string.
   * Handles delimiter and quote character appearing anywhere in string value.
   */
  private[smv] def rowToCsvString(row: Row, ca: CsvAttributes) = {
    require(row.size == entries.size)

    val quote                 = ca.quotechar.toString
    val doubleQuote           = quote + quote
    val regexOfEscapeAndQuote = "([" + "\\\\" + quote + "])"

    // If we are encoding for the Excel CSV format, double up all the double quotes for
    //    strings - that could have double quotes inside them
    //    maps and arrays which could have strings and hence double quotes as well
    //    other types, such as numbers will not have quotes, so no need to escape them
    // If we are not encoding this format, escape all the quote and escape chars
    // for strings, maps, and arrays with an escape character
    row.toSeq
      .zip(entries)
      .map {
        case (v, s) =>
          val se = s.typeFormat
          val quoted = if (ca.isExcelCSV) {
            quote + se.valToStr(v).replace(quote, doubleQuote) + quote
          } else {
            quote + se.valToStr(v).replaceAll(regexOfEscapeAndQuote, "\\\\$1") + quote
          }

          se match {
            case StringTypeFormat(_, _) | MapTypeFormat(_, _) | ArrayTypeFormat(_) => quoted
            case _                                                                 => se.valToStr(v)
          }
      }
      .mkString(ca.delimiter.toString)
  }

  /**
   * Extract the CSV attributes from the schema file.
   * The attributes can be defined as follows:
   * {{{
   * @has-header = true
   * @delimiter = |
   * @quote-char = "
   * }}}
   * "has-header", "delimiter", and "quotechar" are all optional and will default to (true, ",", '"') respectively.
   */
  private[smv] def extractCsvAttributes() = {
    def strToChar(s: String): Char = {
      s match {
        case "\\t" => '\t' // map \t to tab
        case "\\0" => '\0' // map \0 to null
        case x     => x(0)
      }
    }

    val delimiter = strToChar(attributes.getOrElse("delimiter", ","))
    val quotechar = strToChar(attributes.getOrElse("quote-char", "\""))
    val hasHeader = Try(attributes("has-header").toBoolean).getOrElse(true)

    CsvAttributes(delimiter, quotechar, hasHeader)
  }

  /**
   * Create a new `SmvSchema` object with the given CSV attributes.
   * @param csvAttributes
   * @return
   */
  private[smv] def addCsvAttributes(csvAttributes: CsvAttributes): SmvSchema = {
    def charToStr(c: Char): String = {
      c match {
        case '\t' => "\\t"
        case '\0' => "\\0"
        case x    => x.toString
      }
    }

    val aMap = Map(
      "has-header" -> csvAttributes.hasHeader.toString,
      "delimiter"  -> charToStr(csvAttributes.delimiter),
      "quote-char" -> charToStr(csvAttributes.quotechar)
    )

    new SmvSchema(entries, attributes ++ aMap)
  }
}

object SmvSchema {

  /**
   * creates a schema object from an array of raw schema entry/attribute strings.
   * Remove comments, empty lines and create the schema entry object
   * from each line.
   */
  private def schemaFromEntryStrings(schemaFileLines: Array[String]) = {
    // split the schema file lines into attributes (start with @) and general entries (after filtering out comments, ";", empty lines)
    val (strAtts, strEntries) = schemaFileLines.toList
      .map(_.replaceFirst("(//|#).*", ""))
      .filterNot(_.matches("^[ \t]*$"))
      .map(_.replaceFirst(";[ \t]*$", ""))
      .map(_.trim)
      .partition(_.startsWith("@"))

    // convert seq of "@k1 = v1; ..." into a Map(k1->v1, ...)
    val attMap = strAtts.map(_.stripPrefix("@").split("=")).map(x => x(0).trim -> x(1).trim).toMap

    new SmvSchema(
      strEntries.map(SchemaEntry(_)),
      attMap
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

  def fromDataFrame(df: DataFrame, strNullValue: String = "") = {
    new SmvSchema(
      df.schema.fields.map { a =>
        SchemaEntry(a, strNullValue)
      },
      Map.empty
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
    sc.textFile(path)
      .collect
      .map { s =>
        try {
          Some(s.replaceFirst(".*#.*\\$([0-9]+)[ \t]*$", "$1").toInt)
        } catch {
          case _: Throwable => None
        }
      }
      .collect { case Some(c) => c }
  }

  /**
   * map the data file path to schema path.
   * Ignores ".gz", ".csv", ".tsv" extensions when constructions schema file path.
   * For example: "/a/b/foo.csv" --> "/a/b/foo.schema".  Makes for cleaner mapping.
   */
  def dataPathToSchemaPath(dataPath: String): String = {
    // remove all known data file extensions from path.
    val exts          = List("gz", "csv", "tsv").map("\\." + _ + "$")
    val dataPathNoExt = exts.foldLeft(dataPath)((s, e) => s.replaceFirst(e, ""))

    dataPathNoExt + ".schema"
  }

  // `SimpleDateFormat` is not thread-safe.
  private[smv] val threadLocalDateFormat = { fmt: String =>
    new ThreadLocal[DateFormat] {
      override def initialValue() = {
        new SimpleDateFormat(fmt)
      }
    }
  }
}
