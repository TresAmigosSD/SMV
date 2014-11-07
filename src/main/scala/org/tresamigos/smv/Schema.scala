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
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.expressions.{Literal, Row, AttributeReference}
import org.apache.spark.sql.SchemaRDD

import scala.annotation.switch

abstract class SchemaEntry extends java.io.Serializable {
  val structField: StructField
  val zeroVal: Literal
  def strToVal(s: String) : Any
  def valToStr(v: Any) : String = if (v==null) "" else v.toString
  val typeName: String
  override def toString = structField.name + ": " + typeName
}

case class DoubleSchemaEntry(name: String) extends SchemaEntry {
  override val zeroVal = Literal(0.0)
  override def strToVal(s:String) : Any = if (s.isEmpty) null else s.toDouble
  override val typeName = "Double"
  val structField = StructField(name, DoubleType, true)
}

case class FloatSchemaEntry(name: String) extends SchemaEntry {
  override val zeroVal = Literal(0.0f)
  override def strToVal(s:String) : Any = if (s.isEmpty) null else s.toFloat
  override val typeName = "Float"
  val structField = StructField(name, FloatType, true)
}

case class IntegerSchemaEntry(name: String) extends SchemaEntry {
  override val zeroVal = Literal(0)
  override def strToVal(s:String) : Any = if (s.isEmpty) null else s.toInt
  override val typeName = "Integer"
  val structField = StructField(name, IntegerType, true)
}

case class LongSchemaEntry(name: String) extends SchemaEntry {
  override val zeroVal = Literal(0l)
  override def strToVal(s:String) : Any = if (s.isEmpty) null else s.toLong
  override val typeName = "Long"
  val structField = StructField(name, LongType, true)
}

case class BooleanSchemaEntry(name: String) extends SchemaEntry {
  override val zeroVal = Literal(false)
  override def strToVal(s:String) : Any = if (s.isEmpty) null else s.toBoolean
  override val typeName = "Boolean"
  val structField = StructField(name, BooleanType, true)
}

case class StringSchemaEntry(name: String) extends SchemaEntry {
  override val zeroVal = Literal("")
  override def strToVal(s:String) : Any = if (s.isEmpty) null else s
  override val typeName = "String"
  val structField = StructField(name, StringType, true)
}

case class TimestampSchemaEntry(name: String, fmt: String = "yyyyMMdd") extends SchemaEntry {
  override val zeroVal = Literal("") // TODO: should pick an epoch date instead.
  // @transient val fmtObj = new java.text.SimpleDateFormat(fmt)
  val fmtObj = new java.text.SimpleDateFormat(fmt)
  override def strToVal(s:String) : Any = {
    new java.sql.Timestamp(fmtObj.parse(s).getTime())
  }
  override val typeName = "Timestamp"
  val structField = StructField(name, TimestampType, true)
  override def toString = s"$name: $typeName[$fmt]"
}

// TODO: map entries delimiter hardcoded to "|" for now.
// TODO: not worrying about key/val values containing the delimiter for now.
// TODO: only allow basic types to avoid creating a full parser for the sub-types.
case class MapSchemaEntry(name: String,
      keySchemaEntry: SchemaEntry, valSchemaEntry: SchemaEntry) extends SchemaEntry {
  override val zeroVal = Literal(null)
  override val typeName = "Map"
  val structField = StructField(name, MapType(StringType, StringType), true)
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
    val keyNativeType = keySchemaEntry.structField.dataType.asInstanceOf[NativeType]
    val valNativeType = valSchemaEntry.structField.dataType.asInstanceOf[NativeType]
    val m = v.asInstanceOf[Map[Any,Any]]
    m.map{ case (k,v) =>
      val keyAsStr = keySchemaEntry.valToStr(k)
      val valAsStr = valSchemaEntry.valToStr(v)
      s"${keyAsStr}|${valAsStr}"
    }.mkString("|")
  }
}

object SchemaEntry {
  private final val StringPattern = "[sS]tring".r
  private final val DoublePattern = "[dD]ouble".r
  private final val FloatPattern = "[fF]loat".r
  private final val LongPattern = "[lL]ong".r
  private final val IntegerPattern = "[iI]nteger".r
  private final val BooleanPattern = "[bB]oolean".r
  private final val TimestampPatternFmt = "[tT]imestamp\\[(.+)\\]".r
  private final val TimestampPattern = "[tT]imestamp".r
  private final val MapPattern = "[mM]ap\\[(.+),(.+)\\]".r

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
      case MapPattern(keyTypeStr, valTypeStr) =>
        MapSchemaEntry(trimName,
          SchemaEntry("keyType", keyTypeStr),
          SchemaEntry("valType", valTypeStr))
      case _ => throw new IllegalArgumentException(s"unknown type: $typeStr")
    }
  }

  def apply(nameAndType: String) : SchemaEntry = {
    val nameAndTypeArray = nameAndType.split(":")
    require(nameAndTypeArray.size == 2)
    SchemaEntry(nameAndTypeArray(0), nameAndTypeArray(1))
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

class Schema (val entries: Seq[SchemaEntry]) extends java.io.Serializable {
  def getSize = entries.size

  def toValue(ordinal: Int, sVal: String) = entries(ordinal).strToVal(sVal)

  override def toString = "Schema: " + entries.mkString("; ")

  def toStructType : StructType = StructType(entries.map(se => se.structField))

  def ++(that: Schema): Schema = {
    new Schema(entries ++ that.entries)
  }

  def findEntry(sym: Symbol) = {
    entries.find(e => e.structField.name == sym.name)
  }
  def saveToFile(sc: SparkContext, path: String) {
    sc.makeRDD(entries.map(_.toString), 1).saveAsTextFile(path)
  }

  /**
   * convert a data row to a delimited csv string.
   * Handles delimiter and quote character appearing anywhere in string value.
   */
  def rowToCsvString(row: Row)(implicit ca: CsvAttributes) = {
    require(row.size == entries.size)
    val sb = new StringBuilder

    // TODO: should be done using zip,map and mkstring rather than index.
    for (idx <- 0 until row.length) {
      // prepend delimiter except for first item.
      if (idx > 0)
        sb.append(ca.delimiter)

      val se = entries(idx)
      (se.structField.dataType: @switch) match {
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

object Schema {

  /**
   * creates a schema object from an array of raw schema entry strings.
   * Remove comments, empty lines and create the schema entry object
   * from each line.
   */
  private def schemaFromEntryStrings(strEntries : Array[String]) = {
    new Schema(
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

  def fromSchemaRDD(schemaRDD: SchemaRDD) = {
    import org.apache.spark.contrib.smv._
    import org.apache.spark.sql.catalyst.analysis.SimpleAnalyzer
    val lp = extractLogicalPlan(schemaRDD)
    val resolvedLP = SimpleAnalyzer(lp)
    // Convert a sequence of attributes from the resolved logical plan
    // into a sequence of schema entries.  We drop the last 4 characters
    // from the attribute type because the attribute data type is of the
    // form DoubleType, IntType, XType (we want to drop the "Type")
    new Schema(
      resolvedLP.output.map(a =>
        SchemaEntry(a.name, a.dataType.toString.dropRight(4))))
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
}
