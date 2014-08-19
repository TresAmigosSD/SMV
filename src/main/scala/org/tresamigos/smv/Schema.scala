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
import org.apache.spark.sql.catalyst.expressions.{Row, AttributeReference}
import org.apache.spark.sql.SchemaRDD

import scala.annotation.switch

//TODO: add support for other builtin types

abstract class SchemaEntry extends java.io.Serializable {
  def name: String
  def strToVal(s: String) : Any
  def valToStr(v: Any) : String = v.toString
  val typeName: String
  val dataType: DataType
  override def toString = name + ": " + typeName
}

case class DoubleSchemaEntry(name: String) extends SchemaEntry {
  override def strToVal(s:String) : Any = if (s.isEmpty) null else s.toDouble
  override val typeName = "Double"
  override val dataType = DoubleType
}

case class FloatSchemaEntry(name: String) extends SchemaEntry {
  override def strToVal(s:String) : Any = if (s.isEmpty) null else s.toFloat
  override val typeName = "Float"
  override val dataType = FloatType
}

case class IntegerSchemaEntry(name: String) extends SchemaEntry {
  override def strToVal(s:String) : Any = if (s.isEmpty) null else s.toInt
  override val typeName = "Integer"
  override val dataType = IntegerType
}

case class LongSchemaEntry(name: String) extends SchemaEntry {
  override def strToVal(s:String) : Any = if (s.isEmpty) null else s.toLong
  override val typeName = "Long"
  override val dataType = LongType
}

case class BooleanSchemaEntry(name: String) extends SchemaEntry {
  override def strToVal(s:String) : Any = if (s.isEmpty) null else s.toBoolean
  override val typeName = "Boolean"
  override val dataType = BooleanType
}

case class StringSchemaEntry(name: String) extends SchemaEntry {
  override def strToVal(s:String) : Any = if (s.isEmpty) null else s
  override val typeName = "String"
  override val dataType = StringType
}

case class TimestampSchemaEntry(name: String, fmt: String = "yyyyMMdd") extends SchemaEntry {
  // @transient val fmtObj = new java.text.SimpleDateFormat(fmt)
  val fmtObj = new java.text.SimpleDateFormat(fmt)
  override def strToVal(s:String) : Any = {
    new java.sql.Timestamp(fmtObj.parse(s).getTime())
  }
  override val typeName = "Timestamp"
  override val dataType = TimestampType
  override def toString = s"$name: $typeName[$fmt]"
}

// TODO: for now assume Map is String -> String map.  Will add parameterized type later.
// TODO: map entries delimiter hardcoded to "|" for now.
// TODO: not worrying about key/val values containing the delimiter for now.
// TODO: only allow basic types to avoid creating a full parser for the sub-types.
case class MapSchemaEntry(name: String,
      keySchemaType: SchemaEntry, valSchemaEntry: SchemaEntry) extends SchemaEntry {
  override val typeName = "Map"
  override val dataType = MapType(StringType, StringType)
  override def strToVal(s: String) : Any = {
    if (s.isEmpty)
      null
    else
      // TODO: should probably use openCSV to parse the map to array
      s.split("\\|").sliding(2,2).map(a => (a(0),a(1))).toMap
  }
  override def valToStr(v: Any) : String = {
    val m = v.asInstanceOf[Map[String,String]]
    m.map{ case (a,b) => s"$a|$b" }.mkString("|")
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
  // TODO: add K,V types to map type
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
}

class Schema (val entries: Seq[SchemaEntry]) extends java.io.Serializable {
  def getSize = entries.size

  def toValue(ordinal: Int, sVal: String) = entries(ordinal).strToVal(sVal)

  def colNames = entries.map(_.name)
  def colTypes = entries.map(_.dataType)
  def colTypeNames = entries.map(_.typeName)
 
  private val nameTypeMap: Map[Symbol, DataType] = (colNames.map(Symbol(_)) zip colTypes).toMap
  private val typeNameMap: Map[String, List[Symbol]] = 
    (colTypeNames zip colNames.map(Symbol(_))).foldRight(Map[String, List[Symbol]]()){
      case ((k, v), m) => m.updated(k, v :: m.getOrElse(k,Nil))}

  def nameToType(name: Symbol): DataType = nameTypeMap.getOrElse(name, NullType)
  def typeNameToNames(typeName: String): List[Symbol] = typeNameMap.getOrElse(typeName, Nil)

  override def toString = "Schema: " + entries.mkString("; ")

  def toAttribSeq : Seq[AttributeReference] = {
    entries.map(se => AttributeReference(se.name, se.dataType)())
  }

  def saveToFile(sc: SparkContext, path: String) {
    sc.makeRDD(entries.map(_.toString), 1).saveAsTextFile(path)
  }

  /**
   * convert a data row to a delimited csv string.
   * Handles delimiter and quote character appearing any any string value.
   */
  def rowToCsvString(row: Row, delimiter: Char = ',') = {
    require(row.size == entries.size)
    val sb = new StringBuilder

    for (idx <- 0 until row.length) {
      // prepend delimiter except for first item.
      if (idx > 0)
        sb.append(delimiter)

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
}
