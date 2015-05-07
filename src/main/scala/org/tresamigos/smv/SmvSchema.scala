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
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.{Literal, Row, AttributeReference}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SchemaRDD

import org.apache.spark.sql.catalyst.plans.logical._


import scala.annotation.switch

abstract class SchemaEntry extends java.io.Serializable {
  val structField: StructField
  val zeroVal: Literal
  def strToVal(s: String) : Any
  def valToStr(v: Any) : String = if (v==null) "" else v.toString
  val typeName: String
  private[smv] var meta: String = ""
  override def toString = structField.name + ": " + typeName
}

abstract class NativeSchemaEntry extends SchemaEntry {
  private[smv] type JvmType
  private[smv] val ordering: Ordering[JvmType]
}

abstract class NumericSchemaEntry extends NativeSchemaEntry {
  private[smv] val numeric: Numeric[JvmType]
  private[smv] def trim(s: String) = s.replaceAll("""(^ +| +$)""", "")
}

case class DoubleSchemaEntry(name: String) extends NumericSchemaEntry {
  private[smv] type JvmType = Double
  private[smv] val ordering = implicitly[Ordering[JvmType]]
  private[smv] val numeric = implicitly[Numeric[Double]]
  override val zeroVal = Literal(0.0)
  override def strToVal(s:String) : Any = if (s.isEmpty) null else trim(s).toDouble
  override val typeName = "Double"
  val structField = StructField(name, DoubleType, true)
}

case class FloatSchemaEntry(name: String) extends NumericSchemaEntry {
  private[smv] type JvmType = Float
  private[smv] val ordering = implicitly[Ordering[JvmType]]
  private[smv] val numeric = implicitly[Numeric[Float]]
  override val zeroVal = Literal(0.0f)
  override def strToVal(s:String) : Any = if (s.isEmpty) null else trim(s).toFloat
  override val typeName = "Float"
  val structField = StructField(name, FloatType, true)
}

case class IntegerSchemaEntry(name: String) extends NumericSchemaEntry {
  private[smv] type JvmType = Int
  private[smv] val ordering = implicitly[Ordering[JvmType]]
  private[smv] val numeric = implicitly[Numeric[Int]]
  override val zeroVal = Literal(0)
  override def strToVal(s:String) : Any = if (s.isEmpty) null else trim(s).toInt
  override val typeName = "Integer"
  val structField = StructField(name, IntegerType, true)
}

case class LongSchemaEntry(name: String) extends NumericSchemaEntry {
  private[smv] type JvmType = Long
  private[smv] val ordering = implicitly[Ordering[JvmType]]
  private[smv] val numeric = implicitly[Numeric[Long]]
  override val zeroVal = Literal(0l)
  override def strToVal(s:String) : Any = if (s.isEmpty) null else trim(s).toLong
  override val typeName = "Long"
  val structField = StructField(name, LongType, true)
}

case class BooleanSchemaEntry(name: String) extends NativeSchemaEntry {
  private[smv] type JvmType = Boolean
  private[smv] val ordering = implicitly[Ordering[JvmType]]
  override val zeroVal = Literal(false)
  override def strToVal(s:String) : Any = if (s.isEmpty) null else s.toBoolean
  override val typeName = "Boolean"
  val structField = StructField(name, BooleanType, true)
}

case class StringSchemaEntry(name: String) extends NativeSchemaEntry {
  private[smv] type JvmType = String
  private[smv] val ordering = implicitly[Ordering[JvmType]]
  override val zeroVal = Literal("")
  override def strToVal(s:String) : Any = if (s.isEmpty) null else s
  override val typeName = "String"
  val structField = StructField(name, StringType, true)
}

case class TimestampSchemaEntry(name: String, fmt: String = "yyyy-MM-dd hh:mm:ss.S") extends NativeSchemaEntry {
  /**
   * The Default format should match the default "toString" format of
   * java.sql.Timestamp
   */ 
  private[smv] type JvmType = java.sql.Timestamp
  private[smv] val ordering = new Ordering[JvmType] {
    def compare(x: java.sql.Timestamp, y: java.sql.Timestamp) = x.compareTo(y)
  }
  override val zeroVal = Literal("") // TODO: should pick an epoch date instead.
  // @transient val fmtObj = new java.text.SimpleDateFormat(fmt)
  val fmtObj = new java.text.SimpleDateFormat(fmt)
  override def strToVal(s:String) : Any = {
    if(s.isEmpty) null 
    else new java.sql.Timestamp(fmtObj.parse(s).getTime())
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

  override def toString = s"$name: Map[${keySchemaEntry.typeName},${valSchemaEntry.typeName}]"

  val structField = StructField(name, MapType(keySchemaEntry.structField.dataType, valSchemaEntry.structField.dataType), true)
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
 
case class ArraySchemaEntry(name: String, valSchemaEntry: SchemaEntry) extends SchemaEntry {
  override val zeroVal = Literal(null)
  override val typeName = "Array"

  override def toString = s"$name: Array[${valSchemaEntry.typeName}]"

  val structField = StructField(name, ArrayType(valSchemaEntry.structField.dataType), true)
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
 
object NumericSchemaEntry {
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
  def getSize = entries.size

  def toValue(ordinal: Int, sVal: String) = entries(ordinal).strToVal(sVal)

  override def toString = "Schema: " + entries.mkString("; ")

  def toStructType : StructType = StructType(entries.map(se => se.structField))

  def ++(that: SmvSchema): SmvSchema = {
    val thisNames = names.toSet
    val thatNames = that.names.toSet
    require(thisNames.intersect(thisNames).isEmpty)
    
    new SmvSchema(entries ++ that.entries)
  }
  
  def selfJoined(): SmvSchema = {
    val renamed = entries.map{e => SchemaEntry("_" + e.structField.name, e.structField.dataType)}
    new SmvSchema(entries ++ renamed)
  }
  
  def names: Seq[String] = {
    entries.map(e => e.structField.name)
  }
  
  def getIndices(keys: String*): Seq[Int] = {
    keys.map{s => names.indexWhere(s == _)}
  }

  def findEntry(s: String): Option[SchemaEntry] = {
    entries.find(e => e.structField.name == s)
  }
  def findEntry(sym: Symbol): Option[SchemaEntry] = findEntry(sym.name)

  def toLocalRelation(): LocalRelation = {
    val schemaAttr = entries.map{e =>
      val s = e.structField
      AttributeReference(s.name, s.dataType, s.nullable)()
    }
    LocalRelation(schemaAttr)
  }
  
  def addMeta(sym: Symbol, metaStr: String): SmvSchema = {
    findEntry(sym).get.meta = metaStr
    this
  }
  
  def addMeta(metaPairs: (Symbol, String)*): SmvSchema = {
    metaPairs.foreach{case (v, m) => addMeta(v, m)}
    this
  }

  def toStringWithMeta = entries.map{e => 
    e.toString + (if (e.meta.isEmpty) "" else ("\t\t# " + e.meta))
  }

  def saveToFile(sc: SparkContext, path: String) {
    sc.makeRDD(toStringWithMeta, 1).saveAsTextFile(path)
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
  
  def fromSchemaRDD(schemaRDD: SchemaRDD) = fromDataFrame(schemaRDD)

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
}
