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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{JoinType, Inner}

class SchemaRDDHelper(schemaRDD: SchemaRDD) {

  private[smv] var schemaWithMeta: Schema = null

  // TODO: add schema file path as well.
  def saveAsCsvWithSchema(dataPath: String)(implicit ca: CsvAttributes) {

    val schema = if (schemaWithMeta == null) {Schema.fromSchemaRDD(schemaRDD)} else {schemaWithMeta}

    //Adding the header to the saved file all the time even when ca.hasHeader is
    //False.
    val fieldNames = schemaRDD.schema.fieldNames
    val headerStr = fieldNames.map(_.trim).map(fn => "\"" + fn + "\"").
      mkString(ca.delimiter.toString)

    val csvHeaderRDD = schemaRDD.sparkContext.parallelize(Array(headerStr),1)
    val csvBodyRDD = schemaRDD.map(schema.rowToCsvString(_))

    //As far as I know the union maintain the order. So the header will end up being the
    //first line in the saved file.
    val csvRDD = csvHeaderRDD.union(csvBodyRDD)

    schema.saveToFile(schemaRDD.context, Schema.dataPathToSchemaPath(dataPath))
    csvRDD.saveAsTextFile(dataPath)
  }

  /**
   * Dump the schema and data of given srdd to screen for debugging purposes.
   * TODO: add debug flag to turn on/off this method.  Hmm, I think adding a flag would encourage people to leave this in code :-)
   */
  def dumpSRDD = {
    println(Schema.fromSchemaRDD(schemaRDD))
    schemaRDD.collect.foreach(println)
  }

  /**
   * selects all the current columns in current SRDD plus the supplied expressions.
   */
  def selectPlus(exprs: Expression*): SchemaRDD = {
    val all = schemaRDD.schema.fieldNames.map{l=>schemaRDD.sqlContext.symbolToUnresolvedAttribute(Symbol(l))}
    schemaRDD.select( all ++ exprs : _* )
  }

  /**
   * Same as selectPlus but the new columns are prepended to result.
   */
  def selectPlusPrefix(exprs: Expression*): SchemaRDD = {
    val all = schemaRDD.schema.fieldNames.map{l=>schemaRDD.sqlContext.symbolToUnresolvedAttribute(Symbol(l))}
    schemaRDD.select( exprs ++ all : _* )
  }

  def selectMinus(symb: Symbol*): SchemaRDD = {
    val all = schemaRDD.schema.fieldNames.map{l=>Symbol(l)} diff symb
    val allExprs = all.map{l=>schemaRDD.sqlContext.symbolToUnresolvedAttribute(l)}
    schemaRDD.select(allExprs : _* )
  }

  def renameField(namePairs: (Symbol,Symbol)*): SchemaRDD = {
    import schemaRDD.sqlContext._

    val namePairsMap = namePairs.toMap
    val renamedFields = schemaRDD.schema.fieldNames.map {
      fn => Symbol(fn) as namePairsMap.getOrElse(Symbol(fn), Symbol(fn))
    }
    schemaRDD.select(renamedFields: _*)
  }

  def addMeta(metaPairs: (Symbol, String)*): SchemaRDDHelper = {
    if (schemaWithMeta == null) schemaWithMeta = Schema.fromSchemaRDD(schemaRDD)
    metaPairs.foreach{case (v, m) => schemaWithMeta.addMeta(v, m)}
    this
  }

  def renameWithMeta(nameMetaPairs: (Symbol, (Symbol, String))*): SchemaRDDHelper = {
    val namePairs = nameMetaPairs.map{case (orig, (dest, meta)) => (orig, dest)}
    val metaPairs = nameMetaPairs.map{case (orig, (dest, meta)) => (dest, meta)}

    renameField(namePairs: _*).addMeta(metaPairs: _*)
  }

  def prefixFieldNames(prefix: String) : SchemaRDD = {
    import schemaRDD.sqlContext._
    val renamedFields = schemaRDD.schema.fieldNames.map {
      fn => Symbol(fn) as Symbol(prefix + fn)
    }
    schemaRDD.select(renamedFields: _*)
  }

  def postfixFieldNames(postfix: String) : SchemaRDD = {
    import schemaRDD.sqlContext._
    val renamedFields = schemaRDD.schema.fieldNames.map {
      fn => Symbol(fn) as Symbol(fn + postfix)
    }
    schemaRDD.select(renamedFields: _*)
  }

  def joinUniqFieldNames(otherPlan: SchemaRDD, joinType: JoinType = Inner, on: Option[Expression] = None) : SchemaRDD = {
    val namesL = schemaRDD.schema.fieldNames.toSet
    val namesR = otherPlan.schema.fieldNames.toSet

    val dup = (namesL & namesR).toSeq
    val renamedFields = dup.map{l => (Symbol(l) -> Symbol("_" + l))}

    schemaRDD.join(otherPlan.renameField(renamedFields: _*), joinType, on)
  }

  def joinByKey(otherPlan: SchemaRDD, joinType: JoinType, keys: Seq[Symbol]): SchemaRDD = {
    import schemaRDD.sqlContext._

    val rightKeys = keys.map{k => Symbol("_" + k.name)}
    val renamedFields = keys.zip(rightKeys).map{case (l,r) => (l -> r)}
    val joinOpt = keys.zip(rightKeys).map{case (l, r) => (l === r):Expression}.reduce(_ && _)

    schemaRDD.joinUniqFieldNames(otherPlan.renameField(renamedFields: _*), joinType, Option(joinOpt)).selectMinus(rightKeys: _*)
  }

  def dedupByKey(keys: Symbol*) : SchemaRDD = {
    import schemaRDD.sqlContext._

    val selectExpressions = schemaRDD.schema.fieldNames.map {
      fn => First(Symbol(fn)) as Symbol(fn)
    }

    val allKeys = keys.map { k=>schemaRDD.sqlContext.symbolToUnresolvedAttribute(k) }

    schemaRDD.groupBy(allKeys: _*)(selectExpressions: _*)
  }

  /** See PivotOp class for documentation */
  def pivot_sum(keyCol: Symbol, pivotCols: Seq[Symbol], valueCols: Seq[Symbol]) = {
    new PivotOp(schemaRDD, keyCol, pivotCols, valueCols).transform
  }

  def pivot_sum(keyCols: Symbol*)(pivotCols: Symbol*)(valueCols: Symbol*) = {
    new PivotOp(schemaRDD, keyCols, pivotCols, valueCols).transform
  }

  def smvUnpivot(valueCols: Seq[Symbol]) = {
    new UnpivotOp(schemaRDD, valueCols).unpivot()
  }

  /** See RollupCubeOp for details. */
  def smvCube(cols: Symbol*)(groupExprs: Expression*) = {
    new RollupCubeOp(schemaRDD, cols, Seq.empty, groupExprs).cube()
  }
  def smvCubeFixed(cols: Symbol*)(fixedCols: Symbol*)(groupExprs: Expression*) = {
    new RollupCubeOp(schemaRDD, cols, fixedCols, groupExprs).cube()
  }

  /** See RollupCubeOp for details. */
  def smvRollup(cols: Symbol*)(groupExprs: Expression*) = {
    new RollupCubeOp(schemaRDD, cols, Seq.empty, groupExprs).rollup()
  }
  def smvRollupFixed(cols: Symbol*)(fixedCols: Symbol*)(groupExprs: Expression*) = {
    new RollupCubeOp(schemaRDD, cols, fixedCols, groupExprs).rollup()
  }

  /** See QuantileOp for details. */
  def smvQuantile(groupCols: Seq[Symbol], keyCol: Symbol, valueCol: Symbol, numBins: Integer) = {
    new QuantileOp(schemaRDD, groupCols, keyCol, valueCol, numBins).quantile()
  }
  def smvDecile(groupCols: Seq[Symbol], keyCol: Symbol, valueCol: Symbol) = {
    new QuantileOp(schemaRDD, groupCols, keyCol, valueCol, 10).quantile()
  }

  /** adds a rank column to an srdd. */
  def smvRank(rankColumnName: String, startValue: Long = 0) = {
    val oldSchema = Schema.fromSchemaRDD(schemaRDD)
    val newSchema = oldSchema ++ new Schema(Seq(LongSchemaEntry(rankColumnName)))

    val res: RDD[Row] = schemaRDD.
      zipWithIndex().
      map{ case (row, idx) =>
        new GenericRow(Array[Any](row ++ Seq(idx + startValue): _*)) }

    schemaRDD.sqlContext.applySchemaToRowRDD(res, newSchema)
  }

  /**
   * Create an EDD builder on SchemaRDD 
   * 
   * @param groupingExprs specify grouping expression(s) to compute EDD over
   * @return an EDD object 
   */
  def groupEdd(groupingExprs : Expression*): EDD = {
    EDD(schemaRDD, groupingExprs)
  }

  /**
   * Create an EDD builder on SchemaRDD population
   */
  def edd: EDD = groupEdd()

  def dqm(keepReject: Boolean = false): DQM = DQM(schemaRDD, keepReject)

  /**
   * chunkBy and chunkByPlus apply user defined functions to a group of
   * records and out put a group of records
   *
   * @param keys specify the group key(s) to apply the UDF over
   * @param func is an SmvChunkFunc object which defines the input, output and
   * the UDF itself.
   * 
   * The chunkBy version will only output the keys and the columns output
   * from the UDF, while the chunckByPlus version add the UDF output columns
   * in addition to the input SchemaRDD columns
   *
   * The SmvChunkFunc interface:
   * 
   * SmvChunkFunc(para: Seq[Symbol], outSchema: Schema, eval: List[Seq[Any]] => List[Seq[Any]])
   *
   * @param para specify the columns in the SchemaRDD which will be used in
   * the UDF
   * @param outSchema is a SmvSchema object which specify how the out SRDD
   * will interpret the UDF generated columns
   * @param eval is a Scala function which does teh real work. It will refer
   * the input columns by their indexs as ordered in the para
   * 
   * Example:
   *   val srdd=sqlContext.createSchemaRdd("k:String; v:String", "z,1;a,3;a,2;z,8;")   
   *   val runCat = (l: List[Seq[Any]]) => l.map{_(0)}.scanLeft(Seq("")){(a,b) => Seq(a(0) + b)}.tail
   *   val runCatFunc = SmvChunkFunc(Seq('v), Schema.fromString("vcat:String"), runCat)
   *   val res = srdd.orderBy('k.asc, 'v.asc).chunkBy('k)(runCatFunc)
   *
   * res of above code is 
   *   Schema: k: String; vcat: String
   *   [a,2]
   *   [a,23]
   *   [z,1]
   *   [z,18]
   * 
   */
  def chunkByPlus(keys: Symbol*)(func: SmvChunkFunc): SchemaRDD = {
    val smvChunk = new SmvChunk(schemaRDD, keys)
    smvChunk.applyUDF(func, true)
  }

  def chunkBy(keys: Symbol*)(func: SmvChunkFunc): SchemaRDD = {
    val smvChunk = new SmvChunk(schemaRDD, keys)
    smvChunk.applyUDF(func, false)
  }

}
