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

import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.catalyst.expressions._

class SchemaRDDHelper(schemaRDD: SchemaRDD) {

  // TODO: add schema file path as well.
  def saveAsCsvWithSchema(dataPath: String)(implicit ca: CsvAttributes) {
    val schema = Schema.fromSchemaRDD(schemaRDD)

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
  def smvQuantile(groupCol: Symbol, keyCol: Symbol, valueCol: Symbol, numBins: Integer) = {
    new QuantileOp(schemaRDD, groupCol, keyCol, valueCol, numBins).quantile()
  }
  def smvDecile(groupCol: Symbol, keyCol: Symbol, valueCol: Symbol) = {
    new QuantileOp(schemaRDD, groupCol, keyCol, valueCol, 10).quantile()
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
}
