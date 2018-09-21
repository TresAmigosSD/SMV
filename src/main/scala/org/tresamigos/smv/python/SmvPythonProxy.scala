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
package python

import py4j.GatewayServer

import scala.collection.JavaConversions._
import java.util.ArrayList
import java.io.File

import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.sql.{Column, DataFrame, SQLContext, SparkSession}
import org.apache.spark.sql.types.DataType
import matcher._
import org.tresamigos.smv.dqm.ParserLogger

// Serialize scala map to json w/o reinventing any wheels
import org.json4s.jackson.Serialization

/** Provides access to enhanced methods on DataFrame, Column, etc */
object SmvPythonHelper {
  def peekStr(df: DataFrame, pos: Int, colRegex: String): String = df._peek(pos, colRegex)

  def smvExpandStruct(df: DataFrame, cols: Array[String]): DataFrame =
    df.smvExpandStruct(cols: _*)

  def smvGroupBy(df: DataFrame, cols: Array[Column]): SmvGroupedDataAdaptor =
    new SmvGroupedDataAdaptor(df.smvGroupBy(cols: _*))

  def smvGroupBy(df: DataFrame, cols: Array[String]): SmvGroupedDataAdaptor = {
    import df.sqlContext.implicits._
    new SmvGroupedDataAdaptor(df.smvGroupBy(cols.toSeq.map { c =>
      $"$c"
    }: _*))
  }

  def smvJoinByKey(df: DataFrame,
                   other: DataFrame,
                   keys: Seq[String],
                   joinType: String,
                   isNullSafe: Boolean): DataFrame =
    df.smvJoinByKey(other, keys, joinType, isNullSafe)

  def smvJoinMultipleByKey(df: DataFrame,
                           keys: Array[String],
                           joinType: String): SmvMultiJoinAdaptor =
    new SmvMultiJoinAdaptor(df.smvJoinMultipleByKey(keys, joinType))

  def smvSelectMinus(df: DataFrame, cols: Array[String]): DataFrame =
    df.smvSelectMinus(cols.head, cols.tail: _*)

  def smvSelectMinus(df: DataFrame, cols: Array[Column]): DataFrame =
    df.smvSelectMinus(cols: _*)

  def smvDedupByKey(df: DataFrame, keys: Array[String]): DataFrame =
    df.dedupByKey(keys.head, keys.tail: _*)

  def smvDedupByKey(df: DataFrame, cols: Array[Column]): DataFrame =
    df.dedupByKey(cols: _*)

  def smvDedupByKeyWithOrder(df: DataFrame,
                             keys: Array[String],
                             orderCol: Array[Column]): DataFrame =
    df.dedupByKeyWithOrder(keys.head, keys.tail: _*)(orderCol: _*)

  def smvDedupByKeyWithOrder(df: DataFrame,
                             cols: Array[Column],
                             orderCol: Array[Column]): DataFrame =
    df.dedupByKeyWithOrder(cols: _*)(orderCol: _*)

  def smvRenameField(df: DataFrame, namePairsAsList: ArrayList[ArrayList[String]]): DataFrame = {
    val namePairs = namePairsAsList.map(inner => Tuple2(inner(0), inner(1)))
    df.smvRenameField(namePairs: _*)
  }

  def smvConcatHist(df: DataFrame, colNames: Array[String]) = df._smvConcatHist(colNames.toSeq)

  def smvBinHist(df: DataFrame, _colWithBin: java.util.List[java.util.List[Any]]) = {
    val colWithBin = _colWithBin.map { t =>
      (t.get(0).asInstanceOf[String], t.get(1).asInstanceOf[Double])
    }.toSeq
    df._smvBinHist(colWithBin: _*)
  }

  def smvIsAllIn(col: Column, values: Any*): Column = col.smvIsAllIn(values: _*)
  def smvIsAnyIn(col: Column, values: Any*): Column = col.smvIsAnyIn(values: _*)

  //case class DiscoveredPK(pks: ArrayList[String], cnt: Long)
  def smvDiscoverPK(df: DataFrame, n: Int): (ArrayList[String], Long) = {
    val res = df.smvDiscoverPK(n, false)
    (new ArrayList(res._1), res._2)
  }

  def smvDiscoverSchemaToFile(path: String, n: Int, csvattr: CsvAttributes): Unit = {
    implicit val csvAttributes = csvattr
    val helper                 = new SchemaDiscoveryHelper(SmvApp.app.sqlContext)
    val schema                 = helper.discoverSchemaFromFile(path, n)
    val outpath                = SmvSchema.dataPathToSchemaPath(path) + ".toBeReviewed"
    val outFileName            = (new File(outpath)).getName
    schema.saveToLocalFile(outFileName)
    println(s"Discovered schema file saved as ${outFileName}, please review and make changes.")
  }

  def discoverSchemaAsSmvSchema(path: String, nsamples: Int, csvattr: CsvAttributes): SmvSchema = {
    implicit val csvAttributes = csvattr
    new SchemaDiscoveryHelper(SmvApp.app.sqlContext).discoverSchemaFromFile(path, nsamples)
  }

  /**
   * Update the port of callback client
   */
  def updatePythonGatewayPort(gws: GatewayServer, port: Int): Unit = {
    val cl = gws.getCallbackClient
    val f  = cl.getClass.getDeclaredField("port")
    f.setAccessible(true)
    f.setInt(cl, port)
  }

  def createMatcher(
      leftId: String,
      rightId: String,
      exactMatchFilter: PreFilter,
      groupCondition: AbstractGroupCondition,
      levelLogics: Array[LevelLogic]
  ): SmvEntityMatcher = {
    val lls = levelLogics.toSeq
    SmvEntityMatcher(leftId,
                     rightId,
                     if (exactMatchFilter == null) NoOpPreFilter else exactMatchFilter,
                     if (groupCondition == null) NoOpGroupCondition else groupCondition,
                     lls)
  }

  def smvOverlapCheck(df: DataFrame, key: String, otherDf: Array[DataFrame]): DataFrame =
    df.smvOverlapCheck(key)(otherDf: _*)

  def smvDesc(df: DataFrame, colDescs: ArrayList[ArrayList[String]]): DataFrame = {
    val colDescPairs = colDescs.map(inner => Tuple2(inner(0), inner(1)))
    df.smvDesc(colDescPairs: _*)
  }

  def smvRemoveDesc(df: DataFrame, colNames: Array[String]): DataFrame = {
    df.smvRemoveDesc(colNames: _*)
  }

  def smvCollectSet(col: Column, datatypeJson: String): Column = {
    val dt = DataType.fromJson(datatypeJson)
    smvfuncs.smvCollectSet(col, dt)
  }

  def smvStrCat(sep: String, cols: Array[Column]): Column = {
    smvfuncs.smvStrCat(sep, cols: _*)
  }

  def smvHashKey(prefix: String, cols: Array[Column]): Column =
    smvfuncs.smvHashKey(prefix, cols: _*)

  def getTerminateParserLogger() = 
    dqm.TerminateParserLogger
}

class SmvGroupedDataAdaptor(grouped: SmvGroupedData) {
  def smvTopNRecs(maxElems: Int, orders: Array[Column]): DataFrame =
    grouped.smvTopNRecs(maxElems, orders: _*)

  def smvPivot(pivotCols: java.util.List[Array[String]],
                  valueCols: Array[String],
                  baseOutput: Array[String]): DataFrame =
    grouped.smvPivot(pivotCols.map(_.toSeq).toSeq: _*)(valueCols: _*)(baseOutput: _*).toDF

  def smvPivotSum(pivotCols: java.util.List[Array[String]],
                  valueCols: Array[String],
                  baseOutput: Array[String]): DataFrame =
    grouped.smvPivotSum(pivotCols.map(_.toSeq).toSeq: _*)(valueCols: _*)(baseOutput: _*)

  def smvPivotCoalesce(pivotCols: java.util.List[Array[String]],
                       valueCols: Array[String],
                       baseOutput: Array[String]): DataFrame =
    grouped.smvPivotCoalesce(pivotCols.map(_.toSeq).toSeq: _*)(valueCols: _*)(baseOutput: _*)

  def toDF(): DataFrame =
    grouped.toDF

  def smvRePartition(numParts: Int): SmvGroupedDataAdaptor =
    new SmvGroupedDataAdaptor(grouped.smvRePartition(numParts))

  def smvFillNullWithPrevValue(orderCols: Array[Column], valueCols: Array[String]): DataFrame =
    grouped.smvFillNullWithPrevValue(orderCols: _*)(valueCols: _*)

  def smvWithTimePanel(timeColName: String, start: panel.PartialTime, end: panel.PartialTime) =
    grouped.smvWithTimePanel(timeColName, start, end)

  def smvTimePanelAgg(timeColName: String, start: panel.PartialTime, end: panel.PartialTime, aggCols: Array[Column]) =
    grouped.smvTimePanelAgg(timeColName, start, end)(aggCols: _*)

  def smvQuantile(valueCols: Array[String], numBins: Integer, ignoreNull: Boolean) =
    grouped.smvQuantile(valueCols.toSeq, numBins, ignoreNull)

  def smvPercentRank(valueCols: Array[String], ignoreNull: Boolean) =
    grouped.smvPercentRank(valueCols.toSeq, ignoreNull)
}

class SmvMultiJoinAdaptor(joiner: SmvMultiJoin) {
  def joinWith(df: DataFrame, postfix: String, joinType: String): SmvMultiJoinAdaptor =
    new SmvMultiJoinAdaptor(joiner.joinWith(df, postfix, joinType))

  def doJoin(dropExtra: Boolean): DataFrame = joiner.doJoin(dropExtra)
}

/**
 * Provide app-level methods for use in Python.
 *
 * All returned collection types should be declared in the method
 * signature as their Java counterparts so that they are accessible
 * through the py4j gateway.
 */
class SmvPyClient(val j_smvApp: SmvApp) {
  val config      = j_smvApp.smvConfig
  val publishHive = j_smvApp.publishHive

  def callbackServerPort: Option[Int] = config.cmdLine.cbsPort.get

  def publishVersion: Option[String] = config.cmdLine.publish.get

  def mergedPropsJSON: String = Serialization.write(j_smvApp.smvConfig.mergedProps)(org.json4s.DefaultFormats)

  def setDynamicRunConfig(runConfig: java.util.Map[String, String]): Unit = {
    val dynamicRunConfig: Map[String, String] = if (null == runConfig) Map.empty else mapAsScalaMap(runConfig).toMap
    j_smvApp.smvConfig.dynamicRunConfig = dynamicRunConfig
  }

  def setAppDir(appDir: String): Unit = {
    // set the scala smv app's app dir, will cause conf for that app to be loaded + reevaluated
    // effectively changing SMV's working app directory

    j_smvApp.smvConfig.setAppDir(appDir)
  }

  /** Output directory for files */
  def outputDir: String = j_smvApp.smvConfig.outputDir

  def stages: Array[String] = j_smvApp.stages.toArray

  def userLibs: Array[String] = j_smvApp.userLibs.toArray

  def inferDS(name: String): SmvDataSet =
    j_smvApp.dsm.inferDS(name).head

  /** Used to create small dataframes for testing */
  def dfFrom(schema: String, data: String): DataFrame =
    j_smvApp.createDF(schema, data)

  def urn2fqn(modUrn: String): String = org.tresamigos.smv.urn2fqn(modUrn)

  def getDsHash(name: String, runConfig: java.util.Map[String, String]): String =
    j_smvApp.getDsHash(name, javaMapToImmutableMap(runConfig))

  /** Runs an SmvModule written in either Python or Scala */
  def runModule(urn: String,
                forceRun: Boolean,
                version: Option[String],
                runConfig: java.util.Map[String, String],
                quickCompute: Boolean = false): RunModuleResult = {
    val dynamicRunConfig: Map[String, String] = if (null == runConfig) Map.empty else mapAsScalaMap(runConfig).toMap
    val collector = new SmvRunInfoCollector
    val df =  j_smvApp.runModule(URN(urn), forceRun, version, dynamicRunConfig, collector, quickCompute)
    RunModuleResult(df, collector)
  }

  /** Runs an SmvModule written in either Python or Scala */
  def runModuleByName(name: String,
                forceRun: Boolean,
                version: Option[String],
                runConfig: java.util.Map[String, String],
                quickCompute: Boolean = false): RunModuleResult = {
    val dynamicRunConfig: Map[String, String] = if (null == runConfig) Map.empty else mapAsScalaMap(runConfig).toMap
    val collector = new SmvRunInfoCollector
    val df =  j_smvApp.runModuleByName(name, forceRun, version, dynamicRunConfig, collector, quickCompute)
    RunModuleResult(df, collector)
  }

  def publishModuleToHiveByName(name: String,
                                runConfig: java.util.Map[String, String]) = {
      val dynamicRunConfig: Map[String, String] = if (null == runConfig) Map.empty else mapAsScalaMap(runConfig).toMap
      val collector = new SmvRunInfoCollector
      j_smvApp.publishModuleToHiveByName(name, dynamicRunConfig, collector)
  }

  /**
   * Returns the run information of a dataset and all its dependencies
   * from the last run.
   */
  def getRunInfo(urn: String, runConfig: java.util.Map[String, String]): SmvRunInfoCollector =
    j_smvApp.getRunInfo(URN(urn), javaMapToImmutableMap(runConfig))

  def getRunInfoByPartialName(partialName: String, runConfig: java.util.Map[String, String]): SmvRunInfoCollector =
    j_smvApp.getRunInfo(partialName, javaMapToImmutableMap(runConfig))

  def copyToHdfs(in: IAnyInputStream, dest: String): Unit =
    SmvHDFS.writeToFile(in, dest)

  /** Returns metadata for a given urn*/
  def getMetadataJson(urn: String): String =
    j_smvApp.getMetadataJson(URN(urn))

  /** Returns metadata history for a given urn*/
  def getMetadataHistoryJson(urn: String): String =
    j_smvApp.getMetadataHistoryJson(URN(urn))

  // TODO: The following method should be removed when Scala side can
  // handle publish-hive SmvOutput tables
  def moduleNames: java.util.List[String] = {
    val cl                      = j_smvApp.smvConfig.cmdLine
    val directMods: Seq[String] = cl.modsToRun()
    directMods
  }

  /**
   * Returns the lock file status, if any, for a given module in a
   * java.util.Map.  The file modification time will be a String
   * representation of the datetime in milliseconds, under the key
   * 'lockTime'.
   *
   * Returns an empty map if the lock is not found for the module.
   *
   * If the module is not found, the call to inferDS() would throw an
   * SmvRuntimeException.
   *
   * @param modName a name that uniquely identifies a module, typically its FQN
   * @return a map with the key "lockTime" and the file modification time in milliseconds
   */
  def getLockfileStatusForModule(modName: String): java.util.Map[String, String] = {
    j_smvApp.dsm.inferDS(modName) match {
      case Nil => java.util.Collections.emptyMap()
      case ds::_ =>
        ds.lockfileStatus match {
          case None => java.util.Collections.emptyMap()
          case Some(fs) => Map("lockTime" -> fs.getModificationTime.toString)
        }
    }
  }

  // ---- User Run Configuration Parameters proxy funcs.
  def getRunConfig(key: String): String = j_smvApp.smvConfig.getRunConfig(key)
  def getRunConfigHash()                = j_smvApp.smvConfig.getRunConfigHash()

  def registerRepoFactory(id: String, iRepoFactory: IDataSetRepoFactoryPy4J): Unit =
    j_smvApp.registerRepoFactory(new DataSetRepoFactoryPython(iRepoFactory, j_smvApp.smvConfig))

  def javaMapToImmutableMap(javaMap: java.util.Map[String, String]): Map[String, String] =
    if (javaMap == null) Map.empty else mapAsScalaMap(javaMap).toMap

  def getSmvSchema() = SmvSchema

  def readCsvFromFile(
    fullPath: String,
    schema: SmvSchema,
    csvAttr: CsvAttributes,
    parserLogger: ParserLogger
  ) = {
    // Python side always provide schema instead of schemaPath
    val handler = new FileIOHandler(j_smvApp.sparkSession, fullPath, None, parserLogger)
    handler.csvFileWithSchema(csvAttr, Some(schema))
  }

  /**
   * Map the data file path to schema file path,
   * and then try to read the schema from schema file.
   * 
   * Return the schema as a SmvSchema instance.
   * If the schema file does not exist, return null.
   */
  def readSchemaFromDataPathAsSmvSchema(dataFilePath: String) = {
    val schemaFilePath = SmvSchema.dataPathToSchemaPath(dataFilePath)

    try {
      SmvSchema.fromFile(j_smvApp.sc, schemaFilePath)
    } catch {
      case _: InvalidInputException => null
    }
  }

  def getDirList(dirPath: String): java.util.List[String] = SmvHDFS.dirList(dirPath)
}

/** Not a companion object because we need to access it from Python */
object SmvPyClientFactory {
  def init(sparkSession: SparkSession): SmvPyClient = init(Array("-m", "None"), sparkSession)

  def init(args: Array[String], sparkSession: SparkSession): SmvPyClient =
    new SmvPyClient(SmvApp.init(args, Option(sparkSession)))
}


/** For use by Python API to return a tuple */
case class RunModuleResult(df: DataFrame, collector: SmvRunInfoCollector)
