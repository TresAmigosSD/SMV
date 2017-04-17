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

package org.tresamigos.smv.hierTestPkg1 {
  import org.tresamigos.smv._

  object GeoMapFile extends SmvModule("") with SmvOutput {
    override def requiresDS = Seq.empty

    override def run(i: runParams) = {
      app.createDF(
        "zip:String;Territory:String;Division:String;Division_name:String",
        """100,001,01,D01;
        102,001,01,D01;
        103,001,01,D01;
        201,002,01,D01;
        301,003,02,D02;
        401,004,02,D02;
        405,004,02,D02"""
      )
    }
  }
}

package org.tresamigos.smv {

  import org.apache.spark.sql.DataFrame

  class SmvHierarchyTest extends SmvTestUtil {
    val singleStage = Seq("--smv-props", "smv.stages=org.tresamigos.smv.hierTestPkg1")

    override def appArgs = singleStage ++ Seq("-m", "None", "--data-dir", testcaseTempDir)

    test("Test SmvHierarchyFuncs") {
      val ssc = sqlContext; import ssc.implicits._

      val df = dfFrom(
        "id:String;zip3:String;terr:String;div:String;v:Double",
        """a,100,001,01,10.3;
       a,102,001,01,1.0;
       a,102,001,01,2.0;
       a,102,001,01,3.0;
       b,102,001,01,4.0;
       b,103,001,01,10.1;
       b,103,001,01,10.2;
       a,103,001,01,10.3;
       a,201,002,01,11.0;
       b,301,003,02,15;
       b,401,004,02,15;
       b,405,004,02,20"""
      )

      object TestHeir
          extends SmvHierarchies(
            "hier",
            SmvHierarchy("div", null, Seq("zip3", "terr", "div"))
          ) {
        override def applyToDf(df: DataFrame) = df

        def allSum(dfWithKey: SmvDFWithKeys, cols: String*) =
          levelSum(dfWithKey, "terr", "div")(cols: _*)()
      }

      val res = TestHeir.allSum(df.smvWithKeys("id"), "v")

      assertSrddSchemaEqual(res, "id: String; hier_type: String; hier_value: String; v: Double")
      assertSrddDataEqual(
        res,
        """b,div,02,50.0;
                                a,div,01,37.6;
                                a,terr,001,26.6;
                                b,terr,003,15.0;
                                b,div,01,24.299999999999997;
                                b,terr,001,24.299999999999997;
                                a,terr,002,11.0;
                                b,terr,004,35.0"""
      )
    }

    test("module with name column test") {
      object GeoHier
          extends SmvHierarchies(
            "geo",
            SmvHierarchy("terr", hierTestPkg1.GeoMapFile, Seq("zip", "Territory", "Division"))
          )

      object TestModule extends SmvModule("") {
        override def requiresDS()  = Seq.empty
        override def requiresAnc() = Seq(GeoHier)

        override def run(i: runParams) = {
          val srdd = app.createDF(
            "zip:String; V1:double; V2:Double; V3:Double; time:Integer",
            """100,  -5.0, 2.0, -150.0, 2013;
          102, -10.0, 8.0,  300.0, 2014;
          201, -50.0, 5.0,  500.0, 2013;
          301, -50.0, 5.0,  500.0, 2014;
          401, -50.0, 5.0,  500.0, 2013;
          405, -50.0, 5.0,  500.0, 2013"""
          )

          GeoHier.levelSum(srdd.smvWithKeys("time"), "Territory", "Division")("V1", "V2", "V3")(
            SmvHierOpParam(true, None))
        }
      }

      hierTestPkg1.GeoMapFile.rdd
      val res = TestModule.rdd

      assertSrddDataEqual(
        res,
        """2013,Territory,002,null,-50.0,5.0,500.0;
                                2013,Division,02,D02,-100.0,10.0,1000.0;
                                2014,Division,01,D01,-10.0,8.0,300.0;
                                2014,Territory,001,null,-10.0,8.0,300.0;
                                2014,Division,02,D02,-50.0,5.0,500.0;
                                2013,Division,01,D01,-55.0,7.0,350.0;
                                2013,Territory,001,null,-5.0,2.0,-150.0;
                                2013,Territory,004,null,-100.0,10.0,1000.0;
                                2014,Territory,003,null,-50.0,5.0,500.0"""
      )
    }

    test("SmvHierarchies with parent") {
      object GeoHier
          extends SmvHierarchies(
            "geo",
            SmvHierarchy("terr", hierTestPkg1.GeoMapFile, Seq("zip", "Territory", "Division"))
          )

      val df = app.createDF(
        "zip:String; V1:double; V2:Double; V3:Double; time:Integer",
        """100,  -5.0, 2.0, -150.0, 2013;
      102, -10.0, 8.0,  300.0, 2014;
      201, -50.0, 5.0,  500.0, 2013;
      301, -50.0, 5.0,  500.0, 2014;
      401, -50.0, 5.0,  500.0, 2013;
      405, -50.0, 5.0,  500.0, 2013"""
      )

      val res2 =
        GeoHier.levelSum(df.smvWithKeys("time"), "Territory", "Division")("V1", "V2", "V3")(
          SmvHierOpParam(true, Some("terr")))

      assertSrddSchemaEqual(
        res2,
        """time: Integer; geo_type: String; geo_value: String; geo_name: String;
                                   parent_geo_type: String; parent_geo_value: String; parent_geo_name: String;
                                   V1: Double; V2: Double; V3: Double"""
      )

      assertSrddDataEqual(
        res2,
        """2013,Territory,002,null,Division,01,D01,-50.0,5.0,500.0;
                                  2013,Division,02,D02,null,null,null,-100.0,10.0,1000.0;
                                  2014,Division,01,D01,null,null,null,-10.0,8.0,300.0;
                                  2014,Territory,001,null,Division,01,D01,-10.0,8.0,300.0;
                                  2014,Division,02,D02,null,null,null,-50.0,5.0,500.0;
                                  2013,Division,01,D01,null,null,null,-55.0,7.0,350.0;
                                  2013,Territory,001,null,Division,01,D01,-5.0,2.0,-150.0;
                                  2013,Territory,004,null,Division,02,D02,-100.0,10.0,1000.0;
                                  2014,Territory,003,null,Division,02,D02,-50.0,5.0,500.0"""
      )
    }

    test("SmvHierarchies appendParentValues test") {
      object GeoHier
          extends SmvHierarchies(
            "geo",
            SmvHierarchy("terr", hierTestPkg1.GeoMapFile, Seq("zip", "Territory", "Division"))
          )

      val df = app.createDF(
        "zip:String; V1:double; V2:Double; V3:Double; time:Integer",
        """100,  -5.0, 2.0, -150.0, 2013;
      102, -10.0, 8.0,  300.0, 2014;
      201, -50.0, 5.0,  500.0, 2013;
      301, -50.0, 5.0,  500.0, 2014;
      401, -50.0, 5.0,  500.0, 2013;
      405, -50.0, 5.0,  500.0, 2013"""
      )

      val res1 =
        GeoHier.levelSum(df.smvWithKeys("time"), "Territory", "Division")("V1", "V2", "V3")()

      val res2 = GeoHier.appendParentValues(res1.smvWithKeys("time"), "terr")

      assertSrddSchemaEqual(
        res2,
        """time: Integer; geo_type: String; geo_value: String;
                                   parent_geo_type: String; parent_geo_value: String;
                                   V1: Double; V2: Double; V3: Double;
                                   parent_V1: Double; parent_V2: Double; parent_V3: Double"""
      )
      assertSrddDataEqual(
        res2,
        """2014,Territory,003,Division,02,-50.0,5.0,500.0,-50.0,5.0,500.0;
                        2013,Division,01,null,null,-55.0,7.0,350.0,null,null,null;
                        2013,Division,02,null,null,-100.0,10.0,1000.0,null,null,null;
                        2013,Territory,001,Division,01,-5.0,2.0,-150.0,-55.0,7.0,350.0;
                        2013,Territory,002,Division,01,-50.0,5.0,500.0,-55.0,7.0,350.0;
                        2013,Territory,004,Division,02,-100.0,10.0,1000.0,-100.0,10.0,1000.0;
                        2014,Division,01,null,null,-10.0,8.0,300.0,null,null,null;
                        2014,Division,02,null,null,-50.0,5.0,500.0,null,null,null;
                        2014,Territory,001,Division,01,-10.0,8.0,300.0,-10.0,8.0,300.0"""
      )
    }

    test("levelRollup with count aggregation test") {
      import org.apache.spark.sql.functions._
      val ssc = sqlContext; import ssc.implicits._

      object GeoHier
          extends SmvHierarchies(
            "geo",
            SmvHierarchy("terr", hierTestPkg1.GeoMapFile, Seq("zip", "Territory", "Division"))
          )

      val df = app.createDF("zip:String", """100;
      102;
      103;
      201;
      301;
      401;
      405""").smvSelectPlus($"zip" as "_zip")

      val res = GeoHier
        .levelRollup(
          df,
          "zip",
          "Territory",
          "Division"
        )(
          count("_zip") as "Zip_Cnt"
        )()
        .orderBy(asc("geo_type"), asc("geo_value"))

      assertSrddSchemaEqual(res, """geo_type: String;
                        geo_value: String;
                        Zip_Cnt: Long""")

      assertSrddDataEqual(
        res,
        """
      Division,01,4;
      Division,02,3;
      Territory,001,3;
      Territory,002,1;
      Territory,003,1;
      Territory,004,2;
      zip,100,1;
      zip,102,1;
      zip,103,1;
      zip,201,1;
      zip,301,1;
      zip,401,1;
      zip,405,1
      """
      )
    }

  }

}
