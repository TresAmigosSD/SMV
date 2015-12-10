
.First <- function() {
  spark_home <- Sys.getenv("SPARK_HOME")
  .libPaths(c(file.path(spark_home, "R", "lib"), .libPaths()))
  Sys.setenv(NOAWT=1)

  # Make sure SparkR package is the last loaded one
  old <- getOption("defaultPackages")
  options(defaultPackages = c(old, "SparkR"))

  sc <- SparkR::sparkR.init(sparkJars=Sys.getenv("SMV_APP_JAR"))
  assign("sc", sc, envir=.GlobalEnv)
  sqlContext <- SparkR::sparkRSQL.init(sc)
  sparkVer <- SparkR:::callJMethod(sc, "version")
  assign("sqlContext", sqlContext, envir=.GlobalEnv)

  cat("\n Spark context is available as sc, SQL context is available as sqlContext, SMV App is availabe as smvApp.\n")
}
