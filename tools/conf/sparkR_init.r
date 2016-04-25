
# This is a simplified copy of the "shell.R" profile from Spark.
# We had to duplicate here to avoid having to create another SparkContext just to pass the smv app jar to the init function.

.First <- function() {
  spark_home <- Sys.getenv("SPARK_HOME")
  smv_home <- Sys.getenv("SMV_HOME")

  .libPaths(c(file.path(spark_home, "R", "lib"),
              file.path(smv_home, "R", "lib"),
              .libPaths()))
  Sys.setenv(NOAWT=1)

  old <- getOption("defaultPackages")
  options(defaultPackages = c(old, "SparkR", "smvR"))

  sc <- SparkR::sparkR.init(sparkJars=Sys.getenv("SMV_APP_JAR"))
  assign("sc", sc, envir=.GlobalEnv)

  sqlContext <- SparkR::sparkRSQL.init(sc)
  assign("sqlContext", sqlContext, envir=.GlobalEnv)

  sparkR:::callJMethod(sc, "setLogLevel", "ERROR")
  cat("\nSet Log level to ERROR.\n")

  sparkVer <- SparkR:::callJMethod(sc, "version")

  smvApp <- smvR:::smvR.init(sqlContext)
  assign("smvApp", smvApp, envir=.GlobalEnv)


  cat("\n Spark context is available as sc, SQL context is available as sqlContext, SMV App is availabe as smvApp.\n")
}
