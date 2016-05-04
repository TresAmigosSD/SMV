#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

.smvREnv <- new.env()

#' Initialize the SMV R shell
#' @param sqlContext Spark SQL context
#' @export

smvR.init <- function(sqlContext, path) {
    app <- SparkR:::callJStatic("org.tresamigos.smv.SmvApp", "newApp", sqlContext, path)
    assign(".smvApp", app, envir = .smvREnv)
    app
}

#' Run the specified smv module and return the Raw RDD (Spark Dataframe) result.
#' @param moudleName The module name to run (can be FQN of module or just the base name)
#' @export

runSmvModuleRdd <- function(moduleName) {
    app <- get(".smvApp", envir = .smvREnv)
    rdd <- SparkR:::callJMethod(smvApp, "runModuleByName", moduleName)
    df <- SparkR:::dataFrame(rdd)
    df
}

#' Dynamically load and Run a specified smv model and return the Spark Dataframe.
#' @param moudleName The module name to run (must be FQN of module)
#' @export

dynSmvModuleRdd <- function(moduleName) {
    app <- get(".smvApp", envir = .smvREnv)
    rdd <- SparkR:::callJMethod(smvApp, "runDynamicModuleByName", moduleName)
    df <- SparkR:::dataFrame(rdd)
    df
}

#' Run the specified smv module and return an R dataframe of the result.
#' @param moudleName The module name to run (can be FQN of module or just the base name)
#' @export

runSmvModule <- function(moduleName) {
    rdd <- runSmvModuleRdd(moduleName)
    df = collect(rdd)
    df
}
