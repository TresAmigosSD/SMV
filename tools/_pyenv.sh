#!/usr/bin/env bash
#
# Set up common python environment used by smv-pyrun, smv-pytest scripts
#

export PYTHONPATH="$PYTHONPATH:${SMV_TOOLS}/../python:src/main/python"
export SPARK_PRINT_LAUNCH_COMMAND=1

# The following delivers the packaged python script in the jar as the
# script, as arguments to the --py-files option has precedence over
# PYTHONPATH
#
#spark-submit --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --master ${MASTER} --jars "$APP_JAR" --driver-class-path "$APP_JAR" --py-files "$APP_JAR"

function run_pyspark_with () {
  spark-submit --executor-memory ${EXECUTOR_MEMORY} --driver-memory ${DRIVER_MEMORY} --master ${MASTER} --jars "$APP_JAR" --driver-class-path "$APP_JAR" $1 ${ARGS[@]}
}
