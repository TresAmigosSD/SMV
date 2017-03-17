#!/usr/bin/env bash
#
# Set up common python environment used by smv-pyrun, smv-pytest scripts
#

export SPARK_PRINT_LAUNCH_COMMAND=1

function run_pyspark_with () {
  spark-submit "${SPARK_ARGS[@]}" --jars "$APP_JAR" --driver-class-path "$APP_JAR" $1 "${SMV_ARGS[@]}"
}
