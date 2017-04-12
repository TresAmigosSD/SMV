#!/usr/bin/env bash
#
# Set up common python environment used by smv-pyrun, smv-pytest scripts
#

# Another way to add smv.py is through the --py-files option passed to
# pyspark, as in `pyspark --py-files $SMV_TOOLS/../python/smv.py`
# Not sure yet which way is best practice.
SMV_HOME="$(cd "`dirname "$0"`/.."; pwd)"
export PYTHONPATH="$SMV_HOME/src/main/python:$PYTHONPATH"

export SPARK_PRINT_LAUNCH_COMMAND=1

function run_pyspark_with () {
  spark-submit "${SPARK_ARGS[@]}" --jars "$APP_JAR" --driver-class-path "$APP_JAR" $1 "${SMV_ARGS[@]}"
}
