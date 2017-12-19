#!/usr/bin/env bash
#
# Set up common python environment used by smv-pyrun, smv-pytest scripts
#

# Another way to add smv.py is through the --py-files option passed to
# pyspark, as in `pyspark --py-files $SMV_TOOLS/../python/smv.py`
# Not sure yet which way is best practice.
SMV_HOME="$(cd "`dirname "$0"`/.."; pwd)"
export PYTHONPATH="$SMV_HOME/src/main/python:$PYTHONPATH"
# Suppress creation of .pyc files. These cause complications with
# reloading code and have led to discovering deleted modules (#612)
export PYTHONDONTWRITEBYTECODE=1
export SPARK_PRINT_LAUNCH_COMMAND=1

function run_pyspark_with () {
  "${SMV_SPARK_SUBMIT_FULLPATH}" "${SPARK_ARGS[@]}" --jars "$APP_JAR,$EXTRA_JARS" --driver-class-path "$APP_JAR" $1 "${SMV_ARGS[@]}"
}
