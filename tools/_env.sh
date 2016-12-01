#!/bin/bash
# setup common environment used by smv-run and smv-shell scripts
#
# The following vars will be set once this script is sourced:
# SMV_ARGS : command line args (other than the spark args which are extracted separately)
# SPARK_ARGS : spark specific command line args (everything after "--" on command line)
# USER_CMD : name of the script that was launched (caller of this script)
# SMV_APP_CLASS : user specified --class name to use for spark-submit or SmvApp as default
# APP_JAR : user specified --jar option or the discovered application fat jar.
#

# This function is used to split the command line arguments into SMV / Spark
# arguments.  Everything before "--" are considered SMV arguments and everything
# after "--" are considered spark arguments.
function split_smv_spark_args()
{
    while [ $# -ne 0 ]; do
        if [ "$1" == "--" ]; then
            shift
            break
        fi
        SMV_ARGS=("${SMV_ARGS[@]}" "$1")
        shift
    done

    SPARK_ARGS=("$@")
}

function find_fat_jar()
{
  # find latest fat jar in target directory.
  # try sbt-build location first
  APP_JAR=`ls -1t target/scala-2.10/*jar-with-dependencies.jar 2>/dev/null| head -1`

  # if not found try mvn-build location next
  if [ -z "$APP_JAR" ]; then
    APP_JAR=`ls -1t target/*jar-with-dependencies.jar 2>/dev/null| head -1`
  fi

  if [ -z "$APP_JAR" ]; then
    echo "ERROR: could not find an app jar in target directory"
    exit 1
  fi
}

function set_spark_home() {
    if [ -z "$SPARK_HOME" ]; then
        sparkSubmit=$(type -p spark-submit)
        if [ -z "$sparkSubmit" ]; then
            echo "Can not find spark-submit script"
            exit 1
        fi
        export SPARK_HOME=$(cd $(dirname $sparkSubmit)/..; pwd)
    fi
}

function show_run_usage_message() {
  echo "USAGE: $1 [-h] <smv_app_args> [-- spark_args]"
  echo "smv_app_args:"
  echo "    [--purge-old-output]"
  echo "    [--data-dir dir] ..."
  echo "    <-m mod1 [mod2 ...] | -s stage1 [stage2 ...] | --run-app> ..."
  echo "    ..."
  echo "spark_args:"
  echo "    [--master master]"
  echo "    [--driver-memory=driver-mem]"
  echo "    ..."
}


# --- MAIN ---
declare -a SMV_ARGS SPARK_ARGS
USER_CMD=`basename $0`
SMV_APP_CLASS="org.tresamigos.smv.SmvApp"
find_fat_jar
split_smv_spark_args "$@"
