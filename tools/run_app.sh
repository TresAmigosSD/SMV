#!/bin/bash
#
# Run one or more SMV modules / stages / app.
# USAGE: run_app.sh [-h] spark_args smv_app_args
#
# user can specify spark args (such as --master, --class or --jar to override the fat jar selection)
# the rest of the arguments are the standard SmvApp arguments.

set -e
if [ $# -lt 1 -o "$1" = "-h" ]; then
  echo "USAGE: `basename $0` [-h] [spark_args] <smv_app_args>"
  echo "spark_args:"
  echo "    [--master master]"
  echo "    [--class class]"
  echo "    [--jar fat_jar]"
  echo "smv_app_args:"
  echo "    [--purge-old-output]"
  echo "    [--data-dir dir] ..."
  echo "    <-m mod1 [mod2 ...] | -s stage1 [stage2 ...] | --run-app> ..."
  exit 1
fi

declare -a ARGS
ARGS=($@)

# extract an argument of the form "--arg arg_val" from the command line arguments
# Return the arg_val if found (and remove the flag and value from ARGS), else return
# the passed in default value.
# NOTE: assumes there is an array named ARGS that is set to the command line arguments.
# USAGE: extract_arg dest_var_name argument_flag default_value
# Example: extract_arg MASTER --master localhost
# the above will look for "--master value" in args and if found, set MASTER to value,
# otherwise, set MASTER to default value "localhost"
function extract_arg()
{
    local arg_var="$1"
    local arg_name="$2"
    local arg_default="$3"

    local arg_val=""
    for i in ${!ARGS[@]}; do
      if [ "${ARGS[i]}" = "$arg_name" ]; then
        arg_val="${ARGS[i+1]}"
        unset "ARGS[i]" "ARGS[i+1]"
        break
      fi
    done

    if [ -z "$arg_val" ]; then
      arg_val="$arg_default"
    fi

    eval $arg_var="'$arg_val'"
}

function find_fat_jar()
{
  # find latest fat jar in target directory.
  APP_JAR=`ls -1t target/*jar-with-dependencies.jar 2>/dev/null| head -1`
  if [ -z "$APP_JAR" ]; then
    echo "ERROR: could not find an app jar in target directory"
    exit 1
  fi
}

function extract_spark_args()
{
    extract_arg MASTER --master 'local[*]'
    extract_arg SMV_APP_CLASS --class "org.tresamigos.smv.SmvApp"
    extract_arg APP_JAR --jar ""

    if [ -z "$APP_JAR" ]; then
        find_fat_jar
    fi
}

# ---- MAIN ----

extract_spark_args

echo "START RUN =============================="

date
# TODO: use executer-mem config instead!
export SPARK_MEM=6G
export SPARK_PRINT_LAUNCH_COMMAND=1
spark-submit --master ${MASTER} --class ${SMV_APP_CLASS} "$APP_JAR" ${ARGS[@]}
date

