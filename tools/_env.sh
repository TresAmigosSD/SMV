#!/bin/bash
# setup common environment used by smv-run and smv-shell scripts
#
# The following vars will be set once this script is sourced:
# ARGS : command line args (other than the spark args which are extracted separately)
# USER_CMD : name of the script that was launched (caller of this script)
# MASTER : the user specified --master spark arg (or a valid default)
# SMV_APP_CLASS : user specified --class name to use for spark-submit or SmvApp as default
# APP_JAR : user specified --jar option or the discovered application fat jar.
#

declare -a ARGS
ARGS=($@)
USER_CMD=`basename $0`

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

function extract_spark_args()
{
    extract_arg MASTER --master ${SMV_MASTER:-'local[*]'}
    extract_arg SMV_APP_CLASS --class "org.tresamigos.smv.SmvApp"
    extract_arg APP_JAR --jar ""

    if [ -z "$APP_JAR" ]; then
        find_fat_jar
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


# --- MAIN ---
extract_spark_args
