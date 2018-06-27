#!/bin/bash
# setup common environment used by smv-run and smv-shell scripts
#
# The following vars will be set once this script is sourced:
# SMV_ARGS : command line args (other than the spark args which are extracted separately)
# SPARK_ARGS : spark specific command line args (everything after "--" on command line)
# USER_CMD : name of the script that was launched (caller of this script)
# SMV_APP_CLASS : user specified --class name to use for spark-submit or SmvApp as default
# APP_JAR : user specified --jar option or the discovered application fat jar.
# SMV_USER_SCRIPT : optional user-defined launch script
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
        
        if [ "$1" == "--script" ]; then 
          SMV_USER_SCRIPT="$2"
        fi

        if [ "$1" == "--spark-home" ]; then
          shift
          SPARK_HOME_OPT="$1"
          shift
        else
          SMV_ARGS=("${SMV_ARGS[@]}" "$1")
          shift
        fi
    done

    # Need to extract the --jars option so we can concatenate those jars with
    # the APP_JAR when we run the spark-submit. Spark will not accept 2 separate
    # --jars options. Also need to handle the case when user uses equal signs (--jars=xyz.jar)
    # Same for --driver-class-path
    while [ $# -ne 0 ]; do
        if [ "$1" == "--jars" ]; then
            shift
            EXTRA_JARS="$1"
            shift
        # See http://wiki.bash-hackers.org/syntax/pe#search_and_replace for bash string parsing
        # tricks
        elif [ ${1%%=*} == "--jars" ]; then
            local ACTUAL_JARS_PORTION="${1#*=}"
            EXTRA_JARS="${ACTUAL_JARS_PORTION}"
            # Only need to shift once since we dont have a space separator
            shift
        elif [ "$1" == "--driver-class-path" ]; then
            EXTRA_DRIVER_CLASSPATHS="$1"
        elif [ ${1%%=*} == "--driver-class-path" ]; then
            local ACTUAL_CLASSPATHS_PORTION="${1#*=}"
            EXTRA_DRIVER_CLASSPATHS="${ACTUAL_CLASSPATHS_PORTION}"
            # Only need to shift once since we dont have a space separator
            shift
        else
          SPARK_ARGS=("${SPARK_ARGS[@]}" "$1")
          shift
        fi
    done

}

function find_file_in_dir()
{
  filepat=$1
  shift
  for dir in "$@"; do
    APP_JAR=`ls -1t ${dir}/${filepat} 2>/dev/null | head -1`
    if [ -n "$APP_JAR" ]; then
      break
    fi
  done
}

# find latest fat jar in target directory.
function find_fat_jar()
{
  # SMV_TOOLS should have been set by caller.
  if [ -z "$SMV_TOOLS" ]; then
    echo "ERROR: SMV_TOOLS not set by calling script!"
    exit 1
  fi
  SMV_FAT_JAR="${SMV_TOOLS}/../target/scala-2.11"

  # try sbt-build location first if not found try mvn-build location next.
  # then repeat from the parent directory, because the shell is
  # sometimes run from a notebook subdirectory of a data project

  dirs=("target/scala-2.11" "target" "../target/scala-2.11" "../target" "$SMV_FAT_JAR")
  find_file_in_dir "*jar-with-dependencies.jar" "${dirs[@]}"
  echo APP_JAR = $APP_JAR

  if [ -z "$APP_JAR" ]; then
    echo "ERROR: could not find an app jar in local target directory or SMV build target"
    exit 1
  fi
}

# creates the SMV_SPARK_SUBMIT_FULLPATH and SMV_PYSPARK_FULLPATH from user
# specified spark home and possibly overriden command names (for cloudera support).
# users can specify SMV_SPARK_SUBMIT_CMD and SMV_PYSPARK_CMD to override the
# executable used for spark-submit and pyspark respectively.
function set_smv_spark_paths() {
  # if user specified --spark-home, use that as prefix to all spark commands.
  local prefix=""
  if [ -n "$SPARK_HOME_OPT" ]; then
    prefix="${SPARK_HOME_OPT}/bin/"
  fi

  # create the submit/pyspark full paths from spark home and override env vars.
  export SMV_SPARK_SUBMIT_FULLPATH="${prefix}${SMV_SPARK_SUBMIT_CMD:-spark-submit}"
  export SMV_PYSPARK_FULLPATH="${prefix}${SMV_PYSPARK_CMD:-pyspark}"

  echo "Using $SMV_SPARK_SUBMIT_FULLPATH to submit jobs"
  echo "Using $SMV_PYSPARK_FULLPATH to start shells"

  # verify that the two paths are valid
  local valid_paths=1
  type -p "${SMV_SPARK_SUBMIT_FULLPATH}" > /dev/null || valid_paths=0
  type -p "${SMV_PYSPARK_FULLPATH}" > /dev/null || valid_paths=0
  if [ $valid_paths -eq 0 ]; then
    echo "ERROR: The combination of --spark-home with spark commands override"
    echo "produced invalid paths above!"
    exit 1
  fi
}

function set_smv_home() {
  export SMV_HOME="$(cd "`dirname "$0"/`/.."; pwd)"
}

# Remove trailing alphanum characters in dot-separated version text.
function sanitize_version () {
  # match a digit, followed by a letter, "+" or "_," and anything up to a "."
  # keep just the digit -- essentially removing any trailing alphanum between dots
  echo $(sed -E 's/([0-9])[_+a-zA-Z][^.]*/\1/g' <<< "$1")
}

function installed_spark_major_version() {
  local installed_version=$(${SMV_SPARK_SUBMIT_FULLPATH} --version 2>&1 | \
    grep -v "Spark Command" | grep version | head -1 | sed -e 's/.*version //')
  local sanitized_version=$(sanitize_version $installed_version)
  echo ${sanitized_version:0:1}
}

function verify_spark_version() {
  local installed_major_version=$(installed_spark_major_version)
  local required_major_version=2
  if [[ $installed_major_version != $required_major_version ]]; then
    echo "Spark $installed_major_version detected. Please install Spark $required_major_version."
    exit 1
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

# We intercept --help (and -h) so that we can make a simple spark-submit for the
# help message without running pyspark and creating a SparkContext
function check_help_option() {
  for opt in $SMV_ARGS; do
    if [ $opt = "--help" ] || [ $opt = "-h" ]; then
      print_help
      exit 0
    fi
  done
}

function print_help() {
  # Find but don't print the app jar
  find_fat_jar > /dev/null
  "${SMV_SPARK_SUBMIT_FULLPATH}" --class ${SMV_APP_CLASS}  "${APP_JAR}" --help
}

# --- MAIN ---
declare -a SMV_ARGS SPARK_ARGS
# SMV_TOOLS should have been set by caller.
if [ -z "$SMV_TOOLS" ]; then
    echo "ERROR: SMV_TOOLS not set by calling script!"
    exit 1
fi
USER_CMD=`basename $0`
SMV_APP_CLASS="org.tresamigos.smv.SmvApp"
split_smv_spark_args "$@"
set_smv_spark_paths
set_smv_home
verify_spark_version
check_help_option
find_fat_jar
