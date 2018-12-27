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
    # --jars options
    while [ $# -ne 0 ]; do
        if [ "$1" == "--jars" ]; then
            shift
            EXTRA_JARS="$1"
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
  SMV_FAT_JAR="${SMV_TOOLS}/../target/scala-2.10"

  # try sbt-build location first if not found try mvn-build location next.
  # then repeat from the parent directory, because the shell is
  # sometimes run from a notebook subdirectory of a data project
  # Fall back to SMV fat jar for python only projects.
  dirs=("target/scala-2.10" "target" "../target/scala-2.10" "../target" "$SMV_FAT_JAR")
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

# Remove trailing alphanum characters in dot-separated version text.
function sanitize_version () {
  # match a digit, followed by a letter, "+" or "_," and anything up to a "."
  # keep just the digit -- essentially removing any trailing alphanum between dots
  echo $(sed -E 's/([0-9])[_+a-zA-Z][^.]*/\1/g' <<< "$1")
}

# Compares the two versions (required, found) after sanitizing using
# the function above. Versions are dot-separated text. The major and
# minor parts must match exactly with required, and the patch part in
# the found version must be no less than required.
#
# echoes 0 if the found version meets the criteria
#        1 otherwise
function accept_version () {
  local sane=$(sanitize_version $2)
  local IFS=.
  local required=($1) found=($sane)
  if [[ ${required[0]} == ${found[0]} ]] && [[ ${required[1]} == ${found[1]} ]]; then
    echo 0
  else
    echo 1
  fi
}

function verify_spark_version() {
  local installed_version=$(${SMV_SPARK_SUBMIT_FULLPATH} --version 2>&1 | \
    grep -v "Spark Command" | grep version | head -1 | sed -e 's/.*version //')
  local required_version=$(cat "$SMV_TOOLS/../.spark_version")
  local vercmp=$(accept_version "$required_version" "$installed_version")
  if [[ $vercmp != "0" ]]; then
    echo "Spark $installed_version detected. Please install Spark $required_version."
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
USER_CMD=`basename $0`
SMV_APP_CLASS="org.tresamigos.smv.SmvApp"
split_smv_spark_args "$@"
set_smv_spark_paths
verify_spark_version
check_help_option
find_fat_jar
