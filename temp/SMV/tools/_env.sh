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

# This function prints to stdout the full path to the site-packages directory of the Python
# distribution on the PATH
function get_python_site_packages_dir() {
  # site.getsitepackages is broken inside of virtual environments on mac os x, so we fall back to distutils
    # https://github.com/dmlc/tensorboard/issues/38#issuecomment-343017735
    local python_site_packages
    python_site_packages=$( \
      python -c "import site; print(site.getsitepackages()[0])" 2>/dev/null ||  \
      python -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())" 2>/dev/null \
    )

    if [ -z "${python_site_packages}" ]; then
      (>&2 echo "Could not find site-packages directory. Need to exit")
      exit 3
    fi

    echo "${python_site_packages}"
}

# This function is used to determine where the "real" SMV_TOOLS directory lives. In the most basic case,
# SMV_TOOLS is specifed by an environment variable, and we simply take that as the SMV_TOOLS dir.
# If not specified as an environment variable, then we take one of two paths:
# (1) if it appears like the get_smv_tools_dir got invoked from within a python environment (i.e. installed
#     via pip into virtualenv_root/lib/python/site-packages, and CLI tools in virtualenv_root/bin), then
#     we can set SMV tools to site-packages/SMV/tools
# (2) otherwise, assume that where the called of get_smv_tools_dir is inside of the SMV_TOOLS dir
function get_smv_tools_dir() {
  local smv_tools_candidate=""
  local this_file_dir
  this_file_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"
  local bin_dir_pattern=".*/bin"

  # SMV_TOOLS env var is already populated
  if [[ ! -z "${SMV_TOOLS:-}" ]]; then
    smv_tools_candidate="${SMV_TOOLS}"
  # We appaer to be inside of a python distributions bin directory. Full path has
  # bin in the name and there is a python file (executable) alongside us
  elif [[ "${this_file_dir}" =~ $bin_dir_pattern ]]; then
    local site_package_dir
    site_package_dir="$(get_python_site_packages_dir)"
    smv_tools_candidate="${site_package_dir}/smv/tools"
  else
    smv_tools_candidate="${this_file_dir}"
  fi

  if [[ ! -f "${smv_tools_candidate}/smv-run" ]]; then
    (>&2 echo "Could not a suitable candidate for the SMV_TOOLS directory")
    (>&2 echo "Best guess was ${smv_tools_candidate}, but it did not contain smv-run command")
    exit 1
  fi

  echo "${smv_tools_candidate}"
}

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
  local file_candidate=""
  filepat=$1
  shift
  for dir in "${@}"; do
    if [ -d $dir ]; then
      file_candidate=$(find $dir -maxdepth 1 -name "${filepat}")
      if [ -n "$file_candidate" ]; then
        echo "${file_candidate}"
        break
      fi
    fi
  done
}

# find latest fat jar in target directory.
function find_fat_jar()
{
  local smv_tools_dir="$(get_smv_tools_dir)"
  SMV_FAT_JAR="${smv_tools_dir}/../target/scala-2.11"

  # try sbt-build location first if not found try mvn-build location next.
  # then repeat from the parent directory, because the shell is
  # sometimes run from a notebook subdirectory of a data project
  dirs=("target/scala-2.11" "target" "../target/scala-2.11" "../target" "$SMV_FAT_JAR" "${SMV_HOME}/target" "${SMV_HOME}/target/scala-2.11")
  APP_JAR=$(find_file_in_dir "smv*jar-with-dependencies.jar" "${dirs[@]}")

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
  local this_file_dir
  this_file_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null && pwd )"

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
    echo "Are you sure that pyspark and spark-submit are on your PATH?"
    exit 1
  fi

  if ! java -version &> /dev/null; then
    echo "ERROR: java command does not appear on the PATH. Spark will not work without java8 installed"
    exit 3
  fi

  # The reason why I added the check at all is because if you are on java !== 1.8 and spark 2.2
  # the error message is absolutely atrocious...
  # If `spark-submit` is run:
  #   * In Java 1.9 you get a nasty java stacktrace that either talks about a string class exception
  #   * In Java 1.7 you get a java stacktrace byte code version mismatch (this message is the most helpful of the lot)
  if ! (java -version 2>&1 | grep "1.8" &>/dev/null); then
    echo "WARNING: java command found, but version may not be supported by spark"
    echo "WARNING: spark 2.2+ only support java 1.8. Current java version is: "
    java -version
  fi
}

function set_smv_home() {
  local smv_tools
  smv_tools="$(get_smv_tools_dir)"

  export SMV_HOME
  SMV_HOME="${SMV_HOME:-$(cd $smv_tools; cd ..; pwd)}"
  echo "SMV_HOME set to: ${SMV_HOME}"
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

# We eagerly add the basename of the APP_JAR so that this same command
# works in YARN Cluster mode, where the JAR gets added to the root directory
# of the container. Also note the colon separator
function run_pyspark_with () {
  local tools_dir="$(get_smv_tools_dir)"
  local SMV_HOME="$(cd ${tools_dir}/..; pwd)"
  local PYTHONPATH="$SMV_HOME/src/main/python:$PYTHONPATH"
  # Suppress creation of .pyc files. These cause complications with
  # reloading code and have led to discovering deleted modules (#612)
  local PYTHONDONTWRITEBYTECODE=1
  local SPARK_PRINT_LAUNCH_COMMAND=1
  local SMV_LAUNCH_SCRIPT="${SMV_LAUNCH_SCRIPT:-${SMV_SPARK_SUBMIT_FULLPATH}}"

  ( export PYTHONDONTWRITEBYTECODE SPARK_PRINT_LAUNCH_COMMAND PYTHONPATH; \
    "${SMV_LAUNCH_SCRIPT}" "${SPARK_ARGS[@]}" \
    --jars "$APP_JAR,$EXTRA_JARS" \
    --driver-class-path "$APP_JAR:$(basename ${APP_JAR}):$EXTRA_DRIVER_CLASSPATHS" \
    $1 "${SMV_ARGS[@]}"
  )
}

# --- MAIN ---
declare -a SMV_ARGS SPARK_ARGS

USER_CMD=`basename $0`
SMV_APP_CLASS="org.tresamigos.smv.SmvApp"
split_smv_spark_args "$@"
set_smv_spark_paths
set_smv_home
verify_spark_version
check_help_option
find_fat_jar
