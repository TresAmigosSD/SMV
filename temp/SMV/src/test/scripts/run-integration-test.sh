#!/bin/bash

set -e


TEST_DIR=target/test-sample
I_APP_NAME=IntegrationApp
S_APP_NAME=SimpleApp
E_APP_NAME=EnterpriseApp

# Test stages containing a dependency scenario with a Python output module
PYTHON_MODULE_STAGES="\
test2 \
test4 \
"

HASH_TEST_MOD="integration.test.hashtest.modules.M"

PIP_INSTALL=0

function parse_args() {
  local _spark_home="${SPARK_HOME:-}"

  for arg in "${@}"; do
    if [ "${arg}" == "--spark-home" ]; then
      shift
      _spark_home="${1}"
      shift
    fi
    if [ "${arg}" == "--pip-install" ]; then
      PIP_INSTALL=1
    fi
  done

  # In insatlling in pip, make sure eo emulate a user environment without a spark home
  if [ $PIP_INSTALL == 1 ]; then
    echo "Not setting the SPARK_HOME since this is a pip installation"
    unset SPARK_HOME
  else
    export SPARK_HOME
    # If _spark_home is still empty, try to read if from wherever spark-submit lives
    if [ -z "${_spark_home}" ]; then
      local _spark_submit_path
      local _spark_bin
      _spark_submit_path="$(type -p spark-submit)"
      _spark_bin="$(dirname "${_spark_submit_path}")"
      _spark_home="$(cd "${_spark_bin}/.."; pwd)"
    fi

    SPARK_HOME=$(cd "${_spark_home}"; pwd)
    echo "Using Spark installation at ${SPARK_HOME:? Expected SPARK_HOME to have been set}"
    PATH="${SPARK_HOME}/bin:${PATH}"

    export SMV_HOME
    SMV_HOME=$(pwd)
  fi
}

function setup_virtualenv_if_needed() {
  if [ $PIP_INSTALL == 1 ]; then
    echo "Performing virtualenv pip installation of smv"
    rm -rf venv
    python -m virtualenv venv
    source venv/bin/activate
    pip install ".[pyspark]"
  fi
}

function verify_test_context() {
  echo "Using SMV_HOME of: ${SMV_HOME:-No SMV_HOME set}"

  if [ ! -z "${SMV_HOME}" ] && [ ! -d "${SMV_HOME}/src/test/scripts" ]; then
    echo "Must run this script from top level SMV directory"
    exit 1
  fi

  if [ ${PIP_INSTALL} == 1 ]; then
    SMV_INIT="smv-init"
    SMV_RUN="smv-run"
  else
    SMV_TOOLS="${SMV_HOME}/tools"
    SMV_INIT="${SMV_TOOLS}/smv-init"
    SMV_RUN="${SMV_TOOLS}/smv-run --spark-home ${SPARK_HOME}"
  fi

  echo "Using SMV_INIT of ${SMV_INIT}"
  echo "Using SMV_RUN of ${SMV_RUN}"
}

function enter_clean_test_dir() {
  rm -rf ${TEST_DIR}
  mkdir -p ${TEST_DIR}
  cd ${TEST_DIR}
}

function execute_in_dir() {
  target_dir="${1}"
  func_to_exec="${2}"
  (
    cd "${target_dir}"
    ${func_to_exec}
  )
}

function count_output() {
  echo $(wc -l <<< "$(ls -d data/output/*.csv)")
}

function verify_hash_unchanged() {
  if [ $(count_output) -gt 1 ]; then
    echo "Unchanged module's hashOfHash changed"
    exit 1
  fi
}

function verify_hash_changed() {
  previous_num_outputs="$1"
  if [ $(count_output) -le "$previous_num_outputs" ]; then
    echo "Changed module's hashOfHash didn't change"
    exit 1
  fi
}

function generate_integration_app() {
  echo "--------- GENERATE INTEGRATION APP -------------"
  ${SMV_INIT} -test ${I_APP_NAME}
}

function test_integration_app() {
  run_integration_app
  validate_integration_app_output
  validate_hash_test_module_cache_invalidation
  test_custom_driver
}

# TODO: when implement the generic output, should create csv output modules
# and inspect the output, instead of inspect the persisted csv
function run_integration_app() {
  echo "--------- RUN INTEGRATION APP -------------"
  ${SMV_RUN} \
      --smv-props \
      smv.sparkdf.defaultPersistFormat=smvcsv_on_hdfs \
      smv.inputDir="file://$(pwd)/data/input" \
      smv.outputDir="file://$(pwd)/data/output" --run-app \
      -- --master 'local[*]'
}

function test_custom_driver() {
  echo "--------- RUN APPLICATION WITH CUSTOM DRIVER -------------"
  rm -rf data/output/*
  ${SMV_RUN} --smv-props smv.config.keys=custom_key --smv-props smv.config.custom_key=custom_value \
    --script src/main/runners/custom_driver.py arg1 arg2 arg3
  if [ $(count_output) == 0 ]; then
    echo "Driver script wasn't exectuted!"
    exit 1
  fi
}

function validate_integration_app_output() {
  echo "--------- CHECK INTEGRATION APP OUTPUT -------------"

  for stage in $PYTHON_MODULE_STAGES; do
    TEST_INPUT=$(< data/input/$stage/table.csv)
    TEST_OUTPUT=$(cat data/output/integration.test.$stage.modules.M2_*.csv/part*)
    if [[ $TEST_INPUT != $TEST_OUTPUT ]]; then
      echo "Test failure: $stage"
      echo "Expected output:"
      echo $TEST_OUTPUT
      echo "Got:"
      echo $TEST_INPUT
      exit 1
    fi
  done
}


function validate_hash_test_module_cache_invalidation() {
  echo "--------- RUN HASH TEST MODULE -------------"
  rm -rf data/output/*
  ${SMV_RUN} -m $HASH_TEST_MOD \
      --smv-props \
      smv.sparkdf.defaultPersistFormat=smvcsv_on_hdfs

  echo "--------- RERUN UNCHANGED TEST MODULE -------------"
  ${SMV_RUN} -m $HASH_TEST_MOD \
      --smv-props \
      smv.sparkdf.defaultPersistFormat=smvcsv_on_hdfs

  echo "--------- VERIFY HASH UNCHANGED -------------"
  verify_hash_unchanged

  echo "--------- CHANGE MODULE -------------"
  HASH_TEST_PKG=$(sed -e "s/\(.*\)\.[^.]*/\1/g" <<< "$HASH_TEST_MOD")
  HASH_TEST_MOD_FILE="src/main/python/$(sed -e "s/\./\//g" <<< "$HASH_TEST_PKG").py"
  sed -i"" -e "s/table1/table2/" $HASH_TEST_MOD_FILE

  echo "--------- RUN CHANGED MODULE -------------"
  ${SMV_RUN} -m hashtest.modules.M \
      --smv-props \
      smv.sparkdf.defaultPersistFormat=smvcsv_on_hdfs

  echo "--------- VERIFY HASH CHANGED -------------"
  verify_hash_changed 1

  echo "--------- TOUCH INPUT CSV -------------"
  touch data/input/hashtest/table.csv

  echo "--------- RUN MODULE WITH UPDATED CSV -------------"
  ${SMV_RUN} -m hashtest.modules.M \
      --smv-props \
      smv.sparkdf.defaultPersistFormat=smvcsv_on_hdfs

  echo "--------- VERIFY HASH CHANGED -------------"
  verify_hash_changed 2

  echo "--------- CHANGE INPUT SCHEMA -------------"
  sed -i"" -e "s/String/Integer/" data/input/hashtest/table.schema

  echo "--------- RUN MODULE WITH UPDATED SCHEMA -------------"
  ${SMV_RUN} -m hashtest.modules.M \
      --smv-props \
      smv.sparkdf.defaultPersistFormat=smvcsv_on_hdfs

  echo "--------- VERIFY HASH CHANGED -------------"
  verify_hash_changed 3
}

function test_enterprise_app() {
  echo "--------- GENERATE ENTERPRISE APP -------------"
  ${SMV_INIT} -e $E_APP_NAME

  echo "--------- RUN ENTERPRISE APP -------------"
  execute_in_dir "$E_APP_NAME" "${SMV_RUN} --run-app"
}

function test_simple_app() {
  echo "--------- GENERATE SIMPLE APP -------------"
  ${SMV_INIT} -e $S_APP_NAME

  echo "--------- RUN SIMPLE APP -------------"
  execute_in_dir "$S_APP_NAME" "${SMV_RUN} --run-app"
}

echo "--------- INTEGRATION TEST BEGIN -------------"

parse_args $@
setup_virtualenv_if_needed
verify_test_context
enter_clean_test_dir
generate_integration_app
execute_in_dir "${I_APP_NAME}" test_integration_app
test_enterprise_app
test_simple_app

echo "--------- INTEGRATION TEST COMPLETE -------------"

