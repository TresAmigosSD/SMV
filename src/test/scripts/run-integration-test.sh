#!/bin/bash

set -e

if [ ! -d "src/test/scripts" ]; then
    echo "Must run this script from top level SMV directory"
    exit 1
fi

TEST_DIR=target/test-sample
I_APP_NAME=IntegrationApp
S_APP_NAME=SimpleApp
E_APP_NAME=EnterpriseApp

rm -rf ${TEST_DIR}
mkdir -p ${TEST_DIR}
cd ${TEST_DIR}

function clear_data_dir() {
  rm -rf data/output/*
}

# Test stages containing a dependency scenario with a Scala output module
NEW_SCALA_MODULE_STAGES="\
test1 \
test3 \
test5 \
test8 \
"

# Test stages containing a dependency scenario with a Python output module
NEW_PYTHON_MODULE_STAGES="\
test2 \
test4 \
test6 \
test7 \
"

NEW_MODULE_STAGES="$NEW_PYTHON_MODULE_STAGES $NEW_SCALA_MODULE_STAGES"

HASH_TEST_MOD="integration.test.hashtest.modules.M"

echo "--------- GENERATE INTEGRATION APP -------------"
../../tools/smv-init -test ${I_APP_NAME}

(
cd ${I_APP_NAME}
echo "--------- BUILD INTEGRATION APP -------------"
sbt clean assembly

echo "--------- RUN INTEGRATION APP -------------"
../../../tools/smv-run \
    --smv-props \
    smv.inputDir="file://$(pwd)/data/input" \
    smv.outputDir="file://$(pwd)/data/output" --run-app \
    -- --master 'local[*]'

echo "--------- CHECK INTEGRATION APP OUTPUT -------------"
for stage in $NEW_SCALA_MODULE_STAGES; do
  TEST_INPUT=$(< data/input/$stage/table.csv)
  TEST_OUTPUT=$(cat data/output/integration.test.$stage.M2_*.csv/part*)
  if [[ $TEST_INPUT != $TEST_OUTPUT ]]; then
    echo "Test failure: $stage"
    echo "Expected output:"
    echo $TEST_INPUT
    echo "Got:"
    echo $TEST_OUTPUT
    exit 1
  fi
done

for stage in $NEW_PYTHON_MODULE_STAGES; do
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

echo "--------- RUN HASH TEST MODULE -------------"
clear_data_dir
../../../tools/smv-run -m $HASH_TEST_MOD

echo "--------- RERUN UNCHANGED TEST MODULE -------------"
../../../tools/smv-run -m $HASH_TEST_MOD

echo "--------- VERIFY HASH UNCHANGED -------------"
verify_hash_unchanged

echo "--------- CHANGE MODULE -------------"
HASH_TEST_PKG=$(sed -e "s/\(.*\)\.[^.]*/\1/g" <<< "$HASH_TEST_MOD")
HASH_TEST_MOD_FILE="src/main/python/$(sed -e "s/\./\//g" <<< "$HASH_TEST_PKG").py"
sed -i"" -e "s/table1/table2/" $HASH_TEST_MOD_FILE

echo "--------- RUN CHANGED MODULE -------------"
../../../tools/smv-run -m hashtest.modules.M

echo "--------- VERIFY HASH CHANGED -------------"
verify_hash_changed 1

echo "--------- TOUCH INPUT CSV -------------"
touch data/input/hashtest/table.csv

echo "--------- RUN MODULE WITH UPDATED CSV -------------"
../../../tools/smv-run -m hashtest.modules.M

echo "--------- VERIFY HASH CHANGED -------------"
verify_hash_changed 2

echo "--------- CHANGE INPUT SCHEMA -------------"
sed -i"" -e "s/String/Integer/" data/input/hashtest/table.schema

echo "--------- RUN MODULE WITH UPDATED SCHEMA -------------"
../../../tools/smv-run -m hashtest.modules.M

echo "--------- VERIFY HASH CHANGED -------------"
verify_hash_changed 3

echo "--------- RUN APPLICATION WITH CUSTOM DRIVER -------------"
clear_data_dir
../../../tools/smv-run --smv-props smv.config.keys=custom_key --smv-props smv.config.custom_key=custom_value \
    --script src/main/runners/custom_driver.py arg1 arg2 arg3
if [ $(count_output) == 0 ]; then
  echo "Driver script wasn't exectuted!"
  exit 1
fi
)

echo "--------- GENERATE ENTERPRISE APP -------------"
../../tools/smv-init -e $E_APP_NAME

(
cd $E_APP_NAME
echo "--------- RUN ENTERPRISE APP -------------"
../../../tools/smv-run --run-app
)

echo "--------- GENERATE SIMPLE APP -------------"
../../tools/smv-init -s $S_APP_NAME

(
cd $S_APP_NAME
echo "--------- RUN SIMPLE APP -------------"
../../../tools/smv-run --run-app
)

echo "--------- TEST COMPLETE -------------"
