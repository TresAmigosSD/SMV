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
MVN=$(type -P mvn || type -P mvn3)

rm -rf ${TEST_DIR}
mkdir -p ${TEST_DIR}
cd ${TEST_DIR}

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

HASH_TEST_MOD="org.tresamigos.smvtest.hashtest.modules.M"

echo "--------- GENERATE INTEGRATION APP -------------"
../../tools/smv-init -test ${I_APP_NAME}

(
cd ${I_APP_NAME}
echo "--------- BUILD INTEGRATION APP -------------"
sbt clean assembly

echo "--------- RUN INTEGRATION APP -------------"
../../../tools/smv-pyrun \
    --smv-props \
    smv.inputDir="file://$(pwd)/data/input" \
    smv.outputDir="file://$(pwd)/data/output" --run-app \
    -- --master 'local[*]'

echo "--------- CHECK INTEGRATION APP OUTPUT -------------"
for stage in $NEW_SCALA_MODULE_STAGES; do
  TEST_INPUT=$(< data/input/$stage/table.csv)
  TEST_OUTPUT=$(cat data/output/org.tresamigos.smvtest.$stage.M2_*.csv/part*)
  if [[ $TEST_INPUT != $TEST_OUTPUT ]]; then
    echo "Test failure: $stage"
    exit 1
  fi
done

for stage in $NEW_PYTHON_MODULE_STAGES; do
  TEST_INPUT=$(< data/input/$stage/table.csv)
  TEST_OUTPUT=$(cat data/output/org.tresamigos.smvtest.$stage.modules.M2_*.csv/part*)
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
  if [ $(count_output) -le 1 ]; then
    echo "Changed module's hashOfHash didn't change"
    exit 1
  fi
}

echo "--------- RUN HASH TEST MODULE -------------"
rm -rf data/output/*
../../../tools/smv-pyrun -m $HASH_TEST_MOD

echo "--------- RERUN UNCHANGED TEST MODULE -------------"
../../../tools/smv-pyrun -m $HASH_TEST_MOD

echo "--------- VERIFY HASH UNCHANGED -------------"
verify_hash_unchanged

echo "--------- CHANGE MODULE -------------"
HASH_TEST_PKG=$(sed -e "s/\(.*\)\.[^.]*/\1/g" <<< "$HASH_TEST_MOD")
HASH_TEST_MOD_FILE="src/main/python/$(sed -e "s/\./\//g" <<< "$HASH_TEST_PKG").py"
sed -i "s/table1/table2/" $HASH_TEST_MOD_FILE

echo "--------- RUN CHANGED MODULE -------------"
../../../tools/smv-pyrun -m hashtest.modules.M

echo "--------- VERIFY HASH CHANGED -------------"
verify_hash_changed
)


echo "--------- GENERATE ENTERPRISE APP APP -------------"
../../tools/smv-init -e $E_APP_NAME

(
cd $E_APP_NAME
echo "--------- RUN ENTERPRISE APP -------------"
../../../tools/smv-pyrun --run-app
)

echo "--------- GENERATE SIMPLE APP -------------"
../../tools/smv-init -s $S_APP_NAME

(
cd $S_APP_NAME
echo "--------- RUN SIMPLE APP -------------"
../../../tools/smv-pyrun --run-app
)

echo "--------- TEST COMPLETE -------------"
