#!/bin/bash

set -e

if [ ! -d "src/test/scripts" ]; then
    echo "Must run this script from top level SMV directory"
    exit 1
fi

TEST_DIR=target/test-sample
APP_NAME=MyApp
APP_DIR=${TEST_DIR}/${APP_NAME}
MVN=$(type -P mvn || type -P mvn3)

rm -rf ${TEST_DIR}
mkdir -p ${TEST_DIR}
cd ${TEST_DIR}


# Test stages containing a dependency scenario with a Scala output module
NEW_SCALA_MODULE_STAGES="\
test1 \
test3 \
test5 \
"

# Test stages containing a dependency scenario with a Python output module
NEW_PYTHON_MODULE_STAGES="\
test2 \
test4 \
test6 \
"

NEW_MODULE_STAGES="$NEW_PYTHON_MODULE_STAGES $NEW_SCALA_MODULE_STAGES"

echo "--------- GENERATE SAMPLE APP -------------"
../../tools/smv-init -test ${APP_NAME} org.tresamigos.smvtest

echo "--------- BUILD SAMPLE APP -------------"
cd ${APP_NAME}
$MVN clean package

echo "--------- RUN SAMPLE APP -------------"
../../../tools/smv-pyrun \
    --smv-props \
    smv.inputDir="file://$(pwd)/data/input" \
    smv.outputDir="file://$(pwd)/data/output" --run-app \
    -- --master 'local[*]'

echo "--------- VERIFY SAMPLE APP OUTPUT -------------"
COUNT=$(cat data/output/org.tresamigos.smvtest.stage2.StageEmpCategory_*.csv/part* | wc -l)
if [ "$COUNT" -ne 52 ]; then
    echo "Expected 52 lines in output of stage2.StageEmpCategory but got $COUNT"
    exit 1
fi

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

echo "--------- TEST COMPLETE -------------"
