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

# For now, need to run original integration test modules AND the new dependency
# scenario classes, then check output. New output check will be one module per stage
OLD_PASSING_PYTHON_MODULES="com.mycompany.MyApp.stage1.employment.PythonEmploymentByState \
com.mycompany.MyApp.stage1.employment.PythonEmploymentByStateCategory \
com.mycompany.MyApp.stage1.employment.PythonEmploymentByStateCategory2 \
com.mycompany.MyApp.stage2.category.PythonEmploymentByStateCategory \
"

OLD_FAILING_PYTHON_MODULES=" \
com.mycompany.MyApp.stage2.category.PythonEmploymentByStateCategory2 \
"

# Test stages containing a dependency scenario with a Scala output module
NEW_SCALA_MODULE_STAGES="test1"

# Test stages containing a dependency scenario with a Python output module
NEW_PYTHON_MODULE_STAGES="test2"

PYTHON_MODULES_TO_RUN=$OLD_PASSING_PYTHON_MODULES
for stage in $NEW_PYTHON_MODULE_STAGES; do
  PYTHON_MODULES_TO_RUN="$PYTHON_MODULES_TO_RUN com.mycompany.MyApp.$stage.modules.M2"
done

NEW_MODULE_STAGES="$NEW_PYTHON_MODULE_STAGES $NEW_SCALA_MODULE_STAGES"

echo "--------- GENERATE SAMPLE APP -------------"
../../tools/smv-init -test ${APP_NAME} com.mycompany.${APP_NAME}

echo "--------- BUILD SAMPLE APP -------------"
cd ${APP_NAME}
$MVN clean package

echo "--------- RUN SAMPLE APP -------------"
../../../tools/smv-pyrun --smv-props \
    smv.inputDir="file://$(pwd)/data/input" \
    smv.outputDir="file://$(pwd)/data/output" --run-app \
    -- --master 'local[*]'


echo "--------- FORCE RUN PYTHON MODULES -------------"
# The Python modules which are not dependencies of Scala modules won't run
# unless run explicitly with -m

echo "Skipping failing Python example modules: $OLD_FAILING_PYTHON_MODULES"

../../../tools/smv-pyrun -m $PYTHON_MODULES_TO_RUN


echo "--------- VERIFY SAMPLE APP OUTPUT -------------"
COUNT=$(cat data/output/com.mycompany.MyApp.stage2.StageEmpCategory_*.csv/part* | wc -l)
if [ "$COUNT" -ne 52 ]; then
    echo "Expected 52 lines in output of stage2.StageEmpCategory but got $COUNT"
    exit 1
fi

for stage in $NEW_SCALA_MODULE_STAGES; do
  TEST_INPUT=$(< data/input/$stage/table.csv)
  TEST_OUTPUT=$(cat data/output/com.mycompany.MyApp.$stage.M2_*.csv/part*)
  if [[ $TEST_INPUT != $TEST_OUTPUT ]]; then
    echo "Test failure: $stage"
    exit 1
  fi
done

for stage in $NEW_PYTHON_MODULE_STAGES; do
  TEST_INPUT=$(< data/input/$stage/table.csv)
  TEST_OUTPUT=$(cat data/output/com.mycompany.MyApp.$stage.modules.M2_*.csv/part*)
  if [[ $TEST_INPUT != $TEST_OUTPUT ]]; then
    echo "Test failure: $stage"
    exit 1
  fi
done

echo "--------- TEST COMPLETE -------------"
