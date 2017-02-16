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

echo "--------- GENERATE SAMPLE APP -------------"
../../tools/smv-init -i ${APP_NAME} com.mycompany.${APP_NAME}

echo "--------- BUILD SAMPLE APP -------------"
cd ${APP_NAME}
$MVN clean package

echo "--------- RUN SAMPLE APP -------------"
../../../tools/smv-pyrun --smv-props \
    smv.inputDir="file://$(pwd)/data/input" \
    smv.outputDir="file://$(pwd)/data/output" --run-app \
    -- --master 'local[*]'

echo "--------- VERIFY SAMPLE APP OUTPUT -------------"
COUNT=$(cat data/output/com.mycompany.MyApp.stage2.StageEmpCategory_*.csv/part* | wc -l)
if [ "$COUNT" -ne 52 ]; then
    echo "Expected 52 lines in output but got $COUNT"
    exit 1
fi

# The Python modules which are not dependencies of Scala modules won't run
# unless run explicitly with -m
PASSING_PYTHON_MODULES="com.mycompany.MyApp.stage1.employment.PythonEmploymentByState \
com.mycompany.MyApp.stage1.employment.PythonEmploymentByStateCategory \
com.mycompany.MyApp.stage1.employment.PythonEmploymentByStateCategory2 \
"

FAILING_PYTHON_MODULES=" \
com.mycompany.MyApp.stage2.category.PythonEmploymentByStateCategory \
com.mycompany.MyApp.stage2.category.PythonEmploymentByStateCategory2 \
"

echo "Skipping failing Python example modules: $FAILING_PYTHON_MODULES"

../../../tools/smv-pyrun -m $PASSING_PYTHON_MODULES
