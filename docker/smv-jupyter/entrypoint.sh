#!/bin/bash

# Entrypoint must identify find app jar, since the kernel skips smv-shell script

APP_JAR=`ls -1t target/scala-2.10/*jar-with-dependencies.jar 2>/dev/null| head -1`

# if not found try mvn-build location next
if [ -z "$APP_JAR" ]; then
APP_JAR=`ls -1t target/*jar-with-dependencies.jar 2>/dev/null| head -1`
fi

if [ -z "$APP_JAR" ]; then
    echo "ERROR: could not find an app jar in target directory"
    exit 1
fi

export PYSPARK_SUBMIT_ARGS="--verbose --executor-memory 6G --driver-memory 2G --master local[*] --jars $APP_JAR --driver-class-path $APP_JAR pyspark-shell"

jupyter notebook "$@" --ip=0.0.0.0
