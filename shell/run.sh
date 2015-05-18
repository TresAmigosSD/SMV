#!/bin/bash

SPARK_SHELL="${HOME}/spark-1.3.0/bin/spark-shell"
JARS="./target/smv-1.3-SNAPSHOT.jar" 

# Other jars can be added like this:
JARS+=",${HOME}/.m2/repository/joda-time/joda-time/2.7/joda-time-2.7.jar"

${SPARK_SHELL} --executor-memory 2g --jars ${JARS} -i shell/shell_init.scala

