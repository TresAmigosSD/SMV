#!/bin/bash

export SPARK_MEM=2G
ADD_JARS=./target/smv-1.0-SNAPSHOT.jar spark-shell -i shell/shell_init.scala

