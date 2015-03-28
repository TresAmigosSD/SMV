#!/bin/bash

export SPARK_MEM=2G
ADD_JARS=./target/smv-1.3-SNAPSHOT.jar ~/spark-1.3.0-bin-hadoop1/bin/spark-shell -i shell/shell_init.scala

