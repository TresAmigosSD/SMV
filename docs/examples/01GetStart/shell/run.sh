#!/bin/bash

if [ $# -ne 1 ]; then
  echo "$0 _data_dir_"
  exit 1
fi

export DATA_DIR="$1"

SPARK_SHELL="${HOME}/spark-1.3.0/bin/spark-shell"
${SPARK_SHELL} --executor-memory 2g --jars ./target/getstart-1.0-SNAPSHOT-jar-with-dependencies.jar -i shell/shell_init.scala

