#!/bin/bash

SMV_TOOLS="$(cd "`dirname "$0"`"; pwd)"
source $SMV_TOOLS/_env.sh

APP_SHELL_INIT=""
if [ -r "conf/shell_init.scala" ]; then
    APP_SHELL_INIT="-i conf/shell_init.scala"
else
    echo "WARNING: app level conf/shell_init.scala not found."
    echo
fi

# TODO: use --executor-memory or whatever it is called instead.
export SPARK_MEM=6G
spark-shell --jars "$APP_JAR" -i "${SMV_TOOLS}/conf/smv_shell_init.scala" $APP_SHELL_INIT
