#!/bin/bash

# Spark/Yarn configuration parameters.
EXECUTOR_MEMORY="6G"
EXECUTOR_CORES=10
MAX_EXECUTORS=6
DRIVER_MEMORY="2G"
YARN_QUEUE="myqueue"
DATA_DIR="hdfs:///blahblah/data"
HIVE_SCHEMA="xxx."
export HIVE_SCHEMA

# ADD SMV dir to path when run from deployment script
if [ -d SMV ]; then
  export PATH="`pwd`/SMV/tools:${PATH}"
fi

LOGFILE="$(date '+esbrit_run_log.%Y%m%d%H%M%S')"

smv-run --data-dir "${DATA_DIR}" --publish-hive --run-app \
  -- \
  --master yarn-client \
  --executor-memory ${EXECUTOR_MEMORY} \
  --conf spark.dynamicAllocation.maxExecutors=${MAX_EXECUTORS} \
  --driver-memory ${DRIVER_MEMORY} \
  --conf spark.yarn.queue=${YARN_QUEUE} \
  --conf spark.executor.cores=${EXECUTOR_CORES} \
  2>&1 | tee $LOGFILE
