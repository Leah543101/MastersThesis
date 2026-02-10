#!/bin/bash

EVENT_LOG_DIR="file:///$(pwd)/sparktestexperiments"

mkdir -p "$(pwd)/sparktestexperiments"


MASTER="local[4]"                # or yarn, spark://host:7077, k8s://...
APP_NAME="SparkTestApp"
DRIVER_MEMORY="2g"
DRIVER_CORES=2
#EXECUTOR_MEMORY="3g"
#EXECUTOR_CORES=2
#NUM_EXECUTORS=2
#EXECUTOR_MEMORY_OVERHEAD="512m"
EVENT_LOG_CODEC="zstd"  



spark-submit \
--master $MASTER \
--name $APP_NAME \
--driver-memory $DRIVER_MEMORY \
--driver-cores $DRIVER_CORES \
app.py
#--executor-memory $EXECUTOR_MEMORY \
#--executor-cores $EXECUTOR_CORES \
#--num-executors $NUM_EXECUTORS \
#--conf "spark.executor.memoryOverhead=$EXECUTOR_MEMORY_OVERHEAD" \
#--conf "spark.eventLog.enabled=true" \
#--conf "spark.eventLog.dir=$EVENT_LOG_DIR" \
#--conf "spark.eventLog.compression.codec=$EVENT_LOG_CODEC" \
