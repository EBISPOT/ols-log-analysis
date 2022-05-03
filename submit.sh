#!/bin/bash
cd "$(dirname "$0")"


export SPARK_HOME=/home/henriette007/tools/spark/spark-3.1.2-bin-hadoop3.2
export SPARK_CONF_DIR=/home/henriette007/ebi-dev/ols-log-analysis/ols-log-analysis/conf

echo SPARK_CONF_DIR=$SPARK_CONF_DIR

$SPARK_HOME/bin/spark-submit \
  --class uk.ac.ebi.spot.BasicLineCount \
  /home/henriette007/ebi-dev/ols-log-analysis/ols-log-analysis/target/scala-2.12/ols-log-analysis.jar /home/henriette007/data
