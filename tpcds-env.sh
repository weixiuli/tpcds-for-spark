#!/bin/bash
bin="$(cd "$(dirname "$0")";pwd)"

# the path to the tpcds-kit directory
export TPCDS_ROOT=$bin/tpcds-kit

# the table list file of tpcds
export tableNameList=$bin/tableList

# HDFS上存放文件的根目录地址
export FLATFILE_HDFS_ROOT=/user/hive/tpcds_data_5
#export FLATFILE_HDFS_ROOT=/user/hdfs/tpcds_data_test
#export FLATFILE_HDFS_ROOT=/user/hive/tpcds_data_1G

# 本地存放文件的根目录地址
export LOCAL_DATA_DIR=$bin/Data

# 原始数据大小(scale factor in GB)
# SF 3000 yields ~1TB for the store_sales table
export TPCDS_SCALE_FACTOR=5

# this is used to determine the number of dsdgen processes to generate data
# usually set to one per physical CPU core
# example - 20 nodes @ 12 threads each
export DSDGEN_NODES=1
export DSDGEN_THREADS_PER_NODE=10
export DSDGEN_TOTAL_THREADS=$((DSDGEN_NODES * DSDGEN_THREADS_PER_NODE))

# Database名称
export TPCDS_DBNAME=tpcds_jd_5
# AE or No_AE
#export RUN_STYLE=AE
#export RUN_STYLE=No_AE

export SPARK_HOME=/home/ess/weixiuli/spark-2.3.0-bin-default

export QUERY_SQL_DIR=$bin/query_sql_$TPCDS_SCALE_FACTOR
export QUERY_RESULT_DIR=$bin/query_result_$TPCDS_SCALE_FACTOR
