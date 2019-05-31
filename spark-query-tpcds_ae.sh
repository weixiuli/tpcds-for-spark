#!/bin/bash
bin=`dirname $0`
bin=`cd $bin;pwd`
# AE or No_AE
export RUN_STYLE='AE'
#export RUN_STYLE=No_AE
source tpcds-env.sh


if [ ! -d $QUERY_SQL_DIR ];then
        echo "query sql is not exist,exit.."
	exit;
fi

if [ -d $QUERY_RESULT_DIR ];then
        rm -rf $QUERY_RESULT_DIR
fi

mkdir $QUERY_RESULT_DIR


#unsupported sql ids
#ids=(28 61 77 88 90)
ids=()

#skip ids
ids2=()

echo "-----------开始查询-----------"
echo "-----------数据库为: $TPCDS_DBNAME------------"
echo "-----------查询spark模式为: $RUN_STYLE------------"

#日期分区
date=$(date +%Y-%m-%d)

#运行类:AE、No_AE
runstyle=$RUN_STYLE

#数据大小
factor=$TPCDS_SCALE_FACTOR

#查询语句临时拷贝
QUERY_SQL_DIR_NEW=$RUN_STYLE

\rm -rf  $QUERY_SQL_DIR_NEW
cp -r $QUERY_SQL_DIR  $QUERY_SQL_DIR_NEW

#exec sql
for (( i=1;i<100;++i ))
do
    yes=1
    for j in ${ids[@]}
    do
        if [ $i -eq $j ]; then
            yes=0
            break;
        fi
    done
    if [ $yes -eq 0 ]; then
        continue
    fi

    for k in ${ids2[@]}
    do
        if [ $i -eq $k ]; then
            yes=0
            break;
        fi
    done
    if [ $yes -eq 0 ]; then
        continue
    fi

    file="$QUERY_SQL_DIR_NEW/query$i.sql"
    echo $file

    # 添加相关特殊sql语句
    sqladd="drop table if exists query_${i}_${runstyle}; create table if not exists query_${i}_${runstyle} as "
    sed -i  "1i \ $sqladd"  $file


    if [ ! -f $file ]; then
        echo "$file is not exist!"
        exit 1
    fi

    # result="$QUERY_RESULT_DIR/query.result"
    result="$QUERY_SQL_DIR_NEW/query.result"
    echo -n "query$i.sql," >> $result
    for(( times=1;times<=1;times++))
    do
	echo ${file}_$times 查询中

	sysout="$QUERY_SQL_DIR_NEW/query${i}_$times.out"

    if [ "$runstyle" = 'AE' ];then
	   $SPARK_HOME/bin/spark-sql --database $TPCDS_DBNAME --name "${file}_select_use_$times" --conf spark.sql.adaptive.enabled=true -f "$file" --silent > $sysout 2>&1
	else
 	   $SPARK_HOME/bin/spark-sql --database $TPCDS_DBNAME --name "${file}_select_use_$times" -f "$file" --silent > $sysout 2>&1

	fi

    time=`cat $sysout | grep "Time taken:" | grep "Driver" | awk -F 'Time taken:' '{print $2}' | awk -F ' ' '{print $1}' | tail -n 1`

    sqlTest="
    set hive.exec.dynamic.partition=true;
    set hive.exec.dynamic.partition.mode=nonstrict;
    set hive.exec.max.dynamic.partitions=2000;
    create table IF NOT EXISTS runtime
    (
        time             string
    )
    partitioned by (factor string,querynum string,runstyle string, dt string)
    stored as parquet;
    insert overwrite table runtime partition(factor, querynum, runstyle, dt) values('$time','$factor','query${i}','$runstyle','$date');
   "
    echo  $sqlTest

    $SPARK_HOME/bin/spark-sql --database $TPCDS_DBNAME --name "${file}_time_use_$times" -e "$sqlTest"  --silent >> $sysout 2>&1

        if [ "$time" = "" ];then
	   echo -n "0," >> $result
	else
 	   echo -n "$time," >> $result
	fi 

    done 
    echo "" >> $result
done

exit 0
