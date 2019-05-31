#!/bin/bash
bin=`dirname $0`
bin=`cd $bin;pwd`

source tpcds-env.sh
# AE or No_AE
#RUN_STYLE=AE
RUN_STYLE='No_AE'
# ./spark-query-tpcds.sh 1
if [ "$1" = "1" ];then
   RUN_STYLE='AE'
else
   RUN_STYLE='No_AE'
fi

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
echo "-----------spark运行模式为: $RUN_STYLE------------"

#日期分区
date=$(date +%Y-%m-%d)

#运行模式:AE、No_AE
runStyle=$RUN_STYLE

#数据大小
factor=$TPCDS_SCALE_FACTOR

#查询语句临时拷贝
QUERY_SQL_DIR_NEW=$RUN_STYLE

#运行次数
runTimes=1
\rm -rf  $QUERY_SQL_DIR_NEW

#将querysql语句放到相应的目录下
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

    # 添加相关特殊sql语句,目的是将查询结果直接写入数据库，为后续对数做准备
    sqlAdd="drop table if exists query_${i}_${runStyle}; create table if not exists query_${i}_${runStyle} as "
    sed -i  "1i \ $sqlAdd"  $file

    if [ ! -f $file ]; then
        echo "$file is not exist!"
        exit 1
    fi

    result="$QUERY_SQL_DIR_NEW/query.result"
    echo -n "query$i.sql," >> $result

    #目前执行一次，执行多次只是为了统计运行时间
    for(( times=1;times<=$runTimes;times++))
    do
	echo ${file}_$times 查询中------

	sysout="$QUERY_SQL_DIR_NEW/query${i}_$times.out"

	if [ "$runStyle" = 'AE' ];then
	   $SPARK_HOME/bin/spark-sql --database $TPCDS_DBNAME --name "${file}_select_use_$times" --conf spark.sql.adaptive.enabled=true -f "$file" --silent > $sysout 2>&1
	else
 	   $SPARK_HOME/bin/spark-sql --database $TPCDS_DBNAME --name "${file}_select_use_$times" -f "$file" --silent > $sysout 2>&1

	fi

    runTime=`cat $sysout | grep "Time taken:" | grep "Driver" | awk -F 'Time taken:' '{print $2}' | awk -F ' ' '{print $1}' | tail -n 1`

    # 注意分区设置
    # 0、timesnum：运行num
    # 1、factor：数据放大倍数(基准1G)；
    # 2、querynum：sql编号(1~99)
    # 3、runstyle：AE/No_Ae
    # 4、dt：date
    sqlText="
        set hive.exec.dynamic.partition=true;
        set hive.exec.dynamic.partition.mode=nonstrict;
        set hive.exec.max.dynamic.partitions=2000;
        create table IF NOT EXISTS run_time
        (
            runtime             double
        )
        partitioned by (timesnum int, factor string, querynum string, runstyle string, dt string)
        stored as parquet;
        insert overwrite table run_time partition(timesnum, factor, querynum, runstyle, dt) select if('$runTime'='',0.0,${runTime}),$times,'$factor','query${i}','$runStyle','$date';
       "
    echo  $sqlText

    $SPARK_HOME/bin/spark-sql --database $TPCDS_DBNAME --name "${file}_time_use_$times" -e "$sqlText"  --silent >> $sysout 2>&1

    if [ "$time" = "" ];then
	   echo -n "0," >> $result
	else
 	   echo -n "$time," >> $result
	fi 

    done 
    echo "" >> $result
done

exit 0
