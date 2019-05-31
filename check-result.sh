#!/bin/bash
bin=`dirname $0`
bin=`cd $bin;pwd`

export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.6-src.zip:$PYTHONPATH
#数据库
DATABASE=tpcds_jd_5
#unsupported sql ids
#ids=(28 61 77 88 90)
ids=()
#skip ids
ids2=()
#日期分区
DATE=$(date +%Y%m%d)
#查询结果存放目录
CHECK_RESULT="RESULT/$DATE"
echo  $CHECK_RESULT
\rm -rf  $CHECK_RESULT
mkdir -p  $CHECK_RESULT
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
    sysout="$CHECK_RESULT/query${i}.out"
    queryAE="query_${i}_ae"
    queryNoAE="query_${i}_no_ae"

    result="$CHECK_RESULT/query.result"
    echo -n "$i," >> $result
    # 执行对数脚本
    python DiffResult.py $queryAE $queryNoAE $DATABASE > $sysout 2>&1
    # 对数结果
    checkResult=`cat $sysout | grep "hive and spark result equal!" | wc -l`
    if [ "$checkResult" != "1" ];then
       echo "query_${i} check result is NO;"
	   echo -n "0" >> $result
	else
	   echo "query_${i} check result is OK;"
 	   echo -n "1" >> $result
	fi
    echo "" >> $result
done

exit 0
