# 一、环境部署及相关配置

机器：10.198.46.229~233
下面操作在229机器上操作即可
## 1. mysql安装及配置
-略
## 2. hive安装及配置
- 详见hive-site.xml 配置,所在目录229/usr/local/apache-hive-2.3.5/conf

## 3. spark环境部署
- 将spark-2.3.0-bin-default.tgz 放到 /home/ess/weixiuli/下
- 运行./init.sh、reset.sh完成部署安装
- 注意：相关操作在root用户即可

# 二、执行步骤

## 1. 设置环境变量
```shell
vi tpcds-env.sh
详见：tpcds-env.sh相关说明
```
- 数据量
- 环境变量
- 设置数据生成节点```vi nodenum.sh```
- 注意：tpcds-env.sh 与 nodenum.sh 相关配置直接影响到数据大小生成，详见相关脚本说明


## 2. 生成测试数据

```shell
cd tpcds-kit/tools
make clean
make
cd ../..
./gen-data.sh
```

## 3. 创建hdfs数据目录

```shell
./hdfs-mkdirs.sh
```

## 4. 上传数据到hdfs

```shell
./upload-data.sh
```

## 5. 创建外部表
```shell
create-external-tables.sh
```

## 6. 创建对应的分区表、并对事实表进行格式化、压缩

```shell
create-parquet-partition-tables.sh
```
## 7. 生成查询sql

```shell
./gen-sql.sh
```

## 8. 创建spark任务运行时间表

```shell
create table IF NOT EXISTS run_time
(
    runtime             double
)
partitioned by (timesnum int, factor string, querynum string, runstyle string, dt string)

stored as parquet;
```
- 注意分区设置
- 0、timesnum：运行num
- 1、factor：数据放大倍数(基准1G)；
- 2、querynum：sql编号(1~99)
- 3、runstyle：AE/No_Ae
- 4、dt：date

## 9. spark-sql Ae/No-Ae双跑测试(两种模式可以并行)

```shell

1、基于AE模式运行： ./spark-query-tpcds.sh 1 > spark-query-tpcds-ae.log 2>&1 &

2、基于非AE模式运行:./spark-query-tpcds.sh  > spark-query-tpcds-no-ae.log 2>&1 &

```

## 10. 基于AE与非AE对数验证
```shell
./check-result.sh
```
- 对数结果：RESULT/20190531/query.result
- 对数说明: queryid,1/0(1:正确、0:错误)
- 例如：
```
1,1
2,1
3,1
```

## 11. 其他补充说明

当前tpcds-kit目录中的query_templates已为最新修改完成后的templates,并做了相关语法修改,目前适配Spark.


