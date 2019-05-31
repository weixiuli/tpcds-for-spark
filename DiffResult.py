# coding=UTF-8
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import udf
import pyspark.sql.functions as func
from pyspark.sql.window import Window

import re
import sys
reload(sys)
sys.setdefaultencoding('utf8')

# 必须输入表名当参数
if len(sys.argv) != 4:
    print "Please input two tables for diff."
    exit()

# 准备运行spark
spark = SparkSession \
        .builder \
        .enableHiveSupport() \
        .master("local") \
        .appName("DiffResult") \
        .getOrCreate()

# 匹配一个字符串，如果是数字和小数点组成，那么将其四舍五入一下
def toDouble(column):
    try:
        # pattern = re.compile(r'^[-+]?[-0-9]\d*\.\d*$')
        pattern = re.compile(r'^-?\d+\.\d+([E|e]\d+)?$')
        m = pattern.match(str(column))
        if m:
            result = 0.0
            result = round(float(m.group()), 2)
            return str(result)
        else:
            return column
    except UnicodeEncodeError:
        return column

toDoubleUDF = udf(toDouble, StringType())

# 对结果集进行四舍五入操作
def toRound(dataFrame):
    # 循环遍历所有列
    for schema in dataFrame.schema:
        # 将字符串类型的数字四舍五入
        if schema.dataType == StringType():
            dataFrame = dataFrame.withColumn(schema.name, toDoubleUDF(func.col(schema.name)))
        # 将数字四舍五入
        if schema.dataType == DoubleType():
            dataFrame = dataFrame.withColumn(schema.name, func.round(func.col(schema.name), 2))
    return dataFrame

# 获取hive的结果
dfHive  = spark.sql("select * from "+sys.argv[3]+"." + sys.argv[1])
# 获取spark的结果
dfSpark = spark.sql("select * from "+sys.argv[3]+"." + sys.argv[2])
# 这俩结果集数量都不一致，就别再继续其他操作了
if dfHive.count() != dfSpark.count():
    print "hive and spark result count not equal! \n"
    exit()

# 对hive结果进行四舍五入操作
dfHive = toRound(dfHive)
dfSpark = toRound(dfSpark)

# 求两者交集, 没有结果表示两者完全一致
subtractHive = dfHive.subtract(dfSpark)
subtractSpark = dfSpark.subtract(dfHive)
if subtractHive.count() == 0 and subtractSpark.count() == 0:
    print "hive and spark result equal! \n"
    exit()

# 不一致查询一下为啥不一致
# 排序
sortedDfHive = subtractHive.withColumn("id", func.row_number().over(Window.orderBy(dfHive.columns)))
sortedDfSpark = subtractSpark.withColumn("id", func.row_number().over(Window.orderBy(dfSpark.columns)))
# 获取第一行
hiveHeadRow = sortedDfHive.first().asDict()
sparkHeadRow = sortedDfSpark.first().asDict()
# print hiveHeadRow
# print sparkHeadRow
# 对比并输出
for k in hiveHeadRow.keys():
    if hiveHeadRow[k] != sparkHeadRow[k]:
        print ("hive and spark ["),
        print (k),
        print ("] not equal: hive is ["),
        print (hiveHeadRow[k]),
        print ("] spark is ["),
        print sparkHeadRow[k],
        print ("]")


# export PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.6-src.zip:$PYTHONPATH

