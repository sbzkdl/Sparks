from pyspark.sql import SparkSession
from pyspark.sql.types import *

# SparkSession 생성
spark = (SparkSession
            .builder
            .appName("SparkSQLHOF")
            .getOrCreate())

schema = StructType([StructField("celsius", ArrayType(IntergerType()))])

t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
t_c = spark.createDataFrame(t_list, schema)
t_c.createOrReplaceTempView("tC")

# 데이터 프레임 출력
t_c.show()