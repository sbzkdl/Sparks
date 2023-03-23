from pyspark.sql.types import *

# 프로그래밍 스타일
schema = StructType([StructField("author", StringType(), False),
         StructField("title", StructType(), False),
         StructField("pages", IntegerType(), False)])

# DDL(Data Definition Language) 사용
schema = "author STRING, title STRING, pages INT"