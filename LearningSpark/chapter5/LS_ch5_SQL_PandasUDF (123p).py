from pyspark.sql import SparkSession
from pyspark.sql.types import LongType

import pandas as pd
from pyspark.sql.functions import col, pandas_udf

# SparkSession 생성
spark = (SparkSession
            .builder
            .appName("SparkSQLPandasUDF")
            .getOrCreate())

# 큐브 함수 선언
def cubed(a: pd.Series) -> pd.Series:
        return a * a * a

# 큐브 함수에 대한 판다스 UDF 생성
cubed_udf = pandas_udf(cubed, returnType=LongType())

# 판다스 시리즈 생성
x = pd.Series([1, 2, 3])

# 로컬 판다스 데이터를 실행하는 pandas_udf에 대한 함수
print(cubed(x))

# 스파크 데이터 프레임 생성, 'spark'는 기존의 sparkSession과 같다.
df = spark.range(1, 4)

# 백터화된 스파크 UDF를 함수로 실행
df.select("id", cubed_udf(col("id"))).show()