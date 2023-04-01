from pyspark.sql import SparkSession
from pyspark.sql.types import *

# SparkSession 생성
spark = (SparkSession
            .builder
            .appName("SparkSQLHOF")
            .getOrCreate())

schema = StructType([StructField("celsius", ArrayType(IntegerType()))])

t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
t_c = spark.createDataFrame(t_list, schema)
t_c.createOrReplaceTempView("tC")

# 데이터 프레임 출력
t_c.show()

# 온도의 배열에 대해 섭씨를 화씨로 계산
spark.sql("""
        SELECT
                celsius,
                transform(celsius, t -> ((t * 9) div 5) + 32) AS fahrenheit
        FROM
                tC
        """).show()

# 온도의 배열에 대해 섭씨 38도 이상을 필터
spark.sql("""
        SELECT
                celsius,
                filter(celsius, t -> t >38) AS high
        FROM
                tC
        """).show()

# 온도의 배열에 섭씨 38도의 온도가 있는가?
spark.sql("""
        SELECT
                celsius,
                exists(celsius, t -> t = 38) AS threshold
        FROM
                tC
        """).show()

# 온도의 평균을 계산하고 화씨로 변환
spark.sql("""
        SELECT
                celsius,
                reduce(celsius, 
                       0, 
                       (t, acc) -> t + acc, 
                       acc -> (acc div size(celsius) * 9 div 5) + 32
                       ) AS avgFahrenheit
        FROM
                tC
        """).show() # -> 안 됨.