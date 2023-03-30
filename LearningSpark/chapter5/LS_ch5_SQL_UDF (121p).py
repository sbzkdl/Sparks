from pyspark.sql import SparkSession
from pyspark.sql.types import LongType

# SparkSession 생성
spark = (SparkSession
            .builder
            .appName("SparkSQLUDF")
            .getOrCreate())

# 큐브 함수 생성
def cubed(s):
    return s * s * s

# UDF로 등록
spark.udf.register("cubed", cubed, LongType())

# 임시 뷰 생성
spark.range(1, 9).createOrReplaceTempView("udf_test")

# 큐브 UDF를 사용하여 쿼리
spark.sql("""
        SELECT
                id, cubed(id) AS id_cubed
        FROM
                udf_test       
        
        """).show()