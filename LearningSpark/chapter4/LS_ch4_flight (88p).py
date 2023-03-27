from pyspark.sql import SparkSession

# SparkSession 생성
spark = (SparkSession
            .builder
            .appName("SparkSQLExampleApp")
            .getOrCreate())

# 데이터 경로
csv_file = "LearningSpark/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"

# 읽고 임시뷰를 생성
# 스키마 추론 (더 큰 파일의 경우 스키마를 지정하자)
df = (spark.read.format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load(csv_file))

# 임시뷰(테이블) 생성
df.createOrReplaceTempView("us_delay_flights_tbl")

# 비행거리가 1,000마일 이상인 모든 항공편
# SQL문
spark.sql("""
        SELECT
                distance, origin, destination
        FROM
                us_delay_flights_tbl
        WHERE
                distance > 1000
        ORDER BY
                distance DESC
        """).show(10)
# Spark API
# 1
from pyspark.sql.functions import col, desc
(df
   .select("distance", "origin", "destination")
   .where(col("distance") > 1000)
   .orderBy(desc("distance"))
).show(10)
#2
(df
   .select("distance", "origin", "destination")
   .where("distance > 1000")
   .orderBy("distance", ascending=False)
   .show(10)
)

# 샌프란시스코와 시카고 간 2시간 이상 지연이 있었던 모든 항공편
spark.sql("""
        SELECT
                date, delay, origin, destination
        FROM
                us_delay_flights_tbl
        WHERE
                delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
        ORDER BY
                delay DESC
        """).show(10)

# 모든 미국 항공편에 매우 긴 지연(> 6시간), 긴 지연(2~6시간) 등의 지연
spark.sql("""
        SELECT
                delay, origin, destination, 
        CASE
                WHEN 
                        delay > 360 THEN 'Very Long Delays'
                WHEN 
                        delay >= 120 AND delay <= 360 THEN 'Long Delays'
                WHEN 
                        delay >= 60 AND delay < 120 THEN 'Short Delays'
                WHEN 
                        delay > 0 AND delay < 60 THEN 'Tolerable Delays'
                WHEN 
                        delay = 0 THEN 'No Delays'
                ELSE 'Early'
        END AS 
                Flight_Delays
        FROM
                us_delay_flights_tbl
        ORDER BY
                origin, delay DESC                        

        """).show(10)