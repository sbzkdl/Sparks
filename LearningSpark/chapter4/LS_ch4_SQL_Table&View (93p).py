from pyspark.sql import SparkSession

# SparkSession 생성
spark = (SparkSession
            .builder
            .appName("LearnSparkDBApp")
            .getOrCreate())

# 데이터베이스 생성
spark.sql("CREATE DATABASE learn_spark_db")
spark.sql("USE learn_spark_db")

# 관리형 테이블 생성
# 1
spark.sql("""
        CREATE TABLE 
                managed_us_delay_flights_tbl
                (date           STRING, 
                 delay          INT, 
                 distance       INT, 
                 origin         STRING, 
                 destination    STRING)
        """)
# 2 : 데이터 프레임 API 사용
csv_file = "LearningSpark/databricks-datasets/learning-spark-v2/flights/departuredelays.csv" # 데이터 경로

schema = """
        date           STRING, 
        delay          INT, 
        distance       INT, 
        origin         STRING, 
        destination    STRING
        """ # 스키마 작성

flights_df = spark.read.csv(csv_file, schema=schema) # 데이터 읽기

flights_df.write.saveAsTable("managed_us_delay_flights_tbl") # 관리형 테이블 생성

# 비관리형 테이블 생성
# 1
spark.sql("""
        CREATE TABLE 
                us_delay_flights_tbl
                (date           STRING, 
                 delay          INT, 
                 distance       INT, 
                 origin         STRING, 
                 destination    STRING)
        USING
                csv
        OPTIONS
                (PATH "LearningSpark/databricks-datasets/learning-spark-v2/flights/departuredelays.csv")
        """)
# 2 : 데이터 프레임 API 사용
(flights_df
    .write
    .option("path", "LearningSpark/databricks-datasets/learning-spark-v2/flights/departuredelays.csv")
    .saveAsTable("us_delay_flights_tbl"))


# 뉴욕 및 샌프란시스코가 출발지인 공항이 있는 데이터만 보기 위한 뷰 생성
df_sfo = spark.sql("""
                SELECT
                        date, delay, origin, destination
                FROM
                        us_delay_flights_tbl
                WHERE
                        origin = 'SFO'
                """)

df_jfk = spark.sql("""
                SELECT
                        date, delay, origin, destination
                FROM
                        us_delay_flights_tbl
                WHERE
                        origin = 'JFK'
                """)
# 전역 임시 뷰 및 일반 임시 뷰 생성
df_sfo.createOrReplaceGlobalTempView("us_origin_airport_SFO_global_tmp_view")
df_jfk.createOrReplaceTempView("us_origin_airport_JFK_tmp_view")

# 뷰 접근
# 전역 임시 뷰 : global_temp를 통해 접근
spark.sql("""
        SELECT
                *
        FROM
                global_temp.us_origin_airport_SFO_global_tmp_view
        """)
# 일반 임시 뷰
# 1
spark.read.table("us_origin_airport_JFK_tmp_view")
# 2
spark.sql("""
        SELECT
                *
        FROM
                us_origin_airport_JFK_tmp_view
        """)

# 뷰 드롭
spark.catalog.dropGlobalTempView("global_temp.us_origin_airport_SFO_global_tmp_view")
spark.catalog.dropTempView("us_origin_airport_JFK_tmp_view")

# 메타데이터 보기
spark.catalog.listDatabases()
spark.catalog.listTables()
spark.catalog.listColumns("us_delay_flights_tbl")

# 테이블을 데이터 프레임으로 읽기
us_flights_df = spark.sql("SELECT * FROM us_delay_flights_tbl")
us_flights_df2 = spark.table("us_delay_flights_tbl")

us_flights_df.show(10)
us_flights_df2.show(10)