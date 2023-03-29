from pyspark.sql import SparkSession

# SparkSession 생성
spark = (SparkSession
            .builder
            .appName("SparkDataSource")
            .getOrCreate())

# 파케이
# 데이터 프레임으로 읽기
parquet_file = "LearningSpark/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet"

parquet_df = spark.read.format("parquet").load(parquet_file)

parquet_df.show()

# Spark SQL 테이블로 읽기
spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW 
                us_delay_flights_tbl
        USING
                parquet
        OPTIONS
                (PATH "LearningSpark/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet")
        """)

spark.sql("""
        SELECT
                *
        FROM
                us_delay_flights_tbl
        """).show()

# 데이터 프레임을 파케이 파일로 저장
(parquet_df.write.format("parquet")
                 .mode("overwrite")
                 .option("compression", "snappy")
                 .save("LearningSpark/chapter4/data_source_pratice/df_parquet"))

# Spark SQL 테이블에 데이터 프레임 쓰기
(parquet_df.write
           .mode("overwrite")
           .saveAsTable("us_delay_flights_tbl"))