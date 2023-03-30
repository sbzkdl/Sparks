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

parquet_df.show(10)

# Spark SQL로 읽기
spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW 
                us_delay_flights_tbl_parquet
        USING
                parquet
        OPTIONS
                (PATH "LearningSpark/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet")
        """)

spark.sql("""
        SELECT
                *
        FROM
                us_delay_flights_tbl_parquet
        """).show(10)

# 데이터 프레임을 파케이 파일로 저장
(parquet_df.write.format("parquet")
                 .mode("overwrite")
                 .option("compression", "snappy")
                 .save("LearningSpark/chapter4/data_source_pratice/parquet/df_parquet"))

# Spark SQL 테이블에 데이터 프레임 쓰기
(parquet_df.write
           .mode("overwrite")
           .saveAsTable("us_delay_flights_tbl_parquet"))


# JSON
# 데이터 프레임으로 읽기
json_file = "LearningSpark/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"

json_df = spark.read.format("json").load(json_file)

json_df.show(10)

# Spark SQL로 읽기
spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW 
                us_delay_flights_tbl_json
        USING
                json
        OPTIONS
                (PATH "LearningSpark/databricks-datasets/learning-spark-v2/flights/summary-data/json/*")
        """)

spark.sql("""
        SELECT
                *
        FROM
                us_delay_flights_tbl_json
        """).show(10)

# 데이터 프레임을 JSON 파일로 저장
(json_df.write.format("json")
              .mode("overwrite")
              .option("compression", "snappy")
              .save("LearningSpark/chapter4/data_source_pratice/json/df_json"))


# CSV
# 데이터 프레임으로 읽기
csv_file = "LearningSpark/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*"

schema = """
        DEST_COUNTRY_NAME       STRING,
        ORIGIN_COUNTRY_NAME     STRING,
        count                   INT
        """

csv_df = (spark.read.format("json")
                    .option("header", "true")
                    .schema(schema)
                    .option("mode", "FAILFAST") # 에러 발생 시 종료
                    .option("nullValue", "")    # 모든 null 데이터를 따옴표로 교체
                    .load(csv_file))

csv_df.show(10)

# Spark SQL로 읽기
spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW 
                us_delay_flights_tbl_csv
        USING
                csv
        OPTIONS
                (path           "LearningSpark/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*",
                 header         "true",
                 inferSchema    "true",
                 mode           "FAILFAST")
        """)

spark.sql("""
        SELECT
                *
        FROM
                us_delay_flights_tbl_csv
        """).show(10)

# 데이터 프레임을 CSV 파일로 저장
(csv_df.write.format("csv")
             .mode("overwrite")
             .save("LearningSpark/chapter4/data_source_pratice/csv/df_csv"))


# 에이브로
# 데이터 프레임으로 읽기
avro_df = (spark.read.format("avro")
                     .load("LearningSpark/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*"))

avro_df.show(10, truncate=False)

# Spark SQL로 읽기
spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW 
                episode_tbl
        USING
                avro
        OPTIONS
                (path "LearningSpark/databricks-datasets/learning-spark-v2/flights/summary-data/avro/*")
        """)

spark.sql("""
        SELECT
                *
        FROM
                episode_tbl
        """).show(truncate=False)

# 데이터 프레임을 에이브로 파일로 저장
(avro_df.write.format("avro")
              .mode("overwrite")
              .save("LearningSpark/chapter4/data_source_pratice/avro/df_avro"))


# ORC
# 데이터 프레임으로 읽기
orc_file = "LearningSpark/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*"

orc_df = (spark.read.format("orc")
                    .option("path", orc_file)
                    .load())

orc_df.show(10, False)

# Spark SQL로 읽기
spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW 
                us_delay_flights_tbl_orc
        USING
                orc
        OPTIONS
                (path "LearningSpark/databricks-datasets/learning-spark-v2/flights/summary-data/orc/*")
        """)

spark.sql("""
        SELECT
                *
        FROM
                us_delay_flights_tbl_orc
        """).show(10)

# 데이터 프레임을 ORC 파일로 저장
(orc_df.write.format("orc")
             .mode("overwrite")
             .option("compression", "snappy")
             .save("LearningSpark/chapter4/data_source_pratice/orc/df_orc"))


# 이미지
# 데이터 프레임으로 읽기
from pyspark.ml import image

image_dir = "LearningSpark/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"

image_df = spark.read.format("image").load(image_dir)

# 스키마 보기
image_df.printSchema()

# 이미지 형식 보기
(image_df.select("image.height", "image.width", "image.nChannels", "image.mode", "label")
         .show(5, truncate=False))


# 이진 파일
# 데이터 프레임으로 읽기
binary_path = "LearningSpark/databricks-datasets/learning-spark-v2/cctvVideos/train_images/"

binary_files_df1 = (spark.read.format("binaryFile")
                              .option("pathGlobFilter", "*.jpg")
                              .load(binary_path))

binary_files_df1.show(5)

# 디렉터리에서 파티션 데이터 검색 무시 -> recursiveFileLookup을 "true"로 설정
binary_files_df2 = (spark.read.format("binaryFile")
                              .option("pathGlobFilter", "*.jpg")
                              .option("recursiveFileLookup", "true")
                              .load(binary_path))

binary_files_df2.show(5)