from pyspark.sql import SparkSession

# SparkSession 생성
spark = (SparkSession
            .builder
            .appName("DFR&DFW")
            .getOrCreate())

# DataFrameReader
# 파케이 사용
file = "LearningSpark/databricks-datasets/learning-spark-v2/flights/summary-data/parquet/2010-summary.parquet"

parquet_df1 = spark.read.format("parquet").load(file) 

# 파케이가 기본 설정인 경우 format("parquet") 생략 가능
parquet_df2 = spark.read.load(file) 

parquet_df1.show(10)
parquet_df2.show(10)

# CSV 사용
csv_df = (spark.read.format("csv")
                   .option("inferSchema", "true")
                   .option("header", "true")
                   .option("mode", "PERMISSIVE")
                   .load("LearningSpark/databricks-datasets/learning-spark-v2/flights/summary-data/csv/*"))

csv_df.show(10)

# JSON 사용
json_df = (spark.read.format("json")
                     .load("LearningSpark/databricks-datasets/learning-spark-v2/flights/summary-data/json/*"))

json_df.show(10)


# DataFrameWriter
# JSON 사용
location = ...
parquet_df1.write.format("json").mode("overwrite").save(location)