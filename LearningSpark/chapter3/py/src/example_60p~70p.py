from pyspark.sql.types import *

# sparksession 만들기

fire_schema = StructType([StructField("CallNumber", IntegerType(), True),
                          StructField("UnitID", StringType(), True),
                          StructField("IncidentNumber", IntegerType(), True),
                          StructField("CallType", StringType(), True),
                          StructField("CallDate", StringType(), True),
                          StructField("WatchDate", StringType(), True),
                          StructField("CallFinalDisposition", StringType(), True),
                          StructField("AvailableDtTm", StringType(), True),
                          StructField("Address", StringType(), True),
                          StructField("City", StringType(), True),
                          StructField("Zipcode", IntegerType(), True),
                          StructField("Battalion", StringType(), True),
                          StructField("StationArea", StringType(), True),
                          StructField("Box", StringType(), True),
                          StructField("OriginalPriority", StringType(), True),
                          StructField("Priority", StringType(), True),
                          StructField("FinalPriority", IntegerType(), True),
                          StructField("ALSUnit", BooleanType(), True),
                          StructField("CallTypeGroup", StringType(), True),
                          StructField("NumAlarms", IntegerType(), True),
                          StructField("UnitType", StringType(), True),
                          StructField("UnitSequenceInCallDispatch", IntegerType(), True),
                          StructField("FirePreventionDistrict", StringType(), True),
                          StructField("SupervisorDistrict", StringType(), True),
                          StructField("Neighborhood", StringType(), True),
                          StructField("Location", StringType(), True),
                          StructField("RowID", StringType(), True),
                          StructField("Delay", FloatType(), True)])


# DataFrameReader 인터페이스로 CSV 파일 읽기
sf_fire_file = "LearningSpark/chapter3/data/sf-fire-calls.csv"
fire_df = spark.read.csv(sf_fire_file, header=True, schema=fire_schema) # spark.read.csv가 pyspark 안에서만 실행된다. 왜지...?


# 데이터 프레임을 파케이 파일이나 SQL 테이블로 저장하기
# 파케이로 저장
parquet_path = 'fire_df_par'
fire_df.write.format("parquet").save(parquet_path)
# 테이블로 저장
parquet_table = 'fire_df_table'
fire_df.write.format("parquet").saveAsTable(parquet_table)


# 프로젝션과 필터
few_fire_df = (fire_df
               .select("IncidentNumber", "AvailableDtTm", "CallType")
               .where(col("CallType") != "Medical Incident"))
few_fire_df.show(5, truncate=False)
# CallType 종류가 몇 가지인지 알아보기. -> countDistinct()를 써서 신고 타입의 개수를 되돌려 준다.
from pyspark.sql.functions import *
(fire_df
    .select("CallType")
    .where(col("CallType").isNotNull())
    .agg(countDistinct("CallType").alias("DistinctCallTypes"))
    .show())
# null이 아닌 신고 타입의 목록 알아보기. -> 모든 행에서 null이 아닌 개별 CallType를 추출한다.
(fire_df
    .select("CallType")
    .where(col("CallType").isNotNull())
    .distinct()
    .show(10, False))


# 칼럼의 이름 변경 및 추가 삭제
new_fire_df = fire_df.withColumnRenamed("Delay", "ResponseDelayedinMins")
(new_fire_df
    .select("ResponseDelayedinMins")
    .where(col("ResponseDelayedinMins") > 5)
    .show(5, False))


# 칼럼의 내용이나 타입 바꾸기
fire_ts_df = (new_fire_df
                .withColumn("IncidentDate", to_timestamp(col("CallDate"), "MM/dd/yyyy"))
                .drop("CallDate")
                .withColumn("OnWatchDate", to_timestamp(col("WatchDate"), "MM/dd/yyyy"))
                .drop("WatchDate")
                .withColumn("AvailableDtTS", to_timestamp(col("AvailableDtTm"), "MM/dd/yyyy hh:mm:ss a"))
                .drop("AvailableDtTm"))
# 변환된 칼럼 가져오기
(fire_ts_df
    .select("IncidentDate", "OnWatchDate", "AvailableDtTS")
    .show(5, False))


# dayofmonth(), dayofyear(), dayofweek() 함수들 사용하기
# 지난 7일 동안 기록된 통화 수 확인
(fire_ts_df
    .select(year("IncidentDate"))
    .distinct()
    .orderBy(year("IncidentDate"))
    .show())


# 집계연산 -> groupBy(), orderBy(), count() 등
# 가장 흔한 형태의 신고
(fire_ts_df
    .select("CallType")
    .where(col("CallType").isNotNull())
    .groupBy("CallType")
    .count()
    .orderBy("count", ascending=False)
    .show(n=10, truncate=False))


# 그 외 일반적인 데이터 프레임 연산들 -> min(), max(), sum(), avg() 등
# 경보 횟수의 합, 응답시간 평균, 모든 신고에 대한 최소/최장 응답시간 계산
import pyspark.sql.functions as F
(fire_ts_df
    .select(F.sum("NumAlarms"), F.avg("ResponseDelayedinMins"),
            F.min("ResponseDelayedinMins"), F.max("ResponseDelayedinMins"))
    .show())