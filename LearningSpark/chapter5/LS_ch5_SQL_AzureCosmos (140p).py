from pyspark.sql import SparkSession

# SparkSession 생성
spark = (SparkSession
            .builder
            .appName("SparkSQLAzureCosmos")
            .getOrCreate())

# 애저 코스모스 DB로부터 데이터 로드
# 설정 읽기
query = """
        SELECT
                c.colA, c.coln
        FROM
                c
        WHERE
                c.origin = 'SEA'
        """

readConfig = {
            "Endpoint" : "https://[ACCOUNT].documents.azure.com:443/",
            "Masterkey" : "[MASTER_KEY]",
            "Database" : "[DATABASE]",
            "preferredRegions" : "Central US;East US2",
            "Collection" : "[COLLECTION]",
            "SamplingRatio" : "1.0",
            "schema_samplesize" : "1000",
            "query_pagesize" : "2147483647",
            "query_custom" : query
            }

# azure-cosmosdb-spark를 통해 연결하여 스파크 데이터 프레임 생성
df = (spark.read.format("com.microsoft.azure.cosmosdb.spark")
                .options(**readConfig)
                .load())

# 비행 수 카운트
df.count()

# 애저 코스모스 DB에 데이터 저장
# 설정 쓰기
writeConfig = {
                "Endpoint" : "https://[ACCOUNT].documents.azure.com:443/",
                "Masterkey" : "[MASTER_KEY]",
                "Database" : "[DATABASE]",
                "Collection" : "[COLLECTION]",
                "Upsert" : "true"
                }

# 애저 코스모스 DB에 데이터 프레임 업서트 하기
(df.write.format("com.microsoft.azure.cosmosdb.spark")
         .options(**writeConfig)
         .save())