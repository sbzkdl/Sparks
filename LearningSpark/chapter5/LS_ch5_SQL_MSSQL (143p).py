from pyspark.sql import SparkSession

# SparkSession 생성
spark = (SparkSession
            .builder
            .appName("SparkSQLMSSQL")
            .getOrCreate())

# jdbcUrl 설정
jdbcUrl = "jdbc:sqlserver://[DBSERVER]:1433;database=[DATABASE]"

# JDBC 소스로부터 데이터를 로드
jdbcDF = (spark.read.format("jdbc")
                    .option("url", jdbcUrl)
                    .option("dbtable", "[TABLENAME]")
                    .option("user", "[USERNAME]")
                    .option("password", "[PASSWORD]")
                    .load())

# JDBC 소스에 데이터를 저장
(jdbcDF.write.format("jdbc")
             .option("url", jdbcUrl)
             .option("dbtable", "[TABLENAME]")
             .option("user", "[USERNAME]")
             .option("password", "[PASSWORD]")
             .save())