from pyspark.sql import SparkSession

# SparkSession 생성
spark = (SparkSession
            .builder
            .appName("SparkSQLPostgre")
            .getOrCreate())

# 읽기 방법 1 : 로드 함수를 사용하여 JDBC 소스로부터 데이터를 로드
jdbcDF1 = (spark.read.format("jdbc")
                     .option("url", "jdbc:postgresql://[DBSERVER]")
                     .option("dbtable", "[SCHEMA].[TABLENAME]")
                     .option("user", "[USERNAME]")
                     .option("password", "[PASSWORD]")
                     .load())

# 읽기 방법 2 : jdbc 함수를 사용하여 JDBC 소스로부터 데이터를 로드
jdbcDF2 = (spark.read
                .jdbc("jdbc:postgresql://[DBSERVER]",
                      "[SCHEMA].[TABLENAME]",
                      properties={"user": "[USERNAME]", "password": "[PASSWORD]"}))

# 쓰기 방법 1 : 저장 함수를 사용하여 JDBC 소스에 데이터를 저장
(jdbcDF1.write.format("jdbc")
              .option("url", "jdbc:postgresql://[DBSERVER]")
              .option("dbtable", "[SCHEMA].[TABLENAME]")
              .option("user", "[USERNAME]")
              .option("password", "[PASSWORD]")
              .save())

# 쓰기 방법 2 : jdbc 함수를 사용하여 JDBC 소스에 데이터를 저장
(jdbcDF2.write
        .jdbc("jdbc:postgresql://[DBSERVER]",
        "[SCHEMA].[TABLENAME]",
        properties={"user": "[USERNAME]", "password": "[PASSWORD]"}))