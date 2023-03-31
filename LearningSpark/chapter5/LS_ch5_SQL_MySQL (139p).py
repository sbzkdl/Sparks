from pyspark.sql import SparkSession

# SparkSession 생성
spark = (SparkSession
            .builder
            .appName("SparkSQLPostgre")
            .getOrCreate())

# 로드 함수를 사용하여 JDBC 소스로부터 데이터를 로드
jdbcDF = (spark.read.format("jdbc")
                    .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
                    .option("driver", "com.mysql.jdbcDriver")
                    .option("dbtable", "[TABLENAME]")
                    .option("user", "[USERNAME]")
                    .option("password", "[PASSWORD]")
                    .load())

# 저장 함수를 사용하여 JDBC 소스에 데이터를 저장
(jdbcDF.write.format("jdbc")
             .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
             .option("driver", "com.mysql.jdbcDriver")
             .option("dbtable", "[TABLENAME]")
             .option("user", "[USERNAME]")
             .option("password", "[PASSWORD]")
             .save())