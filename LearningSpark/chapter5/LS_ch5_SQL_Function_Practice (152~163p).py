from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# SparkSession 생성
spark = (SparkSession
            .builder
            .appName("SparkSQLPractice")
            .getOrCreate())

# 파일 경로 설정
tripdelaysFilePath = "LearningSpark/databricks-datasets/learning-spark-v2/flights/departuredelays.csv"
airportsnaFilePath = "LearningSpark/databricks-datasets/learning-spark-v2/flights/airport-codes-na.txt"

# 공항 데이터세트를 읽어 오기
airportsna = (spark.read.format("csv")
                        .options(header="true", inferSchema="true", sep="\t")
                        .load(airportsnaFilePath))

airportsna.createOrReplaceTempView("airports_na")

# 출발 지연 데이터세트를 읽어 오기
departureDelays = (spark.read.format("csv")
                             .options(header="true")
                             .load(tripdelaysFilePath))

# expr를 사용하여 delay 및 distance 칼럼을 STRING에서 INT로 변환
departureDelays = (departureDelays
                   .withColumn("delay", expr("CAST(delay as INT) as delay"))
                   .withColumn("distance", expr("CAST(distance as INT) as distance")))

departureDelays.createOrReplaceTempView("departureDelays")

# 작은 임시 테이블 생성
# 작은 시간 범위 동안 시애틀(SEA)에서 출발하여 샌프란시스코(SFO)에 도착하는 3개의 항공편에 대한 정보만 포함
foo = (departureDelays
        .filter(expr("""origin == 'SEA' AND destination == 'SFO' and
                        date like '01010%' and
                        delay > 0""")
                )
        )

foo.createOrReplaceTempView("foo")

# 본래 테이블들과 foo 테이블 비교해서 확인
spark.sql("""
        SELECT
                *
        FROM
                airports_na
        LIMIT
                10
        """).show()

spark.sql("""
        SELECT
                *
        FROM
                departureDelays
        LIMIT
                10
        """).show()

spark.sql("""
        SELECT
                *
        FROM
                foo
        """).show()


# Union
# 두 테이블 결합
bar = departureDelays.union(foo)
bar.createOrReplaceTempView("bar")

# 결합된 결과 보기 (특정 시간 범위에 대한 SEA와 SFO를 필터)
bar.filter(expr("""
                origin == 'SEA' AND
                destination == 'SFO' AND
                date LIKE '01010%' AND
                delay > 0
                """)
        ).show()

# In SQL
spark.sql("""
        SELECT
                *
        FROM
                bar
        WHERE
                origin == 'SEA' AND
                destination == 'SFO' AND
                date LIKE '01010%' AND
                delay > 0
        """).show()


# Join
# 출발 지연 데이터(foo)와 공항 정보의 조인
(foo.join(airportsna, airportsna.IATA == foo.origin)
    .select("City", "State", "date", "delay", "distance", "destination")).show()

# In SQL
spark.sql("""
        SELECT
                a.City, a.State, f.date, f.delay, f.distance, f.destination
        FROM
                foo f
        JOIN
                airports_na a
          ON 
                a.IATA = f.origin
        """).show()


# 윈도우 함수 : dense_rank()
# 시애틀, 샌프란시스코, 뉴욕에서 출발하여 특정 목적지로 이동하는 항공편에서 기록된 TotalDelays에 대한 검토
spark.sql("DROP TABLE IF EXISTS departureDelaysWindow;")

spark.sql("""
        CREATE TABLE
                        departureDelaysWindow AS
        SELECT
                        origin, destination, SUM(delay) AS TotalDelays
        FROM
                        departureDelays
        WHERE
                        origin IN ('SEA', 'SFO', 'JFK') AND
                        destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
        GROUP BY
                        origin, destination;
        """).show()

spark.sql("""
        SELECT
                *
        FROM
                departureDelaysWindow
        """).show()

# 각 출발 공항에 대해 가장 많은 지연이 발생한 3개의 목적지
# 기본 SQL
spark.sql("""
        SELECT
                origin, destination, SUM(TotalDelays) AS TotalDelays
        FROM
                departureDelaysWindow
        WHERE
                origin = '[ORIGIN]'
        GROUP BY
                origin, destination
        ORDER BY
                SUM(TotalDelays) DESC
        LIMIT
                3
        """).show()

# dense_rank() 사용
spark.sql("""
        SELECT
                origin, destination, TotalDelays, rank
        FROM
                (
                SELECT
                        origin, destination, TotalDelays, 
                        dense_rank() OVER (
                                          PARTITION BY
                                                        origin
                                          ORDER BY
                                                        TotalDelays DESC      
                                          ) AS rank
                FROM
                        departureDelaysWindow
                ) t
        WHERE
                rank <= 3
        """).show()


# 수정
# 열 추가 : withColumn()
foo2 = (foo.withColumn("status", expr("""
                                        CASE WHEN 
                                                delay <= 10 
                                        THEN 
                                                'On-time'
                                        ELSE
                                                'Delayed'
                                        END
                                        """)))

foo2.show()

# 열 삭제 : drop()
foo3 = foo2.drop("delay")
foo3.show()

# 칼럼명 바꾸기 : withColumnRenamed()
foo4 = foo3.withColumnRenamed("status", "flight_status")
foo4.show()

# 피벗
# 본 테이블
spark.sql("""
        SELECT
                destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
        FROM
                departureDelays
        WHERE
                origin = 'SEA'
        """).show(10)
# 피벗 적용
# month 칼럼에 이름 배치 가능 (1과 2 대신 각각 Jan과 Feb를 표시)
# 목적지 및 월별 지연에 대한 집계 계산 (평균 및 최대) 수행 가능
spark.sql("""
        SELECT
                *
        FROM
                (
                SELECT
                        destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
                FROM
                        departureDelays
                WHERE
                        origin = 'SEA'
                )
        PIVOT
                (
                CAST
                        (AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay,
                         MAX(delay) AS MaxDelay
                FOR
                        month IN (1 JAN, 2 FEB)                
                )
        ORDER BY
                destination
        """).show()
