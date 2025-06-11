from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import to_timestamp, col, max, min, mean, hour, minute, date_trunc, collect_set, count

import os

def load_data(ss: SparkSession, from_file, schema):
    if from_file:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(base_dir, "data/log.csv")
        return ss.read.schema(schema).csv(file_path)
    return "없음"

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
                        .master("local")\
                        .appName("log dataframe ex")\
                        .getOrCreate()
    
    
    fields = StructType([
        StructField("ip", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("method", StringType(), False),
        StructField("endpoint", StringType(), False),
        StructField("status_code", StringType(), False),
        StructField("latency", StringType(), False),
    ])

    table_name = "log_data"

    load_data(ss, True, fields)\
    .createOrReplaceTempView(table_name)
    
    # ss.sql(f"SELECT * FROM {table_name}").show()
    # ss.sql(f"SELECT * FROM {table_name}").printSchema()

    ss.sql(f"""
            SELECT *, latency / 1000 AS latency_seconds
           FROM {table_name}
           """).show()
    
# a-2) StringType으로 받은 timestamp 컬럼을, TimestampType으로 변경

ss.sql(f"""
        SELECT ip, TIMESTAMP(timestamp) AS timestamp, method, endpoint, status_code, latency
        FROM {table_name}
       """).printSchema()

# b) filter
# b-1) status_code = 400, endpoint = "/users"인 row만 필터링

ss.sql(f"""
        SELECT * FROM {table_name}
        WHERE status_code = '400' AND endpoint = '/users'
       """).show()


# c) group by
# c-1 method, endpint 별 latency의 최댓값, 최솟값, 평균값 확인
ss.sql(f"""
     SELECT
       method,
       endpoint,
       MAX(latency) AS max_latency,
       MIN(latency) AS min_latency,
       AVG(latency) AS mean_latency
    FROM {table_name}
    GROUP BY method, endpoint
    """).show()

# c-2) 분 단위의, 중복을 제거한 ip 리스트, 개수 뽑기
ss.sql(f"""
        SELECT
            hour(date_trunc('HOUR', timestamp)) AS hour,
            minute(date_trunc('MINUTE', timestamp)) AS minute,
            collect_set(ip) AS ip_list,
            count(ip) AS ip_count
        FROM {table_name}
        GROUP BY hour, minute
        ORDER BY hour, minute
       """).explain()