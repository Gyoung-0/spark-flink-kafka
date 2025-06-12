from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local[2]")\
        .appName("Event time windows ex")\
        .getOrCreate()
    
    domain_traffic_schema = StructType([
        StructField("id", StringType(), False),
        StructField("domain", StringType(), False),
        StructField("count", IntegerType(), False),
        StructField("time", TimestampType(), False),
    ])

    def read_traffics_from_socket():
        return ss.readStream \
            .format("socket")\
            .option("host", "localhost")\
            .option("port", 12345).load()\
            .select(F.from_json(F.col("value"),
                                domain_traffic_schema)
                        .alias("traffic")).selectExpr("traffic.*")
    

    def aggregate_traffic_counts_by_sliding_window():
        traffics_df = read_traffics_from_socket()

        windows_by_hours = traffics_df.groupby(F.window(
            F.col("time"),
            windowDuration="2 hours", 
                slideDuration="1 hour").alias("time"))\
                .agg(F.sum("count").alias("total_count")) \
                .select(
                    F.col("time").getField("start").alias("start"),
                    F.col("time").getField("end").alias("end"),
                    F.col("total_count")
                ).orderBy(F.col("start"))
                
        windows_by_hours.writeStream.format("console")\
            .outputMode("complete").start().awaitTermination()
        


    def aggreagate_traffic_counts_by_tumbling_window():
        traffics_df = read_traffics_from_socket()

        windows_by_hours = traffics_df.groupby(F.window(
            F.col("time"), "1 hour").alias("time"))\
                .agg(F.sum("count").alias("total_count")) \
                .select(
                    F.col("time").getField("start").alias("start"),
                    F.col("time").getField("end").alias("end"),
                    F.col("total_count")
                ).orderBy(F.col("start"))
                
        windows_by_hours.writeStream.format("console")\
            .outputMode("complete").start().awaitTermination()
        

    """
    매 시간마닫, traffic이 가장 많은 도메인을 출력
    """

    def read_traffics_from_file():
        return ss.readStream.schema(domain_traffic_schema)\
            .json("/Users/gimgyeong-yeong/fastcampus/spark-practices/part02/ch03_streaming/data/traffics")
    
    def find_largest_traffic_domain_per_hour():
        traffics_df = read_traffics_from_file()

        largest_traffic_domain = traffics_df.groupby(F.col("domain"),F.window(
            F.col("time"), "1 hour").alias("hour"))\
                .agg(F.sum("count").alias("total_count")) \
                .select(
                    F.col("hour").getField("start").alias("start"),
                    F.col("hour").getField("end").alias("end"),
                    F.col("domain"),
                    F.col("total_count")
                ).orderBy(F.col("start"))
                
        largest_traffic_domain.writeStream.format("console")\
            .outputMode("complete").start().awaitTermination()
    # aggregate_traffic_counts_by_sliding_window()
    # aggreagate_traffic_counts_by_tumbling_window()
    find_largest_traffic_domain_per_hour()