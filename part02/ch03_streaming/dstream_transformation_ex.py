import os
from collections import namedtuple

from pyspark import SparkContext, RDD
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext, DStream

base_dir = os.path.dirname(os.path.abspath(__file__))
columns = ["Ticker", "Date", "Open", "High",
           "Low", "Close", "AdjClose", "Volume"]
Finance = namedtuple("Finance",
                     columns)

if __name__ == "__main__":
    sc: SparkContext = SparkSession.builder \
        .master("local[2]") \
        .appName("DStream transformations ex") \
        .getOrCreate().sparkContext

    ssc = StreamingContext(sc, 5)


    def read_finance():
        # 1. map
        def parse(line: str):
            arr = line.split(",")
            return Finance(*arr)

        return ssc.socketTextStream("localhost", 12345) \
            .map(parse)


    finance_stream: DStream[Finance] = read_finance()


    # 2. filter

    def filter_nvda():
        finance_stream.filter(lambda f: f.Ticker == "NVDA").pprint()


    def filter_volume():
        finance_stream.filter(lambda f: int(f.Volume) > 213738500).pprint()

    
    # reduce by, group by
    def count_dates_ticker():
        finance_stream.map(lambda f: (f.Ticker, 1))\
            .reduceByKey(lambda a, b: a + b).pprint()
    
    def group_by_dates_volume():
        finance_stream.map(lambda f: (f.Date, int(f.Volume)))\
            .groupByKey().mapValues(sum).pprint()
        
    def save_to_json():
        def foreach_func(rdd: RDD):
            if rdd.isEmpty():
                print("RDD is empty")
                return      
            df = rdd.toDF(columns)
            dir_path = os.path.join(base_dir, "data", "stocks", "outputs")
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
            
            n_files = len(os.listdir(dir_path))
            full_path = f"{dir_path}/finance--{n_files}.json"
            df.write.json(full_path)
            print(f"num-partitions => {df.rdd.getNumPartitions()}")
            print("write completed")

        finance_stream.foreachRDD(foreach_func)

    save_to_json()
    ssc.start()
    ssc.awaitTermination()