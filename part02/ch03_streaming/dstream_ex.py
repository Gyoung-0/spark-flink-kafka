import os

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext, DStream

if __name__ == '__main__':
    base_dir = os.path.dirname(os.path.abspath(__file__))

    sc: SparkContext = SparkSession.builder \
            .master("local[2]") \
            .appName("DStream Ex") \
            .getOrCreate().sparkContext
    ssc = StreamingContext(sc, 5)

    def read_from_socket():
        socket_stream: DStream[str]= \
            ssc.socketTextStream("localhost", 12345)
        words_stream: DStream[str] = \
            socket_stream.flatMap(lambda line: line.split(" "))
        
        words_stream.pprint()

        ssc.start()
        ssc.awaitTermination()

    def read_from_file():
        stocks_file_path = os.path.join(base_dir, "data", "stocks")
        text_stream: DStream[str] = \
            ssc.textFileStream(stocks_file_path)
        text_stream.pprint()

        ssc.start()
        ssc.awaitTermination()

    read_from_file()
