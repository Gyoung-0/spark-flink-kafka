from pyspark.sql import SparkSession
import pyspark.sql.functions as f

if __name__ == '__main__':
    ss: SparkSession = SparkSession\
                    .builder\
                    .master("local")\
                    .appName("wordCount RDD ver")\
                    .getOrCreate()
    df = ss.read.text("/Users/gimgyeong-yeong/fastcampus/spark-practices/part02/ch01_intro/data/words.txt")

    df = df.withColumn('word', f.explode(f.split(f.col('value'), " ")))\
        .withColumn("count", f.lit(1)).groupBy("word").sum()
    
    df.show()