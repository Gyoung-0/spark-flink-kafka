from pyspark.sql import SparkSession
import os

DIR_PATH = os.path.dirname(os.path.abspath(__file__))
if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
                        .master("local")\
                        .appName("spark_jobs_ex")\
                        .getOrCreate()
    file_path = os.path.join(DIR_PATH, "data","titanic_data.csv")    
    df = ss.read.option("header", "true")\
            .option("inferSchema", "true")\
            .csv(file_path)
    
    # 1. repartition ex)

    # df = df.repartition(10)
    # df = df.repartition(10, "Age")
    # df.cache()

    # df.count()


    df = df.repartition(20).where("Pclass = 1")\
        .select("PassengerId", "Survived")\
        .coalesce(5)\
        .where("Age >= 20")\
        

    df.count()

    while True:
        pass
