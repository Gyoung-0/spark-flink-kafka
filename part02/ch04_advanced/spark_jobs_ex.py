from pyspark.sql import SparkSession
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
if __name__ == "__main__":

    ss: SparkSession = SparkSession.builder\
                        .master("local[2]")\
                        .appName("spark_jobs_ex")\
                        .getOrCreate()
    file_path = os.path.join(BASE_DIR, "data", "titanic_data.csv")
    df = ss.read\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .csv(file_path)
    
    df = df.repartition(5).where("Sex ='male'")\
        .select("Survived", "Pclass", "Fare")\
        .groupby("Survived", "Pclass")\
        .mean()
    
    print(f"result ==> {df.collect()}")

    df.explain(mode="extended")

    while True:
        pass