from pyspark.sql import SparkSession
from pyspark.sql.functions import col

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder\
                        .master("local")\
                        .appName("cache_persistence_sql_ex")\
                        .getOrCreate()
    
    df = ss.range(1000000)\
        .toDF("id")\
        .withColumn("square", col("id") * col("id"))
    
    df.createOrReplaceTempView("dfTable")
    ss.sql("CACHE TABLE dfTable")
    ss.sql("SELECT count(*) FROM dfTable").show()

    while True:
        pass





URLS = {
    "ssg_all": "http://ep2.ssgadm.com/channel/ssg/ssg_facebookAgenEpAll.txt",#덮어씌워질대상
    "ssg_brief": "http://ep2.ssgadm.com/channel/ssg/ssg_facebookAgenEpBrief.txt",
    "e_all": "http://ep2.ssgadm.com/channel/emart/e_facebookAgenEpAll.txt",
    "e_brief": "http://ep2.ssgadm.com/channel/emart/e_facebookAgenEpBrief.txt",
}