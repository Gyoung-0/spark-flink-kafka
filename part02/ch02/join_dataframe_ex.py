from pyspark.sql import SparkSession
from pyspark.sql.types import *

def load_user_visits(ss: SparkSession):
    schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("visits", IntegerType(), False),
    ])

    data = [
        (1, 10),
        (2, 27),
        (3, 2),
        (4, 5),
        (5, 88),
        (6, 1),
        (7, 5)
    ]

    return ss.createDataFrame(data, schema)

def load_user_names(ss: SparkSession):
    schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("name", StringType(), False),
    ])

    data = [
        (1,"Isa"),
        (2,"Martyn"),
        (3,"Tomkin"),
        (4,"Arvin"),
        (5,"Carl"),
        (6,"Brooks"),
        (7,"Arne"),
        (8,"Berkie")
    ]

    return ss.createDataFrame(data, schema)

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
                        .master("local") \
                        .appName("datafe")\
                        .getOrCreate()
    
    user_visits_df = load_user_visits(ss)
    user_names_df = load_user_names(ss)

    # 1) 컬럼 지정 X: catersian join ( row: M X N 생성)
    user_names_df.join(user_visits_df)

    # 2) inner join
    user_names_df.join(user_visits_df, on="user_id").show()

    # 3) left outer join
    user_names_df.join(user_visits_df, on="user_id", how="left").show()

    # 4) right outer join
    user_names_df.join(user_visits_df, on="user_id", how="right").show()

    # 5) full outer join
    user_names_df.join(user_visits_df, on="user_id", how="outer").show()