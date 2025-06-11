from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    ss: SparkSession = SparkSession.builder \
        .master("local[2]") \
        .appName("streaming dataframe join examples") \
        .getOrCreate()

    authors = ss.read \
        .option("inferSchema", True).json("/Users/gimgyeong-yeong/fastcampus/spark-practices/part02/ch03_streaming/data/authors.json")

    books = ss.read \
        .option("inferSchema", True).json("/Users/gimgyeong-yeong/fastcampus/spark-practices/part02/ch03_streaming/data/books.json")

    # 1. join (static, static)
    authors_books_df = authors.join(books,
                                    authors["book_id"] == books["id"],
                                    "inner")

    # authors_books_df.show()

    """
    2. join (static, stream)
    
    제한 사항)
    (left : static, right : stream) join -> left outer join, full outer join 불가능
    (left : stream, right : static) join -> right outer join, full outer join 불가능.
    
    ≈outputMode = append만 지원
    
    """


    def join_stream_with_static():
        streamed_books = \
            ss.readStream.format("socket") \
                .option("host", "localhost") \
                .option("port", 12345) \
                .load() \
                .select(F.from_json(F.col("value"),
                                    books.schema).alias("book")) \
                .selectExpr("book.id as id",
                            "book.name as name",
                            "book.year as year")
        # join : PER BATCH
        authors_books_df = authors.join(streamed_books,
                                        authors["book_id"] == streamed_books["id"],
                                        "full_outer")

        authors_books_df.writeStream \
            .format("console") \
            .outputMode("append") \
            .start().awaitTermination()
        
    def join_stream_with_stream():
        streamed_books = \
            ss.readStream.format("socket")\
                .option("host", "localhost") \
                .option("port", 12345) \
                .load() \
                .select(F.from_json(F.col("value"),
                                    books.schema).alias("book"))\
                .selectExpr("book.id as id",
                            "book.name as name",
                            "book.year as year")
        
        streamed_authors = \
            ss.readStream.format("socket")\
                .option("host", "localhost")\
                .option("port", 12346)\
                .load() \
                .select(F.from_json(F.col("value"),
                                    authors.schema).alias("author"))\
                .selectExpr("author.id as id",
                            "author.name as name",
                            "author.book_id as book_id")
        authors_books_df = \
            streamed_authors.join(streamed_books,
                                    streamed_authors["book_id"] == streamed_books["id"],
                                    "inner")
        authors_books_df.writeStream \
            .format("console")\
            .outputMode("append")\
            .start().awaitTermination()
        

    join_stream_with_stream()