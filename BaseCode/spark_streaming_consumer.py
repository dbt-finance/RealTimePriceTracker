from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType
from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, when



if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("stock Streaming Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.jars", "mysql-connector-j-8.1.0.jar") \
        .getOrCreate()

    schema = StructType([
        StructField("timestamp", StringType()),
        StructField("symbol", StringType()),
        StructField("stock_name", StringType()),
        StructField("current_price", StringType())
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "amazon.com") \
        .option("startingOffsets", "earliest") \
        .load()
    

    kafka_df.printSchema()
    """
    |-- key: binary (nullable = true)
    |-- value: binary (nullable = true)
    |-- topic: string (nullable = true)
    |-- partition: integer (nullable = true)
    |-- offset: long (nullable = true)
    |-- timestamp: timestamp (nullable = true)
    |-- timestampType: integer (nullable = true)
    """

    print("kafka topic 읽기 완료")

    value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))
    value_df.createOrReplaceTempView("amazon")
    value_df.printSchema()



    stock_df = spark.sql("SELECT value.symbol, value.current_price FROM amazon")

    print(stock_df)
    # 로컬 mysql에 적재
    jdbc_url = "jdbc:mysql://localhost:3306/finance"
    mysql_username = "root"
    mysql_password = "tndud123"
    mysql_table = "finance.overseas"

    query = stock_df.writeStream \
        .foreachBatch(lambda batch_df, batch_id: batch_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", mysql_table) \
            .option("user", mysql_username) \
            .option("password", mysql_password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("append") \
            .save()
        ) \
        .start()

    query.awaitTermination()

    