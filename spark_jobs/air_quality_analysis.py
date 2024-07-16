from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, struct
from pyspark.sql.types import StructType, StructField, StringType, FloatType

def main():
    spark = SparkSession.builder \
        .appName("Air Quality Analysis") \
        .getOrCreate()

    schema = StructType([
        StructField("location", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("pm10", FloatType(), True),
        StructField("pm2_5", FloatType(), True)
    ])

    kafka_df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "air_quality") \
        .load()

    air_quality_df = kafka_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), schema).alias("data")) \
        .select("data.*")

    query = air_quality_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
