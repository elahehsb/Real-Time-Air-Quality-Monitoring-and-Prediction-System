from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder \
        .appName("Air Quality Prediction Model") \
        .getOrCreate()

    # Load and preprocess data
    df = spark.read.csv("hdfs://namenode:9000/data/air_quality.csv", header=True)
    assembler = VectorAssembler(inputCols=["pm10", "pm2_5"], outputCol="features")

    # Split data
    (training_data, test_data) = df.randomSplit([0.7, 0.3])

    # Define model
    rf = RandomForestRegressor(labelCol="pm10", featuresCol="features")

    # Build pipeline
    pipeline = Pipeline(stages=[assembler, rf])

    # Train model
    model = pipeline.fit(training_data)

    # Evaluate model
    predictions = model.transform(test_data)
    predictions.select("features", "pm10", "prediction").show()

    spark.stop()

if __name__ == "__main__":
    main()
