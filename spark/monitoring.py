from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, window, current_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

# Spark Setup
spark = SparkSession.builder \
    .appName("DriftMonitoring") \
    .getOrCreate()

# Schema matching the producer output
schema = StructType([
    StructField("Time", DoubleType()),
    StructField("Amount", DoubleType()),
    StructField("Class", IntegerType()),
] + [StructField(f"V{i}", DoubleType()) for i in range(1, 29)])

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "transactions") \
    .load()

# Deserialize JSON
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", current_timestamp())

# Track Fraud Rate over time
monitoring_df = json_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window(col("timestamp"), "10 minutes", "1 minute")) \
    .agg(
        avg("Class").alias("avg_fraud_rate"),
        avg("Amount").alias("avg_amount")
    )

# Output Monitoring Metrics to Console
query = monitoring_df.select("window.start", "window.end", "avg_fraud_rate", "avg_amount") \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()