from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, stddev, window, current_timestamp
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType, TimestampType

# Spark Setup
spark = SparkSession.builder \
    .appName("AnomalyDetection") \
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
    .withColumn("timestamp", current_timestamp()) # Add timestamp for windowing

# Compute Rolling Stats (e.g., 5-minute window)
rolling_stats = json_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(window(col("timestamp"), "5 minutes")) \
    .agg(
        avg("Amount").alias("avg_amount"),
        stddev("Amount").alias("std_amount")
    )

# Join Stats with Stream to detect anomalies
# we'll flag any transaction with Amount > 1800 as an anomaly (3 sigma if mean=1000, std=260ish)
anomalies_df = json_df.withColumn("is_anomaly", col("Amount") > 1800)

# Output Anomalies to Console
query = anomalies_df.filter(col("is_anomaly") == True) \
    .select("Time", "Amount", "is_anomaly") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()