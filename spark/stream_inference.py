import os
import joblib
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Load the pre-trained model
model_path = "spark/model.joblib"
if not os.path.exists(model_path):
    raise FileNotFoundError(f"Model not found at {model_path}. Run train_model.py first.")

model = joblib.load(model_path)

# Spark Setup
spark = SparkSession.builder \
    .appName("StreamingInference") \
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
    .select("data.*")

# Inference UDF
def predict(amount, *v_features):
    features = [amount] + list(v_features)
    # Features must match X order in training: Amount, V1-V28
    return int(model.predict([features])[0])

# Register UDF
predict_udf = udf(predict, IntegerType())

# Apply Model
# Features used in training: Amount, V1-V28
feature_cols = [col("Amount")] + [col(f"V{i}") for i in range(1, 29)]
prediction_df = json_df.withColumn("prediction", predict_udf(*feature_cols))

# Output to Console
query = prediction_df.select("Time", "Amount", "Class", "prediction") \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()