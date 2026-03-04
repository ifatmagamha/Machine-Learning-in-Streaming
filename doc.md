# Machine Learning on Streams - Lab Documentation

This document summarizes the implementation and execution of the real-time Machine Learning pipeline using Kafka and Spark Structured Streaming.

## 1. Infrastructure Setup

The pipeline is containerized using Docker. The `docker-compose.yml` file defines:
- **Zookeeper**: Manages Kafka cluster state.
- **Kafka**: The message broker for streaming transactions.
- **Spark Master/Worker**: The processing engine for streaming ML.

### How to Start
```bash
docker-compose up -d
```

## 2. Pipeline Components

### Data Generation (`kafka/producer.py`)
A Python producer simulates a stream of credit card transactions. 
- **Features**: Time, Amount, and V1-V28 (PCA components).
- **Target**: `Class` (0 for normal, 1 for synthetic fraud).
- **Rate**: 1 transaction/second.

### Phase 1: Streaming Inference (`spark/stream_inference.py`)
- **Model**: A Logistic Regression model trained on synthetic data (`spark/train_model.py`).
- **Logic**: Loads the `model.joblib` and applies it to each Kafka event using a Spark UDF.
- **Output**: Real-time predictions printed to the console.

### Phase 2: Anomaly Detection (`spark/anomaly_detection.py`)
- **Logic**: Flags transactions where `Amount > 1800` as potential anomalies.
- **Windowing**: Uses Spark Structured Streaming windowing for potential rolling statistics.

### Phase 3: Drift Monitoring (`spark/monitoring.py`)
- **Logic**: Aggregates the `Class` (fraud rate) and `Amount` over 10-minute sliding windows.
- **Purpose**: Detects changes in data distribution or model performance over time.

## 4. Verification & Testing

Follow these steps to ensure the entire pipeline is working correctly:

### 1. Infrastructure (Docker)
Check that all containers are healthy:
```bash
docker compose ps
```
**Expected**: `zookeeper`, `kafka`, `spark-master`, and `spark-worker` should all be `Up` or `Running`.

### 2. Kafka Topic Existence
List topics to confirm the broker is ready:
```bash
docker compose exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

### 3. Data Ingestion (Producer)
Start the producer in your terminal:
```bash
python kafka/producer.py
```
**Expected**: "Message delivered to transactions [0]" printed every second.

### 4. Streaming Inference (Spark)
Run the inference job directly inside the Docker container (with a writable Ivy cache):
```bash
docker compose exec -e PYSPARK_PYTHON=python3 spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 --conf "spark.jars.ivy=/tmp/ivy2" --master spark://spark-master:7077 spark/stream_inference.py
```
**Expected**: A console output table showing `Time`, `Amount`, `Class`, and the binary `prediction`.

## 5. Troubleshooting journaling
- **Spark Job Fails (Java/Python)**: Ensure your `JAVA_HOME` is set to Java 8 or 11 and your `venv` is activated.
- **Model Not Found / NumPy Version Error**: If you see `ModuleNotFoundError: No module named 'numpy._core'`, it means your local NumPy version is too new for the container. Retrain the model **inside** docker:
  ```bash
  docker compose exec spark-master python3 spark/train_model.py
  ```
