import json
import time
import random
from confluent_kafka import Producer

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def generate_transaction():
    """Generates a synthetic transaction mimicking the Credit Card Fraud dataset."""
    transaction = {
        "Time": time.time(),
        "Amount": round(random.uniform(1.0, 2000.0), 2),
    }
    # V1 to V28 PCA components
    for i in range(1, 29):
        transaction[f"V{i}"] = round(random.uniform(-2.0, 2.0), 4)
    
    # Simple rule for synthetic fraud
    if transaction["Amount"] > 1500 and random.random() > 0.8:
        transaction["Class"] = 1
    else:
        transaction["Class"] = 0
        
    return transaction

def main():
    conf = {
        'bootstrap.servers': 'localhost:9092',
        'client.id': 'python-producer'
    }

    producer = Producer(conf)
    topic = 'transactions'

    print(f"Starting producer... Sending data to topic: {topic}")

    try:
        while True:
            tx = generate_transaction()
            producer.produce(
                topic, 
                key=str(tx["Time"]), 
                value=json.dumps(tx), 
                callback=delivery_report
            )
            producer.poll(0)
            time.sleep(1) # Send one transaction per second
    except KeyboardInterrupt:
        print("Stopping producer...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()