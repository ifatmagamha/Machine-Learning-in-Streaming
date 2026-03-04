import pandas as pd
import numpy as np
from sklearn.linear_model import LogisticRegression
import joblib
import os

def main():
    # Generate dummy data for training
    np.random.seed(42)
    n_samples = 1000
    
    # Matching the synthetic data features from producer
    data = []
    for _ in range(n_samples):
        row = [np.random.uniform(0, 100000), np.random.uniform(0, 2000)] # Time, Amount
        row.extend(np.random.normal(0, 1, 28)) # V1-V28
        
        # Fraud logic: high amount or some V features
        is_fraud = 1 if (row[1] > 1500 and np.random.random() > 0.7) else 0
        row.append(is_fraud)
        data.append(row)
        
    columns = ["Time", "Amount"] + [f"V{i}" for i in range(1, 29)] + ["Class"]
    df = pd.DataFrame(data, columns=columns)
    
    X = df.drop(columns=["Class", "Time"]) # Exclude Time from inference features
    y = df["Class"]
    
    print("Training a simple Logistic Regression model...")
    model = LogisticRegression()
    model.fit(X, y)
    
    model_path = os.path.join("spark", "model.joblib")
    joblib.dump(model, model_path)
    print(f"Model saved to {model_path}")

if __name__ == "__main__":
    main()
