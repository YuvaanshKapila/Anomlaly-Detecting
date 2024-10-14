from sklearn.ensemble import IsolationForest
import numpy as np
from kafka import KafkaConsumer
# Create synthetic training data (normal and anomalous data)
def generate_data():
    normal_data = np.random.normal(0, 1, (100, 2))  # Generate normal data
    anomalies = np.random.uniform(low=-6, high=6, size=(10, 2))  # Generate anomalies
    return np.vstack([normal_data, anomalies])

# Train the Isolation Forest model
def train_model():
    data = generate_data()  # Use your actual data stream here
    model = IsolationForest(contamination=0.1)  # Set contamination level for anomalies
    model.fit(data)
    return model

# Use the trained model to predict new data
def detect_anomaly(model, new_data):
    prediction = model.predict(new_data)
    if prediction == -1:
        return "Anomaly detected"
    else:
        return "Data is normal"


# Connect to Redpanda Kafka topic
def connect_to_redpanda():
    consumer = KafkaConsumer(
        'your-topic-name',  # Replace with your Redpanda topic
        bootstrap_servers='localhost:9092',  # Adjust if you're using a different server
        auto_offset_reset='earliest',
        enable_auto_commit=True
    )
    return consumer

# Process incoming data and run the anomaly detection
def process_stream(consumer, model):
    for message in consumer:
        # Simulate converting the message to a numpy array (e.g., [x, y] values)
        data = np.frombuffer(message.value, dtype=float).reshape(1, -1)

        # Detect anomaly using the trained model
        result = detect_anomaly(model, data)
        print(f"Data: {data} -> {result}")

# Main function to run the AI
def main():
    model = train_model()  # Train the AI model
    consumer = connect_to_redpanda()  # Connect to the Redpanda stream
    process_stream(consumer, model)  # Process the stream and detect anomalies

if __name__ == "__main__":
    main()