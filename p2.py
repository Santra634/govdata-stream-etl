import requests
from kafka import KafkaProducer
import json

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize JSON
)

# API setup
url = "https://api.data.gov.in/resource/8b75d7c2-814b-4eb2-9698-c96d69e5f128"
params = {
    "api-key": "579b464db66ec23bdd000001a476110a3d03412e5e142883b351c97c",
    "format": "json",
    "limit": 10
}

# Fetch data from API
response = requests.get(url, params=params)
if response.status_code == 200:
    records = response.json().get("records", [])
    # print(records)
    print(f"Sending {len(records)} records to Kafka...")

    for record in records:
        producer.send('gov-data', value=record)
    producer.flush()
    print("All records sent.")
else:
    print("Failed to fetch data. Status:", response.status_code)
