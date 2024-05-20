from kafka import KafkaProducer
import csv
import json

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Define Kafka topic
topic = 'user-events'

# Read CSV file and send data to Kafka
with open('input/online_retail_processed.csv', newline='') as csvfile:
    csvreader = csv.DictReader(csvfile)
    for row in csvreader:
        producer.send(topic, row)


producer.flush()
producer.close()
