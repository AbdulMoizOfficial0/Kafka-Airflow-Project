import json
import csv
import os

from kafka import KafkaConsumer


def write_to_csv(data, ticker, path):
    os.makedirs(path, exist_ok=True)

    with open(os.path.join(path, f'{ticker}.csv'), 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)

        # Convert data to a dictionary (assuming it's a JSON string)
        data_dict = json.loads(data)

        # Check if the file is empty, write header if needed
        if csvfile.tell() == 0:
            writer.writerow(data_dict.keys())

        # Write data as a row to the CSV file
        writer.writerow(data_dict.values())


# Modify the 'path' variable to point to the existing outer directory
path = "../data/"

consumer = KafkaConsumer('-testingkafka', bootstrap_servers='localhost:9092')

try:
    for message in consumer:
        data = message.value.decode('utf-8')
        # Extract the ticker from the data (modify based on your data structure)
        # This assumes the "ticker" key exists in the JSON data
        ticker = json.loads(data)["ticker"]
        write_to_csv(data, ticker, path)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
