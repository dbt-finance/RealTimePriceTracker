from kafka import KafkaConsumer
from json import loads
from time import sleep

def calculate_change(old_price, new_price):
    return ((new_price - old_price) / old_price) * 100

consumer = KafkaConsumer(
    'amazon.com',  # Change to your Kafka topic name
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group-id',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

last_price = None  # Initialize the last price variable

try:
    for event in consumer:
        event_data = event.value
        print(event_data)

        if last_price is not None:  
            old_price = float(last_price)
            new_price = float(event_data['current_price'])
            change = calculate_change(old_price, new_price)
            print(f"Price Change for {event_data['stock_name']} ({event_data['symbol']}): {change}%")
        else:
            print("Initial price. Waiting for the next update.")

        last_price = event_data['current_price']

        sleep(2)  # Sleep for 2 seconds between messages
except KeyboardInterrupt:
    print("Kafka Consumer closed.")