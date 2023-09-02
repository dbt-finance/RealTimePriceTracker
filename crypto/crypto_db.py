from kafka import KafkaConsumer
from json import loads
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

# Define the SQLAlchemy model
Base = declarative_base()


class StockData(Base):
    __tablename__ = 'crypto'

    id = Column(Integer, primary_key=True)
    opening_price: float
    high_price: float
    low_price: float
    last_price: float
    price_change: float
    price_change_percent: float
    volume: float


# Kafka configuration
kafka_config = {
    'bootstrap_servers': 'localhost:9092',
    'group_id': 'my-group-id'
}


# MySQL configuration (Amazon RDS)
db_username = 'your_username'
db_password = 'your_password'
db_endpoint = 'your_rds_endpoint'
db_name = 'your_db_name'

db_connection_string = f'mysql://{db_username}:{db_password}@{db_endpoint}/{db_name}'
db_engine = create_engine(db_connection_string)
Session = sessionmaker(bind=db_engine)


consumer = KafkaConsumer(
    'bithumb',  # Change to your Kafka topic name
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group-id',
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

Base.metadata.create_all(db_engine)  # Create the database table if it doesn't exist


try:
    for event in consumer:
        event_data = event.value
        print(event_data)

        # Create a new record and insert it into the database
        session = Session()
        stock_record = StockData(
            opening_price = event_data['opening_price'],
            high_price = event_data['high_price'],
            low_price = event_data['low_price'],
            last_price = event_data['last_price'],
            price_change = event_data['price_change'],
            price_change_percent = event_data['price_change_percent'],
            volume = event_data['volume']
        )
        session.add(stock_record)
        session.commit()
        session.close()

except KeyboardInterrupt:
    print("Kafka Consumer closed.")