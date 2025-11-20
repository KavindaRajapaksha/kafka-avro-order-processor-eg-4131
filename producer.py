#!/usr/bin/env python3
"""
Kafka Producer with Avro serialization using confluent-kafka and fastavro
"""

import json
import uuid
import random
from confluent_kafka import Producer
from fastavro import writer, parse_schema
from faker import Faker
import io


class AvroKafkaProducer:
    def __init__(self, bootstrap_servers, schema_file):
        """
        Initialize the Kafka producer with Avro schema
        
        Args:
            bootstrap_servers: Kafka broker address
            schema_file: Path to the Avro schema file (.avsc)
        """
        # Configure Kafka producer
        self.producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'avro-producer'
        }
        self.producer = Producer(self.producer_config)
        
        # Load and parse Avro schema
        with open(schema_file, 'r') as f:
            schema_dict = json.load(f)
        self.schema = parse_schema(schema_dict)
        
    def serialize_avro(self, record):
        """
        Serialize a record using Avro schema
        
        Args:
            record: Dictionary containing the record data
            
        Returns:
            bytes: Serialized Avro data
        """
        bytes_writer = io.BytesIO()
        writer(bytes_writer, self.schema, [record])
        return bytes_writer.getvalue()
    
    def delivery_callback(self, err, msg):
        """
        Callback for message delivery reports
        
        Args:
            err: Error if delivery failed
            msg: Message that was delivered
        """
        if err:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')
    
    def send_message(self, topic, record, key=None):
        """
        Send a message to Kafka topic
        
        Args:
            topic: Kafka topic name
            record: Dictionary containing the order data
            key: Optional message key
        """
        try:
            # Serialize the record
            avro_bytes = self.serialize_avro(record)
            
            # Produce message to Kafka
            self.producer.produce(
                topic=topic,
                value=avro_bytes,
                key=key,
                callback=self.delivery_callback
            )
            
            # Trigger delivery reports
            self.producer.poll(0)
            
        except Exception as e:
            print(f'Error sending message: {e}')
    
    def flush(self):
        """
        Wait for all messages to be delivered
        """
        print('Flushing pending messages...')
        self.producer.flush()


def main():
    """
    Main function to demonstrate the producer
    """
    # Initialize Faker
    fake = Faker()
    
    # Initialize producer
    producer = AvroKafkaProducer(
        bootstrap_servers='localhost:9092',
        schema_file='order.avsc'
    )
    
    # Kafka topic
    topic = 'orders'
    
    print(f'Generating and sending 15 random orders to topic: {topic}')
    print('-' * 80)
    
    # Generate and send 15 random order records
    for i in range(15):
        # Generate random order data
        order = {
            'orderId': str(uuid.uuid4()),
            'product': fake.commerce_product_name() if hasattr(fake, 'commerce_product_name') else fake.word().capitalize() + ' ' + fake.word().capitalize(),
            'price': round(random.uniform(5.0, 1500.0), 2)
        }
        
        print(f'\n[{i+1}/15] Generating order:')
        print(f'  Order ID: {order["orderId"]}')
        print(f'  Product: {order["product"]}')
        print(f'  Price: LKR {order["price"]:.2f}')
        
        # Serialize the order using Avro schema
        try:
            avro_bytes = producer.serialize_avro(order)
            print(f'  Serialized to {len(avro_bytes)} bytes')
            
            # Produce the serialized message to Kafka
            producer.producer.produce(
                topic=topic,
                value=avro_bytes,
                key=order['orderId'].encode('utf-8'),
                callback=producer.delivery_callback
            )
            
            # Poll to handle delivery reports
            producer.producer.poll(0)
            
        except Exception as e:
            print(f'  ERROR: Failed to send order: {e}')
    
    # Wait for all messages to be delivered
    print('\n' + '-' * 80)
    print('Flushing remaining messages...')
    producer.flush()
    print('All messages sent successfully!')


if __name__ == '__main__':
    main()
