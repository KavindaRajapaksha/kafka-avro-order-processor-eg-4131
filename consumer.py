#!/usr/bin/env python3
"""
Kafka Consumer with Avro deserialization using confluent-kafka and fastavro
"""

import json
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
from fastavro import reader, parse_schema
import io
import sys
import time


class AvroKafkaConsumer:
    def __init__(self, bootstrap_servers, group_id, schema_file, max_retries=3):
        """
        Initialize the Kafka consumer with Avro schema
        
        Args:
            bootstrap_servers: Kafka broker address
            group_id: Consumer group ID
            schema_file: Path to the Avro schema file (.avsc)
            max_retries: Maximum number of retries before sending to DLQ
        """
        # Configure Kafka consumer
        self.consumer_config = {
            'bootstrap.servers': bootstrap_servers,
            'group.id': group_id,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False  # Manual commit for failure handling
        }
        self.consumer = Consumer(self.consumer_config)
        
        # Configure DLQ producer
        self.dlq_producer_config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': 'dlq-producer'
        }
        self.dlq_producer = Producer(self.dlq_producer_config)
        self.dlq_topic = 'orders-dlq'
        
        # Retry configuration
        self.max_retries = max_retries
        
        # Load and parse Avro schema
        with open(schema_file, 'r') as f:
            schema_dict = json.load(f)
        self.schema = parse_schema(schema_dict)
        
    def deserialize_avro(self, avro_bytes):
        """
        Deserialize Avro bytes to a record
        
        Args:
            avro_bytes: Serialized Avro data
            
        Returns:
            dict: Deserialized record
        """
        bytes_reader = io.BytesIO(avro_bytes)
        avro_reader = reader(bytes_reader, self.schema)
        # Get the first (and only) record
        for record in avro_reader:
            return record
        return None
    
    def dlq_delivery_callback(self, err, msg):
        """
        Callback for DLQ message delivery reports
        
        Args:
            err: Error if delivery failed
            msg: Message that was delivered
        """
        if err:
            print(f'  DLQ delivery failed: {err}')
        else:
            print(f'  Message sent to DLQ topic: {msg.topic()} at offset {msg.offset()}')
    
    def send_to_dlq(self, original_msg, error_reason):
        """
        Send a failed message to the Dead Letter Queue
        
        Args:
            original_msg: The original Kafka message that failed
            error_reason: Reason for the failure
        """
        try:
            # Create DLQ message with error metadata
            dlq_headers = [
                ('error_reason', error_reason.encode('utf-8')),
                ('original_topic', original_msg.topic().encode('utf-8')),
                ('original_partition', str(original_msg.partition()).encode('utf-8')),
                ('original_offset', str(original_msg.offset()).encode('utf-8')),
                ('timestamp', str(int(time.time())).encode('utf-8'))
            ]
            
            # Send to DLQ
            self.dlq_producer.produce(
                topic=self.dlq_topic,
                value=original_msg.value(),
                key=original_msg.key(),
                headers=dlq_headers,
                callback=self.dlq_delivery_callback
            )
            
            # Trigger delivery
            self.dlq_producer.poll(0)
            
            print(f'  *** Message sent to DLQ: {self.dlq_topic} ***')
            print(f'  Reason: {error_reason}')
            
        except Exception as e:
            print(f'  ERROR: Failed to send message to DLQ: {e}')
    
    def subscribe(self, topics):
        """
        Subscribe to Kafka topics
        
        Args:
            topics: List of topic names to subscribe to
        """
        self.consumer.subscribe(topics)
        print(f'Subscribed to topics: {topics}')
    
    def consume_messages(self, timeout=1.0):
        """
        Continuously consume messages from subscribed topics
        
        Args:
            timeout: Polling timeout in seconds
        """
        try:
            print('Starting to consume messages... (Press Ctrl+C to stop)')
            print('-' * 80)
            
            message_count = 0
            # Initialize aggregation variables
            total_price = 0.0
            order_count = 0
            # Initialize DLQ statistics
            dlq_count = 0
            # Initialize failure statistics
            transient_failure_count = 0
            permanent_failure_count = 0
            
            while True:
                # Poll for messages
                msg = self.consumer.poll(timeout=timeout)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print(f'Reached end of partition {msg.partition()} at offset {msg.offset()}')
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    # Process the message
                    message_count += 1
                    result = self.process_message_with_failure_rules(msg, message_count)
                    
                    # Handle different result types
                    if result['status'] == 'success':
                        # Successfully processed, update aggregation
                        order_count += 1
                        total_price += result['price']
                        running_average = total_price / order_count
                        
                        # Print real-time aggregation statistics
                        print(f'  --- Real-time Aggregation ---')
                        print(f'  Orders Processed: {order_count}')
                        print(f'  Total Price: LKR {total_price:.2f}')
                        print(f'  Running Average: LKR {running_average:.2f}')
                        
                        # Commit offset for successful processing
                        self.consumer.commit(msg)
                        
                    elif result['status'] == 'transient_failure':
                        # Transient failure - do NOT commit offset
                        transient_failure_count += 1
                        print(f'  >> Offset NOT committed - message will be re-delivered')
                        
                    elif result['status'] == 'permanent_failure':
                        # Permanent failure - sent to DLQ
                        dlq_count += 1
                        permanent_failure_count += 1
                        # Commit offset after sending to DLQ
                        self.consumer.commit(msg)
                        print(f'  >> Offset committed after DLQ')
                    
        except KeyboardInterrupt:
            print('\n' + '-' * 80)
            print(f'Consumer interrupted. Total messages consumed: {message_count}')
            if order_count > 0:
                print(f'\n=== Final Aggregation Summary ===')
                print(f'Total Orders Processed: {order_count}')
                print(f'Total Price Sum: LKR {total_price:.2f}')
                print(f'Final Average Price: LKR {total_price / order_count:.2f}')
            if transient_failure_count > 0 or permanent_failure_count > 0:
                print(f'\n=== Failure Statistics ===')
                print(f'Transient Failures (not committed): {transient_failure_count}')
                print(f'Permanent Failures (sent to DLQ): {permanent_failure_count}')
                print(f'Total Messages in DLQ: {dlq_count}')
        finally:
            # Flush DLQ producer before closing
            self.dlq_producer.flush()
            # Close the consumer
            self.close()
    
    def process_message_with_failure_rules(self, msg, count):
        """
        Process a message with failure simulation rules
        
        Args:
            msg: Kafka message
            count: Message counter
            
        Returns:
            dict: {'status': 'success'|'transient_failure'|'permanent_failure', 'price': float or None}
        """
        try:
            # Deserialize the message value
            order = self.deserialize_avro(msg.value())
            
            if order is None:
                raise ValueError('Failed to deserialize Avro message')
            
            # Get message metadata
            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()
            key = msg.key().decode('utf-8') if msg.key() else None
            
            # Display message information
            print(f'\n[Message #{count}]')
            print(f'  Topic: {topic} | Partition: {partition} | Offset: {offset}')
            print(f'  Key: {key}')
            print(f'  Order Details:')
            print(f'    Order ID: {order["orderId"]}')
            print(f'    Product: {order["product"]}')
            print(f'    Price: LKR {order["price"]:.2f}')
            
            # Extract price
            price = order.get('price')
            
            if price is None:
                raise ValueError('Missing price field')
            
            # Failure Rule 1: Transient failure for prices between 5.0 and 50.0
            if 5.0 <= price <= 50.0:
                print(f'  ⚠️  TRANSIENT FAILURE: Price {price} is between 5.0 and 50.0')
                print(f'  Simulating transient error - message will be retried')
                return {'status': 'transient_failure', 'price': None}
            
            # Failure Rule 2: Permanent failure for prices greater than 1000.0
            if price > 1000.0:
                print(f'  ❌ PERMANENT FAILURE: Price {price} is greater than 1000.0')
                print(f'  Sending message to DLQ...')
                
                # Send to DLQ with full raw message bytes
                self.send_to_dlq(msg, f'Price exceeds threshold: {price} > 1000.0')
                
                return {'status': 'permanent_failure', 'price': None}
            
            # Success case: price is valid (not in failure ranges)
            print(f'  ✓ Successfully processed')
            return {'status': 'success', 'price': price}
            
        except Exception as e:
            print(f'  ERROR: Unexpected error during processing: {e}')
            # Treat unexpected errors as permanent failures
            self.send_to_dlq(msg, f'Processing error: {str(e)}')
            return {'status': 'permanent_failure', 'price': None}
    
    def process_message_with_retry(self, msg, count):
        """
        Process a message with retry logic
        
        Args:
            msg: Kafka message
            count: Message counter
            
        Returns:
            dict: {'success': bool, 'price': float or None}
        """
        retry_count = 0
        last_error = None
        
        while retry_count <= self.max_retries:
            try:
                # Attempt to process the message
                price = self.process_message(msg, count, retry_count)
                
                # If successful, return the price
                return {'success': True, 'price': price}
                
            except Exception as e:
                retry_count += 1
                last_error = str(e)
                
                if retry_count <= self.max_retries:
                    print(f'  RETRY {retry_count}/{self.max_retries}: Processing failed - {e}')
                    # Optional: Add exponential backoff
                    time.sleep(0.5 * retry_count)
                else:
                    print(f'  FAILED after {self.max_retries} retries: {e}')
        
        # All retries exhausted, send to DLQ
        self.send_to_dlq(msg, f'Processing failed after {self.max_retries} retries: {last_error}')
        
        return {'success': False, 'price': None}
    
    def process_message(self, msg, count, retry_attempt=0):
        """
        Process a received message
        
        Args:
            msg: Kafka message
            count: Message counter
            retry_attempt: Current retry attempt number (0 for first attempt)
            
        Returns:
            float: Price of the order if successfully processed
            
        Raises:
            Exception: If processing fails
        """
        try:
            # Deserialize the message value
            order = self.deserialize_avro(msg.value())
            
            if order is None:
                raise ValueError('Failed to deserialize Avro message')
            
            # Get message metadata
            topic = msg.topic()
            partition = msg.partition()
            offset = msg.offset()
            key = msg.key().decode('utf-8') if msg.key() else None
            
            # Display message information (only on first attempt)
            if retry_attempt == 0:
                print(f'\n[Message #{count}]')
                print(f'  Topic: {topic} | Partition: {partition} | Offset: {offset}')
                print(f'  Key: {key}')
                print(f'  Order Details:')
                print(f'    Order ID: {order["orderId"]}')
                print(f'    Product: {order["product"]}')
                print(f'    Price: LKR {order["price"]:.2f}')
            
            # Validate order data
            if not order.get('orderId'):
                raise ValueError('Missing orderId field')
            if not order.get('product'):
                raise ValueError('Missing product field')
            if order.get('price') is None or order.get('price') < 0:
                raise ValueError('Invalid price value')
            
            # Return the price for aggregation
            return order["price"]
            
        except Exception as e:
            # Re-raise the exception to trigger retry logic
            raise Exception(f'Error processing message: {e}')
    
    def close(self):
        """
        Close the consumer connection
        """
        print('Closing consumer...')
        self.consumer.close()
        print('Consumer closed successfully.')


def main():
    """
    Main function to run the consumer
    """
    # Initialize consumer
    consumer = AvroKafkaConsumer(
        bootstrap_servers='localhost:9092',
        group_id='order-consumer-group',
        schema_file='order.avsc'
    )
    
    # Subscribe to the orders topic
    consumer.subscribe(['orders'])
    
    # Start consuming messages
    consumer.consume_messages()


if __name__ == '__main__':
    main()
