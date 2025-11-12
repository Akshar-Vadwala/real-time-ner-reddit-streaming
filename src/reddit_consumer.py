import json
from kafka import KafkaConsumer


def consume_from_kafka():
    """
    Function to print the details of reddit stream to terminal from Kafka Broker
 
    """

    consumer = KafkaConsumer(
        'reddit-stream',
        bootstrap_servers = ['localhost:9092'],
        auto_offset_reset = 'earliest',
        value_deserializer = lambda v: json.loads(v.decode('utf-8'))
    )

    print(f"Starting to consume messages...")

    for message in consumer:
        print(f"Received message: Topic={message.topic}, Offset={message.offset}, Value={message.value}")


if __name__ == "__main__":
    consume_from_kafka()
    