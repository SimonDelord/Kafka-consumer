import logging
from json import loads
from kafka import KafkaConsumer


def main():
    try:
        # To consume latest messages and auto-commit offsets
        consumer = KafkaConsumer(
            'acs-topic',
            bootstrap_servers='172.30.2.176:9092',
            value_deserializer=lambda x: loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True,
        )
        for message in consumer:
            print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                 message.offset, message.key, message.value))

    except Exception as e:
        logging.info('Connection successful', e)

if __name__ == '__main__':
    main()