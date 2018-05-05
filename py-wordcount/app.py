import sys
import os
import pika
import json

from collections import Counter

from confluent_kafka import Consumer, KafkaException, KafkaError

if __name__ == '__main__':
    topics = [os.environ['CLOUDKARAFKA_TOPIC']]
    exchange = 'twitter.exchange'
    routeKey = 'twitter.word.count'

    # Consumer configuration
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    conf = {
        'bootstrap.servers': os.environ['CLOUDKARAFKA_BROKERS'],
        'group.id': "word-count",
        'session.timeout.ms': 6000,
        'default.topic.config': {'auto.offset.reset': 'smallest'},
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'SCRAM-SHA-256',
        'sasl.username': os.environ['CLOUDKARAFKA_USERNAME'],
        'sasl.password': os.environ['CLOUDKARAFKA_PASSWORD']
    }

    kafkaConsumer = Consumer(**conf)
    kafkaConsumer.subscribe(topics)

    url = os.environ.get('CLOUDAMQP_URL')
    params = pika.URLParameters(url)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(exchange=exchange, exchange_type='topic')

    def processMsg(msg):
        return dict(Counter(msg.lower().split()))

    def sendResults(results):
        msg = json.dumps(results)
        channel.basic_publish(exchange=exchange, routing_key=routeKey, body=msg)

    try:
        while True:
            msg = kafkaConsumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                # Error or event
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    # Error
                    raise KafkaException(msg.error())
            else:
                # Proper message
                sys.stderr.write('%s [%d] at offset %d with key %s:\n' %
                                 (msg.topic(), msg.partition(), msg.offset(),
                                  str(msg.key())))
                sendResults(processMsg(msg.value()))

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')

    # Close down consumer to commit final offsets.
    kafkaConsumer.close()
