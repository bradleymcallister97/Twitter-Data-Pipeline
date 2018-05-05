# Twitter Data Pipeline

Moves data from Twitter to Elasticsearch through Kafka and RabbitMQ.

## Process:
- Tweets are queried through the Twitter API
- Tweet is published to Kafka with the twitter username as the key for the ProducerRecord
- Tweets are consumed, and the word count is calculated for each Tweet
- Word count is serialized to JSON string
- Word count string is published to RabbitMQ
- Word count sting is consumed
- Word count string is parsed to JSON object
- Word count JSON object is saved to Elasticsearch

## Services
### twitter-java-producer
Java service which pulls data from Twitter and publishes it to Kafka

### py-wordcount
Python services which consumes from Kafka. Once a tweet is consumed the word count is calculated. The word count is published to RabbitMQ

### kafka-elasticsearch-connector
Node service which consumes from RabbitMQ. Once a word count object is consumed it is stored to Elasticsearch
