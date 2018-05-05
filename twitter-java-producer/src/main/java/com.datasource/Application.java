package com.datasource;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.lang.Runtime;
import java.io.*;

import twitter4j.conf.ConfigurationBuilder;
import twitter4j.TwitterFactory;
import twitter4j.Twitter;
import twitter4j.Query;
import twitter4j.TwitterException;
import twitter4j.Status;
import twitter4j.QueryResult;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;

// mvn package && java -jar target/gs-spring-boot-0.1.0.jar
@SpringBootApplication
public class Application {
    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    @Bean
    public CommandLineRunner dataSource(){
        return args -> {
            String brokers = System.getenv("CLOUDKARAFKA_BROKERS");
            String topic = System.getenv("CLOUDKARAFKA_TOPIC");
            String username = System.getenv("CLOUDKARAFKA_USERNAME");
            String password = System.getenv("CLOUDKARAFKA_PASSWORD");
            String OAuthConsumerKey = System.getenv("OAuthConsumerKey");
            String OAuthConsumerSecret = System.getenv("OAuthConsumerSecret");
            String OAuthAccessToken = System.getenv("OAuthAccessToken");
            String OAuthAccessTokenSecret = System.getenv("OAuthAccessTokenSecret");

            String jaasTemplate = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";
            String jaasCfg = String.format(jaasTemplate, username, password);

            Properties props = new Properties();
            props.put("bootstrap.servers", brokers);
            props.put("group.id", "twitter-producer");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            props.put("auto.offset.reset", "earliest");
            props.put("session.timeout.ms", "30000");
            props.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
            props.put("value.serializer", org.apache.kafka.common.serialization.StringSerializer.class.getName());
            props.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class.getName());
            props.put("value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class.getName());
            props.put("security.protocol", "SASL_SSL");
            props.put("sasl.mechanism", "SCRAM-SHA-256");
            props.put("sasl.jaas.config", jaasCfg);

            Producer producer = new KafkaProducer<>(props);

            ConfigurationBuilder cb = new ConfigurationBuilder();
            cb.setDebugEnabled(true)
                    .setOAuthConsumerKey(OAuthConsumerKey)
                    .setOAuthConsumerSecret(OAuthConsumerSecret)
                    .setOAuthAccessToken(OAuthAccessToken)
                    .setOAuthAccessTokenSecret(OAuthAccessTokenSecret);

            TwitterFactory tf = new TwitterFactory(cb.build());
            Twitter twitter = tf.getInstance();

            String queryStr = "random query";
            Query query = new Query(queryStr);
            QueryResult result;
            long sinceId = 0;
            int i = 0;
            // Run 100 times, can be as long as want
            while (i < 100) {
                try {
                    query.setSinceId(sinceId);
                    result = twitter.search(query);
                    sinceId = result.getSinceId();

                    List<Status> tweets = result.getTweets();
                    for (Status tweet : tweets) {
                        producer.send(new ProducerRecord(topic, tweet.getUser().getScreenName(), tweet.getText()), (metadata, exception) -> {
                            if (metadata != null) {
                                System.out.printf("meta(partition=%d, offset=%d)\n", metadata.partition(), metadata.offset());
                            } else {
                                exception.printStackTrace();
                            }
                        });
                    }
                } catch (TwitterException te) {
                    te.printStackTrace();
                    System.out.println("Failed to search tweets: " + te.getMessage());
                }
                // Pause since twitter will throw exceptions if poll to often
                Thread.sleep(5000);
                i++;
            }

            producer.close();
        };
    }
}