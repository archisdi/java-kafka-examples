package com.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerThreadDemo {
    Logger logger = LoggerFactory.getLogger(ConsumerThreadDemo.class);

    public static void main(String[] args) {
        new ConsumerThreadDemo().run();
    }

    public ConsumerThreadDemo() {
    }

    public void run() {
        String server = "127.0.0.1:9092";
        String groupId = "my-app";
        String topic = "second_topic";

        CountDownLatch countDownLatch = new CountDownLatch(1);
        ConsumerThread consumerThread = new ConsumerThread(countDownLatch, server, groupId, topic);

        Thread thread = new Thread(consumerThread);
        thread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("caught shutdown hook");
            consumerThread.shutdown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("app interrupted", e);
        } finally {
            logger.info("app is closing");
        }
    }

    public class ConsumerThread implements Runnable {
        private final CountDownLatch latch;
        private final KafkaConsumer<String, String> consumer;

        public ConsumerThread(
                CountDownLatch latch,
                String bootstrapServers,
                String groupId,
                String topic
        ) {
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


            consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                while (true) {
                    ConsumerRecords<String, String> consumerRecords = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record: consumerRecords) {
                        logger.info("Key: " + record.key() + " ,Value: " + record.value());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown();
            }
        }

        public void shutdown() {
            // is a special method to interrupt
            consumer.wakeup();
        }
    }
}
