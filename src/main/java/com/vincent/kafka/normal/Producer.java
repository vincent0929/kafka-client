package com.vincent.kafka.normal;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j(topic = "producer")
public class Producer implements Runnable {

    private KafkaProducer<String, String> producer;

    private String topic;

    private volatile boolean flag = true;

    private static AtomicLong messageIndex;

    public Producer(String bootstrapServer, String topic, AtomicLong messageIndex) {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("ack", "1");

        producer = new KafkaProducer<>(properties);
        this.topic = topic;
        Producer.messageIndex = messageIndex;
    }

    public void stop() {
        this.flag = false;
    }

    public void send(String message, String key) {
        producer.send(new ProducerRecord<>(topic, key, message), (metadata, e) -> {
            if (metadata != null) {
                int partition = metadata.partition();
                long offset = metadata.offset();
                String timeStamp = DateTimeFormatter.ISO_DATE_TIME.format(LocalDateTime.ofEpochSecond(metadata.timestamp() / 1000,
                        0, ZoneId.systemDefault().getRules().getOffset(Instant.now())));
                log.info("消息发送成功,producer={},message={},partition={},offset={},timestamp={}", Thread.currentThread().getName(), message, partition, offset, timeStamp);
            } else {
                log.error("消息发送失败", e);
            }
        });
    }

    @Override
    public void run() {
        Random random = new Random();
        while (flag) {
            long index = messageIndex.getAndIncrement();
            this.send("Message" + index, String.valueOf(index));
            try {
                int i = random.nextInt(10);
                Thread.sleep(i * 100);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    public static void main(String[] args) {
        for (int i = 0 ;i < 2; i ++) {
            Producer producer = new Producer("http://localhost:9092", "test-topic", new AtomicLong());
            Thread thread = new Thread(producer);
            thread.start();
        }
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            log.error("dfa", e);
        }
    }
}
