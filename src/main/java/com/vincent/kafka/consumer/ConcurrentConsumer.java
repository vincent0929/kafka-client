package com.vincent.kafka.consumer;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

@Slf4j(topic = "consumer")
public class ConcurrentConsumer extends Thread {

    private KafkaConsumer<String, String> consumer;

    private ConsumerWorker[] workers;

    private Map<TopicPartition, OffsetAndMetadata> offsets = new ConcurrentHashMap<>();

    private volatile boolean enable;

    private volatile int status = 0;

    private int loopCount;

    public ConcurrentConsumer(String bootstrapServer, String groupId, String clientId, String topic) {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put("group.id", groupId);
        properties.put("client.id", clientId);
        properties.put("enable.auto.commit", "false");
        properties.put("max.poll.records", "10");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        this.consumer = new KafkaConsumer<>(properties);

        List<PartitionInfo> partitionInfos = this.consumer.partitionsFor(topic);
        int partitionNum = partitionInfos.size();

        consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
            public void onPartitionsRevoked(Collection<TopicPartition> topicPartitions) {
                List<Integer> partitions = topicPartitions.stream().map(TopicPartition::partition).collect(Collectors.toList());
                log.info("再均衡之前,consumer={},partitions={}", Thread.currentThread().getName(),
                        partitions.stream().map(String::valueOf).collect(Collectors.joining(",")));

                for (int i = 0; i < partitionNum; i++) {
                    if (partitions.contains(i)) {
                        continue;
                    }
                    offsets.remove(new TopicPartition(topic, i));
                }
                Map<Integer, Long> offsetMap = offsets.entrySet().stream()
                        .collect(Collectors.toMap(entry -> entry.getKey().partition(), entry -> entry.getValue().offset()));
                log.info("再均衡之前提交分区偏移量,consumer={},offset={}", Thread.currentThread().getName(), JSON.toJSONString(offsetMap));
                consumer.commitSync(offsets);
            }

            public void onPartitionsAssigned(Collection<TopicPartition> topicPartitions) {
                List<Integer> partitions = topicPartitions.stream().map(TopicPartition::partition).collect(Collectors.toList());
                log.info("再均衡之后,consumer={},partitions={}", Thread.currentThread().getName(),
                        partitions.stream().map(String::valueOf).collect(Collectors.joining(",")));
                for (int i = 0; i < partitionNum; i++) {
                    ConsumerWorker worker = workers[i];
                    if (partitions.contains(i)) {
                        if (worker == null) {
                            workers[i] = new ConsumerWorker(i, offsets);
                            workers[i].start();
                        }
                    } else {
                        if (worker != null) {
                            worker.shutdown();
                            workers[i] = null;
                        }
                    }
                }
                offsets.clear();
            }
        });

        this.workers = new ConsumerWorker[partitionNum];
        this.enable = true;
        this.setName("kafka-consumer-" + clientId);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shutdown();
            int count = 0;
            do {
                log.info("等待work结束运行，status={}", status);
                if (status == 2) {
                    break;
                }
                if (++count > 100) {
                    break;
                }
                try {
                    Thread.sleep(20);
                } catch (InterruptedException e) {
                    // ignore
                }
            } while (true);
        }));
    }

    public void shutdown() {
        this.enable = false;
        this.consumer.wakeup();
        this.interrupt();
        if (ArrayUtils.isEmpty(workers)) {
            return;
        }
        for (ConsumerWorker worker : workers) {
            if (worker != null) {
                worker.shutdown();
            }
        }
    }

    @Override
    public void run() {
        try {
            status = 1;
            while (enable) {
                try {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    if (records == null || records.isEmpty()) {
                        Thread.sleep(1000);
                        continue;
                    }
                    CountDownLatch countDownLatch = new CountDownLatch(records.count());
                    for (TopicPartition topicPartition : records.partitions()) {
                        List<ConsumerRecord<String, String>> partitionRecords = records.records(topicPartition);
                        int partition = topicPartition.partition();
                        ConsumerWorker worker = this.workers[partition];
                        if (worker != null) {
                            worker.push(partitionRecords, countDownLatch);
                        } else {
                            for (int i = 0; i < partitionRecords.size(); i++) {
                                countDownLatch.countDown();
                            }
                        }
                    }
                    countDownLatch.await();

                    loopCount++;
                    if (loopCount % 5 == 0) {
                        loopCount = 0;
                        this.consumer.commitSync(offsets);
                    } else {
                        this.consumer.commitAsync(offsets, null);
                    }
                } catch (WakeupException | InterruptedException e) {
                    // ignore
                }
            }
            this.consumer.commitSync(offsets);
            status = 2;
        } finally {
            this.consumer.close();
        }
    }

    public static void main(String[] args) throws InterruptedException {
        String bootstrapServer = "http://localhost:9092";
        String groupId = "test-consumer";
        String topic = "test-topic";
        for (int i = 0; i < 4; i++) {
            String clientId = i + "";
            ConcurrentConsumer concurrentConsumer = new ConcurrentConsumer(bootstrapServer, groupId, clientId, topic);
            concurrentConsumer.start();
            Thread.sleep(new Random().nextInt(4000) + 1000);
        }
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            // ignore
        }
    }
}
