package com.vincent.kafka.normal;

import com.alibaba.fastjson.JSON;
import io.github.vincent0929.common.constant.BaseEnum;
import io.github.vincent0929.common.util.BizAssert;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class Consumer implements Runnable {

    private KafkaConsumer<String, String> consumer;

    private volatile boolean enable = true;

    @Getter
    private Map<Integer, List<Integer>> map = new ConcurrentHashMap<>();

    private volatile Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

    public Consumer(String bootstrapServer, String groupId, String clientId, String topic) {
        Properties properties = new Properties();
        properties.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.put("group.id", groupId);
        properties.put("client.id", clientId);
        properties.put("enable.auto.commit", "false");
        properties.put("max.poll.records", "10");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
            public void onPartitionsRevoked(Collection<TopicPartition> topicPartitions) {
                String partitions = topicPartitions.stream().map(topicPartition -> String.valueOf(topicPartition.partition()))
                        .collect(Collectors.joining(","));
                log.info("再均衡之前,consumer={},partitions={}", Thread.currentThread().getName(), partitions);

                Map<Integer, Long> offsetMap = offsets.entrySet().stream()
                        .collect(Collectors.toMap(entry -> entry.getKey().partition(), entry -> entry.getValue().offset()));
                log.info("再均衡之前提交分区偏移量,consumer={},offset={}", Thread.currentThread().getName(), JSON.toJSONString(offsetMap));
                consumer.commitSync(offsets);
            }

            public void onPartitionsAssigned(Collection<TopicPartition> topicPartitions) {
                String partitions = topicPartitions.stream().map(topicPartition -> String.valueOf(topicPartition.partition()))
                        .collect(Collectors.joining(","));
                log.info("再均衡之后,consumer={},partitions={}", Thread.currentThread().getName(), partitions);
                Map<TopicPartition, OffsetAndMetadata> deleteOffsets = new HashMap<>();
                offsets.forEach((key, value) -> {
                    TopicPartition partition = new TopicPartition(topic, key.partition());
                    if (!topicPartitions.contains(partition)) {
                        deleteOffsets.put(key, value);
                    }
                });
                for (Map.Entry<TopicPartition, OffsetAndMetadata> entry : deleteOffsets.entrySet()) {
                    offsets.remove(entry.getKey());
                }
            }
        });
    }

    @Override
    public void run() {
        String assignPartitions = consumer.assignment().stream().map(partition ->
                String.valueOf(partition.partition())).collect(Collectors.joining(","));
        log.info("被分配的分区:{}", assignPartitions);

        try {
            while (enable) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                if (records.isEmpty()) {
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // ignore
                    }
                    continue;
                }

                Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
                while (iterator.hasNext()) {
                    ConsumerRecord<String, String> record = iterator.next();

                    String message = record.value();
                    log.info("接收消息:consumer={},message={},partition={},offset={}", Thread.currentThread().getName(), message, record.partition(), record.offset());

                    // 模拟处理消息耗时
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // ignore
                    }

                    int partition = record.partition();
                    Integer index = Integer.valueOf(message.substring(7));
                    List<Integer> nums = map.getOrDefault(partition, new ArrayList<>());
                    if (CollectionUtils.isNotEmpty(nums)) {
                        Integer last = nums.get(nums.size() - 1);
                        BizAssert.isTrue(!nums.contains(index), new BaseEnum<Integer>() {
                            @Override
                            public Integer getCode() {
                                return 500;
                            }

                            @Override
                            public String getDesc() {
                                return "消息重复消费,consumer=" + Thread.currentThread().getName() + ",partition=" + partition;
                            }
                        });
                        BizAssert.isTrue(last < index, new BaseEnum<Integer>() {
                            @Override
                            public Integer getCode() {
                                return 500;
                            }

                            @Override
                            public String getDesc() {
                                return "消息顺序性出错,consumer=" + Thread.currentThread().getName() + ",partition=" + partition;
                            }
                        });
                    }
                    nums.add(index);
                    map.put(partition, nums);

                    offsets.put(new TopicPartition(record.topic(), record.partition()),
                            new OffsetAndMetadata(record.offset() + 1, "mo-metadata"));
                    consumer.commitAsync(offsets, null);
                }
            }
        } catch (WakeupException e) {
            // ignore
        } catch (Exception e) {
            log.error("consumer异常", e);
        } finally {
            try {
                consumer.commitSync(offsets);
            } finally {
                consumer.close();
            }
        }
    }

    public void wakeup() {
        consumer.wakeup();
    }

    public static void main(String[] args) {
        Consumer consumer = new Consumer("http://localhost:9092", "test-consumer", "1", "test-topic");
        Thread thread = new Thread(consumer);
        thread.start();
        try {
            Thread.currentThread().join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
