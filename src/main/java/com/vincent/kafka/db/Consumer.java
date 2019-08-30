package com.vincent.kafka.db;

import com.vincent.kafka.db.entity.PartitionDO;
import com.vincent.kafka.db.manager.PartitionManager;
import io.github.vincent0929.common.constant.BaseEnum;
import io.github.vincent0929.common.util.BizAssert;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class Consumer implements Runnable {

    private volatile boolean enable = true;

    private KafkaConsumer<String, String> consumer;

    private Map<Integer, List<Integer>> map = new ConcurrentHashMap<>();

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

            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                partitions.forEach(topicPartition -> seekCurrent(topicPartition));
            }
        });
    }

    @Override
    public void run() {
        Set<TopicPartition> assignmentPartitions = consumer.assignment();
        assignmentPartitions.forEach(this::seekCurrent);

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

                    PartitionManager.getInstance().save(record.topic(), partition, record.offset() + 1);
                }
            }
        } catch (WakeupException e) {
            // ignore
        } catch (Exception e) {
            log.error("consumer异常", e);
        } finally {
            consumer.close();
        }
    }

    public void wakeup() {
        consumer.wakeup();
    }

    private void seekCurrent(TopicPartition topicPartition) {
        String topic = topicPartition.topic();
        int partition = topicPartition.partition();
        PartitionDO partitionDO = PartitionManager.getInstance().getByTopicAndPartition(topic, partition);
        if (partitionDO != null) {
            consumer.seek(new TopicPartition(topic, partition), new OffsetAndMetadata(partitionDO.getPartitionOffset(), "no-metadata"));
        }
    }
}
