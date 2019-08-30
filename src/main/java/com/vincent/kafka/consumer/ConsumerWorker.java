package com.vincent.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j(topic = "consumer")
public class ConsumerWorker extends Thread {

    private Map<TopicPartition, OffsetAndMetadata> offsets;

    private BlockingQueue<ConsumerRecord<String, String>> queue;

    private CountDownLatch countDownLatch;

    private int partition;

    private volatile boolean enable;

    public ConsumerWorker(int partition, Map<TopicPartition, OffsetAndMetadata> offsets) {
        this.offsets = offsets;
        this.queue = new LinkedBlockingQueue<>();
        this.enable = true;
        this.partition = partition;
        this.setName("kafka-consumer-thread-" + partition);
    }

    public void push(List<ConsumerRecord<String, String>> records, CountDownLatch countDownLatch) {
        if (CollectionUtils.isEmpty(records)) {
            return;
        }
        this.countDownLatch = countDownLatch;
        records.forEach(record -> queue.offer(record));
    }

    public void shutdown() {
        this.enable = false;
    }

    @Override
    public void run() {
        log.info("启动kafka-consumer-{}", partition);
        while (enable) {
            try {
                ConsumerRecord<String, String> record = queue.poll(100, TimeUnit.MICROSECONDS);
                if (record == null) {
                    continue;
                }
                consume(record);
                offsets.put(new TopicPartition(record.topic(), record.partition()),
                        new OffsetAndMetadata(record.offset() + 1));
                this.countDownLatch.countDown();
            } catch (Exception e) {
                // ignore
            }
        }
        log.info("销毁kafka-consumer-{}", partition);
    }

    private void consume(ConsumerRecord<String, String> record) throws InterruptedException {
        Thread.sleep(new Random().nextInt(1000) + 1000);
        log.info("接收到消息：" + record.topic() + "-" + record.partition() + "-" + record.offset() + "-" + record.value());
    }
}
