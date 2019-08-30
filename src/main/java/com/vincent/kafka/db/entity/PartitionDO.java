package com.vincent.kafka.db.entity;

import lombok.Data;
import org.apache.ibatis.type.Alias;

@Alias("kafkaPartition")
@Data
public class PartitionDO {

    private Long id;

    private String topic;

    private Integer partitionIndex;

    private Long partitionOffset;
}
