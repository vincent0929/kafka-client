package com.vincent.kafka.db.mapper;

import com.vincent.kafka.db.entity.PartitionDO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface PartitionMapper {

    PartitionDO getByTopicAndPartition(@Param("topic") String topic, @Param("partitionIndex") Integer partitionIndex);

    void save(PartitionDO partitionDO);
}
