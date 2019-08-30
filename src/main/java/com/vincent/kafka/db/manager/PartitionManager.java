package com.vincent.kafka.db.manager;

import com.vincent.kafka.db.entity.PartitionDO;
import com.vincent.kafka.db.mapper.PartitionMapper;
import org.apache.ibatis.io.Resources;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;

import java.io.IOException;
import java.io.InputStream;

public class PartitionManager {

    private PartitionMapper partitionMapper;

    private static PartitionManager manager = null;

    private PartitionManager() {
        try {
            String resource = "mybatis.xml";
            InputStream inputStream = Resources.getResourceAsStream(resource);
            SqlSessionFactory sqlSessionFactory = new SqlSessionFactoryBuilder().build(inputStream);
            SqlSession sqlSession = sqlSessionFactory.openSession();
            partitionMapper = sqlSession.getMapper(PartitionMapper.class);
        } catch (IOException e) {
            // ignore
        }
    }

    public static PartitionManager getInstance() {
        if (manager == null) {
            synchronized (PartitionManager.class) {
                if (manager == null) {
                    manager = new PartitionManager();
                }
            }
        }
        return manager;
    }

    public PartitionDO getByTopicAndPartition(String topic, Integer partition) {
        return partitionMapper.getByTopicAndPartition(topic, partition);
    }

    public void save(String topic, Integer partition, Long offset) {
        PartitionDO partitionDO = new PartitionDO();
        partitionDO.setTopic(topic);
        partitionDO.setPartitionIndex(partition);
        partitionDO.setPartitionOffset(offset);
        partitionMapper.save(partitionDO);
    }
}
