package com.netease.middleware.kfclient.consumer;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

/**
 * @title KfConsumer.java
 * @author bjpengpeng
 * @date 2015年7月28日
 */
public class KfConsumer implements IConsumer{
    
    private ConsumerConnector consumer = null;
    
    private ConsumerConfig initConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", "127.0.0.1:2182");
        props.put("group.id", "jipiao_test");
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }
    
    public KfConsumer() {
        consumer = Consumer.createJavaConsumerConnector(initConsumerConfig());
    }
    
    public void createDurableSubscriber(String topic) {
        if (Strings.isNullOrEmpty(topic)) {
            throw new NullPointerException("consumer topic must not null!");
        }
        Map<String, Integer> topicCountMap = Maps.<String, Integer>newHashMap();
        topicCountMap.put(topic, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream =  consumerMap.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> iter = stream.iterator();
        while (iter.hasNext()) {
            MessageAndMetadata<byte[], byte[]> val = iter.next();
            System.out.println(new String(val.message()));
        }
    }
    
    public static void main(String[] args) {
        KfConsumer consumer = new KfConsumer();
        consumer.createDurableSubscriber("jipiao_topic");
    }
}
