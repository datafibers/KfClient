package com.netease.middleware.kfclient.producer;

import static com.netease.middleware.kfclient.producer.Constants.DEFAULT_SERIALIZER_CLASS;
import static com.netease.middleware.kfclient.producer.Constants.METADATE_BROKER_LIST;
import static com.netease.middleware.kfclient.producer.Constants.REQUEST_REQUIRED_ACKS;
import static com.netease.middleware.kfclient.producer.Constants.SERIALIZER_CLASS;

import java.util.List;
import java.util.Properties;

import kafka.admin.TopicCommand;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * @title KfProducer.java
 * @author bjpengpeng
 * @date 2015年7月27日
 */
public class KfProducer {
    
    private Producer<Integer, String> producer = null;
    private final Properties props = new Properties();
    
    public KfProducer() {
        props.put(SERIALIZER_CLASS, DEFAULT_SERIALIZER_CLASS);
        props.put(METADATE_BROKER_LIST, "181.92.243.114:9092");
        props.put(REQUEST_REQUIRED_ACKS, "1");
        ProducerConfig producerConfig = new ProducerConfig(props);
        producer = new Producer<Integer,String>(producerConfig);
    }
    
    public void sendMsg(final String topic, final String msg) {
        if (Strings.isNullOrEmpty(topic) || null == msg) {
            return;
        }
        if (null == producer) {
            throw new NullPointerException("kfclient instance produce failed!");
        }
        producer.send(new KeyedMessage<Integer, String>(topic, msg));
//        if (null != producer) {
//            producer.close();
//        }
    }
    
    private List<KeyedMessage<Integer, String>> encapsulateKeyedMessages(final String topic, final String ...msgs) {
        if (Strings.isNullOrEmpty(topic) || null == msgs || 0 >= msgs.length) {
            return null;
        }
        List<KeyedMessage<Integer, String>> keyedMsgs = Lists.newLinkedList();
        for (String msg : msgs) {
            KeyedMessage<Integer, String> keyedMsg = new KeyedMessage<Integer, String>(topic,msg);
            keyedMsgs.add(keyedMsg);
        }
        return keyedMsgs;
    }
    
    public void batchSendMsg(final String topic, final String ...msgs) {
        if (Strings.isNullOrEmpty(topic) || null == msgs) {
            return;
        }
        if (null == producer) {
            throw new NullPointerException("kfclient instance produce failed!");
        }
        List<KeyedMessage<Integer, String>> keyedMsgs = encapsulateKeyedMessages(topic,msgs);
        if (null != keyedMsgs) {
            producer.send(keyedMsgs);
        }
//        if (null != producer) {
//            producer.close();
//        }
    }
    
    private List<KeyedMessage<Integer, String>> encapsulateKeyedMessages(final String topic, final List<String> msgs) {
        if (Strings.isNullOrEmpty(topic) || null == msgs || msgs.isEmpty()) {
            return null;
        }
        List<KeyedMessage<Integer, String>> keyedMsgs = Lists.newLinkedList();
        for (String msg : msgs) {
            KeyedMessage<Integer, String> keyedMsg = new KeyedMessage<Integer, String>(topic, msg);
            keyedMsgs.add(keyedMsg);
        }
        return keyedMsgs;
    }
    
    public void batchSendMsg(final String topic, final List<String> msgs) {
        if (Strings.isNullOrEmpty(topic) || null == msgs || msgs.isEmpty()) {
            return;
        }
        if (null == producer) {
            throw new NullPointerException("kfclient instance produce failed!");
        }
        List<KeyedMessage<Integer, String>> keyedMsgs = encapsulateKeyedMessages(topic,msgs);
        if (null != keyedMsgs) {
            producer.send(keyedMsgs);
        }
//        if (null != producer) {
//            producer.close();
//        }
    }
    
    public static void main(String[] args) {
        KfProducer producer = new KfProducer();
        producer.sendMsg("jipiao_test2", "jipiao test1!");
        producer.sendMsg("jipiao_test2", "jipiao test2!");
        producer.sendMsg("jipiao_test2", "jipiao test3!");
    }
}
