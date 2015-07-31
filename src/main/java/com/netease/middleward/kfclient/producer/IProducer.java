package com.netease.middleward.kfclient.producer;

import java.util.List;

/**
 * 
 * @title IProducer.java
 * @author bjpengpeng
 * @date 2015年7月28日
 */
public interface IProducer {
    
    /**
     * @author bjpengpeng
     * @date 2015年7月28日
     * @description 一次向kafka broker发送一条信息
     * @param topic
     * @param msg
     */
    public void sendMsg(final String topic, final String msg);
    
    /**
     * @author bjpengpeng
     * @date 2015年7月28日
     * @description 一次向kafka broker发送多条信息
     * @param topic
     * @param msgs
     */
    public void batchSendMsg(final String topic, final String ...msgs);
    
    /**
     * @author bjpengpeng
     * @date 2015年7月28日
     * @description 一次向kafka broker发送多条信息
     * @param topic
     * @param msgs
     */
    public void batchSendMsg(final String topic, final List<String> msgs);
}
