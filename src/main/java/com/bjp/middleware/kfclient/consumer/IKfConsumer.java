package com.bjp.middleware.kfclient.consumer;

public interface IKfConsumer {
    
    /**
     * 对某一主题进行continuous订阅
     * @date 2015年7月31日
     * @description
     * @param topic
     */
    public void createDurableSubscriber(String topic);
}
