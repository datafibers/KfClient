package com.bjp.middleware.kfclient.admin;

public interface IKfTopicAdmin {
    
    /**
     * 列出zookeeper中所有的主题
     */
    public void listTopics();
    
    /**
     * 删除zookeeper中的相关主题  
     * @param topic
     */
    public void deleteTopic(String topic);
    
    /**
     * 创建指定topic的主题
     * @param topic
     * @param partions
     * @param replicationFactor
     */
    public void createTopic(String topic, Integer partions, Integer replicationFactor);
    
    /**
     * 列出指定topic的细节性信息
     * @param topic
     */
    public void describeTopic(String topic);
}
