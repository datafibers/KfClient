package com.netease.middleware.kfclient.admin;

/**
 * @title KfTopicAdmin.java
 * @author bjpengpeng
 * @date 2015年7月31日
 */
public class KfTopicAdmin {
    
    public KfTopicAdmin() {
    }
    
    public static  void listTopics(KfTopicCommand command, String zooKeeperConnectStr) {
        command.exec(zooKeeperConnectStr);
    }
    
    public static void main(String[] args) {
        listTopics(KfTopicCommand.LIST_TOPIC, "127.0.0.1:2182");
    }
}
