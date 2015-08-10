package com.netease.middleware.kfclient.admin;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.netease.middleware.kfclient.Constants;

public class KfTopicAdmin implements IKfTopicAdmin{
    
    private String zookeeperHost = Constants.EMPTY_STRING;
    private Integer zookeeperPort;
    
    /**
     * 需传入zookeeper主机名及端口号的构造函数
     * @param zookeeperHost
     * @param zookeeperPort
     */
    public KfTopicAdmin(String zookeeperHost, Integer zookeeperPort) {
        if (Strings.isNullOrEmpty(zookeeperHost) || null == zookeeperPort) {
            throw new IllegalArgumentException("topic admin zookeeper host or port must not be null or empty");
        }
        this.zookeeperHost = zookeeperHost;
        this.zookeeperPort = zookeeperPort;
    }
    
    /**
     * 需传入配置文件的构造函数，配置文件要求必须有zookeeper相关的配置 
     * @param configPath
     */
    public KfTopicAdmin(String configPath) {
    }
    
    public void listTopics() {
        KfTopicCommand command = KfTopicCommand.LIST_TOPIC;
        String zookeeperConnStr = Joiner.on(Constants.COLON_STRING).join(zookeeperHost,zookeeperPort);
        command.exec(zookeeperConnStr);
    }
    
    public void deleteTopic(String topic) {
        KfTopicCommand command = KfTopicCommand.DELETE_TOPIC;
        String zookeeperConnStr = Joiner.on(Constants.COLON_STRING).join(zookeeperHost,zookeeperPort);
        command.exec(zookeeperConnStr,topic);
    }

    public void createTopic(String topic, Integer partions, Integer replicationFactor) {
        KfTopicCommand command = KfTopicCommand.CREATE_TOPIC;
        String zookeeperConnStr = Joiner.on(Constants.COLON_STRING).join(zookeeperHost,zookeeperPort);
        command.exec(zookeeperConnStr,partions,topic, replicationFactor);
    }

    public void describeTopic(String topic) {
        KfTopicCommand command = KfTopicCommand.DESCRIBE_TOPIC;
        String zookeeperConnStr = Joiner.on(Constants.COLON_STRING).join(zookeeperHost,zookeeperPort);
        command.exec(zookeeperConnStr,topic);
    }
    
    public String getZookeeperHost() {
        return zookeeperHost;
    }

    public void setZookeeperHost(String zookeeperHost) {
        this.zookeeperHost = zookeeperHost;
    }

    public Integer getZookeeperPort() {
        return zookeeperPort;
    }

    public void setZookeeperPort(Integer zookeeperPort) {
        this.zookeeperPort = zookeeperPort;
    }

    public static void main(String[] args) {
        KfTopicAdmin admin = new KfTopicAdmin("127.0.0.1", 2182);
//        admin.deleteTopic("jipiao_topic");
        admin.createTopic("jipiao_topic", 3, 2);
        admin.listTopics();
//        admin.describeTopic("jipiao_test2");
    }
}
