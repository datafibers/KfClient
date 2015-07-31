package com.netease.middleware.kfclient.admin;

import kafka.admin.TopicCommand;

/**
 * @title TopicCommand.java
 * @author bjpengpeng
 * @date 2015年7月31日
 */
public enum KfTopicCommand {
    
    LIST_TOPIC("--list --zookeeper %s") {
        @Override
        public void exec(Object... args) {
            String format = this.getFormat();
            String command = String.format(format, args);
            TopicCommand.main(new String[]{command});
        }
    };
    
    public abstract void exec(Object ...args);
    
    private KfTopicCommand(String format) {
        this.format = format;
    }
    
    protected String getFormat() {
        return format;
    }
    
    private String format;
}
