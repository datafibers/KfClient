package com.netease.middleware.kfclient.admin;
import kafka.admin.TopicCommand;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.netease.middleware.kfclient.Constants;

/**
 * @title TopicCommand.java
 * @author bjpengpeng
 * @date 2015年7月31日
 */
public enum KfTopicCommand {
    
    LIST_TOPIC("--list --zookeeper %s") {
        @Override
        protected void runCommand(String[] options) {
            TopicCommand.main(options);
        }
    },
    DESCRIBE_TOPIC("--describe --zookeeper %s --topic %s") {
        @Override
        protected void runCommand(String[] options) {
            TopicCommand.main(options);
        }
    },
    DELETE_TOPIC("--delete --zookeeper %s --topic %s") {
        @Override
        protected void runCommand(String[] options) {
            TopicCommand.main(options);
        } 
    },
    CREATE_TOPIC("--create --zookeeper %s --partitions %d --topic %s --replication-factor %d") {
        @Override
        protected void runCommand(String[] options) {
            TopicCommand.main(options);
        }         
    };
    
    protected abstract void runCommand(String[] options);
    
    public void exec(Object ...args) {
        String format = this.getFormat();
        String command = String.format(format, args);
        String []commandToArr = stringToArray(Constants.SPACE_STRING, command);
        this.runCommand(commandToArr);
    }
    
    private KfTopicCommand(String format) {
        this.format = format;
    }
    
    protected String getFormat() {
        return format;
    }
    
    /**
     * 将字符串以特定分割符分割成数组
     * @param delimeter
     * @param string
     * @return
     */
    protected String[] stringToArray(String delimeter, String string) {
        if (Strings.isNullOrEmpty(delimeter) || Strings.isNullOrEmpty(string)) {
            new IllegalArgumentException("delemeter or string must not be null or empty");
        }
        return Splitter.on(delimeter).omitEmptyStrings().splitToList(string).toArray(new String[0]);
    }
    
    private String format;
}
