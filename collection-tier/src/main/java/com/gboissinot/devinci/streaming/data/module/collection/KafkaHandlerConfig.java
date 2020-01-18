package com.gboissinot.devinci.streaming.data.module.collection;

import java.util.List;

class KafkaHandlerConfig {

    private String topicName;

    private List<String> bootstrapServers;

    String getTopicName() {
        return topicName;
    }

    void setTopicName(String topicName) {
        this.topicName = topicName;
    }

    List<String> getBootstrapServers() {
        return bootstrapServers;
    }

    void setBootstrapServers(List<String> bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }
}
