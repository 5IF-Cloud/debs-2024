package com.example.debs.connectors;

import com.example.debs.model.InputMessage;
import com.example.debs.schema.InputMessageDeserializerSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class Consumer {
    public static FlinkKafkaConsumer<InputMessage> createInputMessageConsumer(String topic, String kafkaAddress, String kafkaGroup) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaAddress);
        properties.setProperty("group.id", kafkaGroup);
        FlinkKafkaConsumer<InputMessage> consumer = new FlinkKafkaConsumer<>(topic, new InputMessageDeserializerSchema(), properties);
        return consumer;
    }
}
