package com.example.debs;

import com.example.debs.schema.InputMessageDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.example.debs.model.InputMessage;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;

public class FlinkDataStream {

    public static void main(String[] args) throws Exception {
        String topic = "debs-topic";
        String consumerGroup = "debs-consumer-group";
        String kafkaAddress = "localhost:9092";
        PrintSinkFunction<InputMessage> sink = new PrintSinkFunction<>();

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<InputMessage> kafkaSource = KafkaSource.<InputMessage>builder()
                .setBootstrapServers(kafkaAddress)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new InputMessageDeserializer())
                .build();

        // create a stream from the source
        DataStream<InputMessage> inputMessageDataStream = environment.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");

        inputMessageDataStream.addSink(sink);

        // execute the pipeline and return the result
        environment.execute("Debs Flink Data Stream");
    }
}
