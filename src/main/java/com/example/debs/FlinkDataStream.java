package com.example.debs;

import com.example.debs.operators.CountAggregator;
import com.example.debs.operators.MyProcessWindowFunction;
import com.example.debs.schema.InputMessageDeserializerSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.example.debs.model.InputMessage;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.time.Duration;

import static com.example.debs.connectors.Consumer.createInputMessageConsumer;

public class FlinkDataStream {

    public static void main(String[] args) throws Exception {
        String topic = "debs-topic";
        String consumerGroup = "debs-consumer-group";
        String kafkaAddress = "localhost:9092";
        PrintSinkFunction<Tuple3<Long, Long, Long>> printSinkFunction = new PrintSinkFunction<>();

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        // create a kafka source
        KafkaSource<InputMessage> kafkaSource = KafkaSource.<InputMessage>builder()
                .setBootstrapServers(kafkaAddress)
                .setGroupId(consumerGroup)
                .setTopics(topic)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new InputMessageDeserializerSchema())
                .build();

        // create a stream from the source
        // add a watermark to the stream
        DataStream<InputMessage> inputMessageDataStream = environment.fromSource(
                kafkaSource,
                WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(20)),
                "Kafka Source"
        );

        // count a number of failures per vaultId
        // sliding window size is 30 days sliding every 1 day
        // print the result to the console
        DataStream<InputMessage> failures = inputMessageDataStream.filter(InputMessage::getIsFailure);

        DataStream<Tuple3<Long, Long, Long>> failuresPerVaultId = failures
                .keyBy(InputMessage::getVaultId)
                .window(TumblingEventTimeWindows.of(Time.days(30)))
                .process(new MyProcessWindowFunction());

        failuresPerVaultId.print();

        // execute the pipeline and return the result
        environment.execute("Debs Flink Data Stream");
    }
}
