package com.example.debs;

import com.example.debs.operators.CountAggregator;
import com.example.debs.operators.MyProcessWindowFunction;
import com.example.debs.schema.InputMessageDeserializer;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import com.example.debs.model.InputMessage;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.time.ZoneOffset;

public class FlinkDataStream {

    public static void main(String[] args) throws Exception {
        String topic = "debs-topic";
        String consumerGroup = "debs-consumer-group";
        String kafkaAddress = "localhost:9092";
        PrintSinkFunction<Tuple3<Long, Long, Long>> sink = new PrintSinkFunction<>();

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<InputMessage> kafkaSource = KafkaSource.<InputMessage>builder()
                .setBootstrapServers(kafkaAddress)
                .setTopics(topic)
                .setGroupId(consumerGroup)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new InputMessageDeserializer())
                .build();

        // create a stream from the source
        // add a watermark to the stream
        DataStream<InputMessage> inputMessageDataStream = environment.fromSource(kafkaSource,
                WatermarkStrategy
                    .<InputMessage>forMonotonousTimestamps()
                    .withTimestampAssigner((message, date) -> message.getDate().toEpochSecond(ZoneOffset.UTC)),
                "Kafka Source");

        // count a number of failures per vaultId
        // sliding window size is 30 days sliding every 1 day
        // print the result to the console
        DataStream<InputMessage> failures = inputMessageDataStream.filter(InputMessage::getIsFailure);

        DataStream<Tuple3<Long, Long, Long>> failuresPerVaultId = failures
                .keyBy(InputMessage::getVaultId)
                .window(SlidingEventTimeWindows.of(Time.days(30), Time.days(1)))
                .aggregate(new CountAggregator(), new MyProcessWindowFunction());

        failuresPerVaultId.addSink(sink);

        // execute the pipeline and return the result
        environment.execute("Debs Flink Data Stream");
    }
}
