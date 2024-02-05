package com.example.debs;

import com.example.debs.model.InputMessage;
import com.example.debs.model.NFByVault;
import com.example.debs.operators.InputMessageTimestampAssigner;
import com.example.debs.operators.MyProcessWindowFunction;
import com.example.debs.operators.SumAggregator;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import static com.example.debs.connectors.Consumer.createInputMessageConsumer;

public class Q1DataStream {
    public static void main(String[] args) throws Exception {
        String topic = "debs-topic";
        String consumerGroup = "debs-consumer-group";
        String kafkaAddress = "localhost:29092";
        PrintSinkFunction<NFByVault> printSinkFunction = new PrintSinkFunction<>();

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // create a kafka source
        FlinkKafkaConsumer<InputMessage> kafkaSource = createInputMessageConsumer(topic, kafkaAddress, consumerGroup);
        kafkaSource.setStartFromEarliest();

        // assign timestamps and watermarks
        kafkaSource.assignTimestampsAndWatermarks(new InputMessageTimestampAssigner());

        DataStream<InputMessage> inputMessageDataStream = environment.addSource(kafkaSource);

        // count a number of failures per vaultId
        // sliding window size is 30 days sliding every 1 day
        // print the result to the console
        // Apply the SumAggregator to the input stream

        DataStream<NFByVault> failuresPerVaultId = inputMessageDataStream
                .keyBy(InputMessage::getVaultId)
                .window(SlidingEventTimeWindows.of(Time.days(30), Time.days(1)))
                .aggregate(new SumAggregator(), new MyProcessWindowFunction());

        failuresPerVaultId.addSink(printSinkFunction);

        // execute the pipeline and return the result
        environment.execute("Q1 Debs Flink Data Stream");
    }
}
