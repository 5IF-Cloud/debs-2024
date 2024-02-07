package com.example.debs;

import com.example.debs.model.*;
import com.example.debs.operators.InputMessageTimestampAssigner;
import com.example.debs.operators.MyProcessWindowFunction2;
import com.example.debs.operators.AverageAggregator;
import com.example.debs.scaler.MinMaxScaler;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.BufferedReader;
import java.io.FileReader;

import static com.example.debs.connectors.Consumer.createInputMessageConsumer;

public class Q2DataStream {

    public static void main(String[] args) throws Exception {
        String topic = "debs-topic";
        String consumerGroup = "debs-consumer-group";
        String kafkaAddress = "localhost:29092";
        PrintSinkFunction<TimeCentroid> printSinkFunction = new PrintSinkFunction<>();

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // create a kafka source
        FlinkKafkaConsumer<InputMessage> kafkaSource = createInputMessageConsumer(topic, kafkaAddress, consumerGroup);
        kafkaSource.setStartFromEarliest();

        // assign timestamps and watermarks
        kafkaSource.assignTimestampsAndWatermarks(new InputMessageTimestampAssigner());

        DataStream<InputMessage> inputMessageDataStream = environment.addSource(kafkaSource);

        // Read the norm.csv file and create a map of MinMaxScaler objects
        // skip the first line (header)
        String pathFile = "src/main/resources/norm.csv";
        Double[][] minMaxValues = new Double[34][2];
        try (BufferedReader br = new BufferedReader(new FileReader(pathFile))) {
            String line;
            int i = 0;
            while ((line = br.readLine()) != null) {
                if (i == 0) {
                    i++;
                    continue;
                }
                String[] values = line.split(",");
                minMaxValues[i-1][0] = Double.valueOf(values[1]);
                minMaxValues[i-1][1] = Double.valueOf(values[2]);
                i++;
            }
        }

        // Normalize the input data

        DataStream<InputMessage> normalizeMessage = inputMessageDataStream
                .map(inputMessage -> {
                    InputMessage normalizedInputMessage = new InputMessage();
                    normalizedInputMessage.setDate(inputMessage.getDate());
                    normalizedInputMessage.setSerial_number(inputMessage.getSerial_number());
                    normalizedInputMessage.setModel(inputMessage.getModel());
                    normalizedInputMessage.setFailure(inputMessage.getFailure());
                    normalizedInputMessage.setVaultId(inputMessage.getVaultId());
                    Smart smart = MinMaxScaler.scale(inputMessage.getSmart(), minMaxValues);
                    normalizedInputMessage.setSmart(smart);
                    return normalizedInputMessage;
                });

        // Read the centroids, we have 50 clusters and 34 features
        // skip the first line (header)
        pathFile = "src/main/resources/clusters.csv";
        Centroid[] centroids = new Centroid[50];

        try (BufferedReader br = new BufferedReader(new FileReader(pathFile))) {
            String line;
            int i = 0;
            while ((line = br.readLine()) != null) {
                if (i == 0) {
                    i++;
                    continue;
                }
                // Normalize the centroids
                String[] values = line.split(",");
                Centroid centroid = new Centroid();
                centroid.setClusterId(Long.valueOf(values[0]));
                double[] valCentroid = new double[34];
                for (int j = 1; j < 35; j++) {
                    valCentroid[j-1] = Double.parseDouble(values[j]);
                }
                Smart smart = new Smart(valCentroid);
                Smart scaledSmart = MinMaxScaler.scale(smart, minMaxValues);
                centroid.setSmart(scaledSmart);
                centroids[i-1] = centroid;
                i++;
            }
        }

        // Calculate the distance between the input data and the centroids
        // and find the closest centroid

        DataStream<ClusteredHardware> clusteredHardwareDataStream = normalizeMessage
                .map(inputMessage -> {
                    double minDistance = Double.MAX_VALUE;
                    long cluster = -1;
                    Smart inputMessageSmart = inputMessage.getSmart();
                    for (int i = 0; i < 50; i++) {
                        Smart centroidSmart = centroids[i].getSmart();
                        double distance = inputMessageSmart.infinityNorm(centroidSmart);
                        if (distance < minDistance) {
                            minDistance = distance;
                            cluster = i;
                        }
                    }
                    ClusteredHardware clusterHardware = new ClusteredHardware();
                    clusterHardware.setDate(inputMessage.getDate());
                    clusterHardware.setSerial_number(inputMessage.getSerial_number());
                    clusterHardware.setModel(inputMessage.getModel());
                    clusterHardware.setFailure(inputMessage.getFailure());
                    clusterHardware.setVaultId(inputMessage.getVaultId());
                    clusterHardware.setSmart(inputMessage.getSmart());
                    clusterHardware.setClusterId(cluster);
                    return clusterHardware;
                });

        DataStream<TimeCentroid> timeCentroidDataStream = clusteredHardwareDataStream
                .keyBy(ClusteredHardware::getClusterId)
                .window(SlidingEventTimeWindows.of(Time.days(30), Time.days(1)))
                .aggregate(new AverageAggregator(), new MyProcessWindowFunction2());


        // Print the result
        timeCentroidDataStream.addSink(printSinkFunction);

        // execute the pipeline and return the result
        environment.execute("Q2 Debs Flink Data Stream");
    }
}
