package com.example.debs;

import com.example.debs.model.*;
import com.example.debs.operators.InputMessageTimestampAssigner;
import com.example.debs.operators.MyProcessWindowFunction2;
import com.example.debs.operators.SumAggregator2;
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
                Smart smart = new Smart();
                smart.setS1_read_error_rate(Double.valueOf(values[1]));
                smart.setS2_throughput_performance(Double.valueOf(values[2]));
                smart.setS3_spin_up_time(Double.valueOf(values[3]));
                smart.setS4_start_stop_count(Double.valueOf(values[4]));
                smart.setS5_reallocated_sector_count(Double.valueOf(values[5]));
                smart.setS7_seek_error_rate(Double.valueOf(values[6]));
                smart.setS8_seek_time_performance(Double.valueOf(values[7]));
                smart.setS9_power_on_hours(Double.valueOf(values[8]));
                smart.setS10_spin_retry_count(Double.valueOf(values[9]));
                smart.setS12_power_cycle_count(Double.valueOf(values[10]));
                smart.setS173_wear_leveling_count(Double.valueOf(values[11]));
                smart.setS174_unexpected_power_loss_count(Double.valueOf(values[12]));
                smart.setS183_sata_downshift_count(Double.valueOf(values[13]));
                smart.setS187_reported_uncorrectable_errors(Double.valueOf(values[14]));
                smart.setS188_command_timeout(Double.valueOf(values[15]));
                smart.setS189_high_fly_writes(Double.valueOf(values[16]));
                smart.setS190_airflow_temperature_cel(Double.valueOf(values[17]));
                smart.setS191_g_sense_error_rate(Double.valueOf(values[18]));
                smart.setS192_power_off_retract_count(Double.valueOf(values[19]));
                smart.setS193_load_unload_cycle_count(Double.valueOf(values[20]));
                smart.setS194_temperature_celsius(Double.valueOf(values[21]));
                smart.setS195_hardware_ecc_recovered(Double.valueOf(values[22]));
                smart.setS196_reallocated_event_count(Double.valueOf(values[23]));
                smart.setS197_current_pending_sector(Double.valueOf(values[24]));
                smart.setS198_offline_uncorrectable(Double.valueOf(values[25]));
                smart.setS199_udma_crc_error_count(Double.valueOf(values[26]));
                smart.setS200_multi_zone_error_rate(Double.valueOf(values[27]));
                smart.setS220_disk_shift(Double.valueOf(values[28]));
                smart.setS222_loaded_hours(Double.valueOf(values[29]));
                smart.setS223_load_retry_count(Double.valueOf(values[30]));
                smart.setS226_load_in_time(Double.valueOf(values[31]));
                smart.setS240_head_flying_hours(Double.valueOf(values[32]));
                smart.setS241_total_lbas_written(Double.valueOf(values[33]));
                smart.setS242_total_lbas_read(Double.valueOf(values[34]));
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
                        double distance = inputMessageSmart.euclideanDistance(centroidSmart);
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
                .aggregate(new SumAggregator2(), new MyProcessWindowFunction2());


        // Print the result
        timeCentroidDataStream.addSink(printSinkFunction);

        // execute the pipeline and return the result
        environment.execute("Q2 Debs Flink Data Stream");
    }
}
