package com.example.debs;

import com.example.debs.model.ClusteredHardware;
import com.example.debs.model.InputMessage;
import com.example.debs.model.TimeCentroid;
import com.example.debs.operators.InputMessageTimestampAssigner;
import com.example.debs.operators.MyProcessWindowFunction2;
import com.example.debs.operators.SumAggregator2;
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
    public static class MinMaxScaler {
        private double min;
        private double max;

        public MinMaxScaler(double min, double max) {
            this.min = min;
            this.max = max;
        }

        public double scale(double value) {
            return (value - min) / (max - min);
        }
    }

    public static class EuclideanDistance {
        public static double calculateDistance(InputMessage inputMessage1, InputMessage inputMessage2) {
            double distance = 0.0;
            distance += Math.pow(inputMessage1.getS1_read_error_rate() - inputMessage2.getS1_read_error_rate(), 2);
            distance += Math.pow(inputMessage1.getS2_throughput_performance() - inputMessage2.getS2_throughput_performance(), 2);
            distance += Math.pow(inputMessage1.getS3_spin_up_time() - inputMessage2.getS3_spin_up_time(), 2);
            distance += Math.pow(inputMessage1.getS4_start_stop_count() - inputMessage2.getS4_start_stop_count(), 2);
            distance += Math.pow(inputMessage1.getS5_reallocated_sector_count() - inputMessage2.getS5_reallocated_sector_count(), 2);
            distance += Math.pow(inputMessage1.getS7_seek_error_rate() - inputMessage2.getS7_seek_error_rate(), 2);
            distance += Math.pow(inputMessage1.getS8_seek_time_performance() - inputMessage2.getS8_seek_time_performance(), 2);
            distance += Math.pow(inputMessage1.getS9_power_on_hours() - inputMessage2.getS9_power_on_hours(), 2);
            distance += Math.pow(inputMessage1.getS10_spin_retry_count() - inputMessage2.getS10_spin_retry_count(), 2);
            distance += Math.pow(inputMessage1.getS12_power_cycle_count() - inputMessage2.getS12_power_cycle_count(), 2);
            distance += Math.pow(inputMessage1.getS173_wear_leveling_count() - inputMessage2.getS173_wear_leveling_count(), 2);
            distance += Math.pow(inputMessage1.getS174_unexpected_power_loss_count() - inputMessage2.getS174_unexpected_power_loss_count(), 2);
            distance += Math.pow(inputMessage1.getS183_sata_downshift_count() - inputMessage2.getS183_sata_downshift_count(), 2);
            distance += Math.pow(inputMessage1.getS187_reported_uncorrectable_errors() - inputMessage2.getS187_reported_uncorrectable_errors(), 2);
            distance += Math.pow(inputMessage1.getS188_command_timeout() - inputMessage2.getS188_command_timeout(), 2);
            distance += Math.pow(inputMessage1.getS189_high_fly_writes() - inputMessage2.getS189_high_fly_writes(), 2);
            distance += Math.pow(inputMessage1.getS190_airflow_temperature_cel() - inputMessage2.getS190_airflow_temperature_cel(), 2);
            distance += Math.pow(inputMessage1.getS191_g_sense_error_rate() - inputMessage2.getS191_g_sense_error_rate(), 2);
            distance += Math.pow(inputMessage1.getS192_power_off_retract_count() - inputMessage2.getS192_power_off_retract_count(), 2);
            distance += Math.pow(inputMessage1.getS193_load_unload_cycle_count() - inputMessage2.getS193_load_unload_cycle_count(), 2);
            distance += Math.pow(inputMessage1.getS194_temperature_celsius() - inputMessage2.getS194_temperature_celsius(), 2);
            distance += Math.pow(inputMessage1.getS195_hardware_ecc_recovered() - inputMessage2.getS195_hardware_ecc_recovered(), 2);
            distance += Math.pow(inputMessage1.getS196_reallocated_event_count() - inputMessage2.getS196_reallocated_event_count(), 2);
            distance += Math.pow(inputMessage1.getS197_current_pending_sector() - inputMessage2.getS197_current_pending_sector(), 2);
            distance += Math.pow(inputMessage1.getS198_offline_uncorrectable() - inputMessage2.getS198_offline_uncorrectable(), 2);
            distance += Math.pow(inputMessage1.getS199_udma_crc_error_count() - inputMessage2.getS199_udma_crc_error_count(), 2);
            distance += Math.pow(inputMessage1.getS200_multi_zone_error_rate() - inputMessage2.getS200_multi_zone_error_rate(), 2);
            distance += Math.pow(inputMessage1.getS220_disk_shift() - inputMessage2.getS220_disk_shift(), 2);
            distance += Math.pow(inputMessage1.getS222_loaded_hours() - inputMessage2.getS222_loaded_hours(), 2);
            distance += Math.pow(inputMessage1.getS223_load_retry_count() - inputMessage2.getS223_load_retry_count(), 2);
            distance += Math.pow(inputMessage1.getS226_load_in_time() - inputMessage2.getS226_load_in_time(), 2);
            distance += Math.pow(inputMessage1.getS240_head_flying_hours() - inputMessage2.getS240_head_flying_hours(), 2);
            distance += Math.pow(inputMessage1.getS241_total_lbas_written() - inputMessage2.getS241_total_lbas_written(), 2);
            distance += Math.pow(inputMessage1.getS242_total_lbas_read() - inputMessage2.getS242_total_lbas_read(), 2);
            return Math.sqrt(distance);
        }
    }

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
                    normalizedInputMessage.setS1_read_error_rate(new MinMaxScaler(minMaxValues[0][0], minMaxValues[0][1]).scale(inputMessage.getS1_read_error_rate()));
                    normalizedInputMessage.setS2_throughput_performance(new MinMaxScaler(minMaxValues[1][0], minMaxValues[1][1]).scale(inputMessage.getS2_throughput_performance()));
                    normalizedInputMessage.setS3_spin_up_time(new MinMaxScaler(minMaxValues[2][0], minMaxValues[2][1]).scale(inputMessage.getS3_spin_up_time()));
                    normalizedInputMessage.setS4_start_stop_count(new MinMaxScaler(minMaxValues[3][0], minMaxValues[3][1]).scale(inputMessage.getS4_start_stop_count()));
                    normalizedInputMessage.setS5_reallocated_sector_count(new MinMaxScaler(minMaxValues[4][0], minMaxValues[4][1]).scale(inputMessage.getS5_reallocated_sector_count()));
                    normalizedInputMessage.setS7_seek_error_rate(new MinMaxScaler(minMaxValues[5][0], minMaxValues[5][1]).scale(inputMessage.getS7_seek_error_rate()));
                    normalizedInputMessage.setS8_seek_time_performance(new MinMaxScaler(minMaxValues[6][0], minMaxValues[6][1]).scale(inputMessage.getS8_seek_time_performance()));
                    normalizedInputMessage.setS9_power_on_hours(new MinMaxScaler(minMaxValues[7][0], minMaxValues[7][1]).scale(inputMessage.getS9_power_on_hours()));
                    normalizedInputMessage.setS10_spin_retry_count(new MinMaxScaler(minMaxValues[8][0], minMaxValues[8][1]).scale(inputMessage.getS10_spin_retry_count()));
                    normalizedInputMessage.setS12_power_cycle_count(new MinMaxScaler(minMaxValues[9][0], minMaxValues[9][1]).scale(inputMessage.getS12_power_cycle_count()));
                    normalizedInputMessage.setS173_wear_leveling_count(new MinMaxScaler(minMaxValues[10][0], minMaxValues[10][1]).scale(inputMessage.getS173_wear_leveling_count()));
                    normalizedInputMessage.setS174_unexpected_power_loss_count(new MinMaxScaler(minMaxValues[11][0], minMaxValues[11][1]).scale(inputMessage.getS174_unexpected_power_loss_count()));
                    normalizedInputMessage.setS183_sata_downshift_count(new MinMaxScaler(minMaxValues[12][0], minMaxValues[12][1]).scale(inputMessage.getS183_sata_downshift_count()));
                    normalizedInputMessage.setS187_reported_uncorrectable_errors(new MinMaxScaler(minMaxValues[13][0], minMaxValues[13][1]).scale(inputMessage.getS187_reported_uncorrectable_errors()));
                    normalizedInputMessage.setS188_command_timeout(new MinMaxScaler(minMaxValues[14][0], minMaxValues[14][1]).scale(inputMessage.getS188_command_timeout()));
                    normalizedInputMessage.setS189_high_fly_writes(new MinMaxScaler(minMaxValues[15][0], minMaxValues[15][1]).scale(inputMessage.getS189_high_fly_writes()));
                    normalizedInputMessage.setS190_airflow_temperature_cel(new MinMaxScaler(minMaxValues[16][0], minMaxValues[16][1]).scale(inputMessage.getS190_airflow_temperature_cel()));
                    normalizedInputMessage.setS191_g_sense_error_rate(new MinMaxScaler(minMaxValues[17][0], minMaxValues[17][1]).scale(inputMessage.getS191_g_sense_error_rate()));
                    normalizedInputMessage.setS192_power_off_retract_count(new MinMaxScaler(minMaxValues[18][0], minMaxValues[18][1]).scale(inputMessage.getS192_power_off_retract_count()));
                    normalizedInputMessage.setS193_load_unload_cycle_count(new MinMaxScaler(minMaxValues[19][0], minMaxValues[19][1]).scale(inputMessage.getS193_load_unload_cycle_count()));
                    normalizedInputMessage.setS194_temperature_celsius(new MinMaxScaler(minMaxValues[20][0], minMaxValues[20][1]).scale(inputMessage.getS194_temperature_celsius()));
                    normalizedInputMessage.setS195_hardware_ecc_recovered(new MinMaxScaler(minMaxValues[21][0], minMaxValues[21][1]).scale(inputMessage.getS195_hardware_ecc_recovered()));
                    normalizedInputMessage.setS196_reallocated_event_count(new MinMaxScaler(minMaxValues[22][0], minMaxValues[22][1]).scale(inputMessage.getS196_reallocated_event_count()));
                    normalizedInputMessage.setS197_current_pending_sector(new MinMaxScaler(minMaxValues[23][0], minMaxValues[23][1]).scale(inputMessage.getS197_current_pending_sector()));
                    normalizedInputMessage.setS198_offline_uncorrectable(new MinMaxScaler(minMaxValues[24][0], minMaxValues[24][1]).scale(inputMessage.getS198_offline_uncorrectable()));
                    normalizedInputMessage.setS199_udma_crc_error_count(new MinMaxScaler(minMaxValues[25][0], minMaxValues[25][1]).scale(inputMessage.getS199_udma_crc_error_count()));
                    normalizedInputMessage.setS200_multi_zone_error_rate(new MinMaxScaler(minMaxValues[26][0], minMaxValues[26][1]).scale(inputMessage.getS200_multi_zone_error_rate()));
                    normalizedInputMessage.setS220_disk_shift(new MinMaxScaler(minMaxValues[27][0], minMaxValues[27][1]).scale(inputMessage.getS220_disk_shift()));
                    normalizedInputMessage.setS222_loaded_hours(new MinMaxScaler(minMaxValues[28][0], minMaxValues[28][1]).scale(inputMessage.getS222_loaded_hours()));
                    normalizedInputMessage.setS223_load_retry_count(new MinMaxScaler(minMaxValues[29][0], minMaxValues[29][1]).scale(inputMessage.getS223_load_retry_count()));
                    normalizedInputMessage.setS226_load_in_time(new MinMaxScaler(minMaxValues[30][0], minMaxValues[30][1]).scale(inputMessage.getS226_load_in_time()));
                    normalizedInputMessage.setS240_head_flying_hours(new MinMaxScaler(minMaxValues[31][0], minMaxValues[31][1]).scale(inputMessage.getS240_head_flying_hours()));
                    normalizedInputMessage.setS241_total_lbas_written(new MinMaxScaler(minMaxValues[32][0], minMaxValues[32][1]).scale(inputMessage.getS241_total_lbas_written()));
                    normalizedInputMessage.setS242_total_lbas_read(new MinMaxScaler(minMaxValues[33][0], minMaxValues[33][1]).scale(inputMessage.getS242_total_lbas_read()));
                    return normalizedInputMessage;
                });

        // Read the centroids, we have 50 clusters and 34 features
        // skip the first line (header)
        pathFile = "src/main/resources/clusters.csv";
        InputMessage[] centroids = new InputMessage[50];

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
                InputMessage centroid = new InputMessage();
                centroid.setS1_read_error_rate(new MinMaxScaler(minMaxValues[0][0], minMaxValues[0][1]).scale(Double.valueOf(values[1])));
                centroid.setS2_throughput_performance(new MinMaxScaler(minMaxValues[1][0], minMaxValues[1][1]).scale(Double.valueOf(values[2])));
                centroid.setS3_spin_up_time(new MinMaxScaler(minMaxValues[2][0], minMaxValues[2][1]).scale(Double.valueOf(values[3])));
                centroid.setS4_start_stop_count(new MinMaxScaler(minMaxValues[3][0], minMaxValues[3][1]).scale(Double.valueOf(values[4])));
                centroid.setS5_reallocated_sector_count(new MinMaxScaler(minMaxValues[4][0], minMaxValues[4][1]).scale(Double.valueOf(values[5])));
                centroid.setS7_seek_error_rate(new MinMaxScaler(minMaxValues[5][0], minMaxValues[5][1]).scale(Double.valueOf(values[6])));
                centroid.setS8_seek_time_performance(new MinMaxScaler(minMaxValues[6][0], minMaxValues[6][1]).scale(Double.valueOf(values[7])));
                centroid.setS9_power_on_hours(new MinMaxScaler(minMaxValues[7][0], minMaxValues[7][1]).scale(Double.valueOf(values[8])));
                centroid.setS10_spin_retry_count(new MinMaxScaler(minMaxValues[8][0], minMaxValues[8][1]).scale(Double.valueOf(values[9])));
                centroid.setS12_power_cycle_count(new MinMaxScaler(minMaxValues[9][0], minMaxValues[9][1]).scale(Double.valueOf(values[10])));
                centroid.setS173_wear_leveling_count(new MinMaxScaler(minMaxValues[10][0], minMaxValues[10][1]).scale(Double.valueOf(values[11])));
                centroid.setS174_unexpected_power_loss_count(new MinMaxScaler(minMaxValues[11][0], minMaxValues[11][1]).scale(Double.valueOf(values[12])));
                centroid.setS183_sata_downshift_count(new MinMaxScaler(minMaxValues[12][0], minMaxValues[12][1]).scale(Double.valueOf(values[13])));
                centroid.setS187_reported_uncorrectable_errors(new MinMaxScaler(minMaxValues[13][0], minMaxValues[13][1]).scale(Double.valueOf(values[14])));
                centroid.setS188_command_timeout(new MinMaxScaler(minMaxValues[14][0], minMaxValues[14][1]).scale(Double.valueOf(values[15])));
                centroid.setS189_high_fly_writes(new MinMaxScaler(minMaxValues[15][0], minMaxValues[15][1]).scale(Double.valueOf(values[16])));
                centroid.setS190_airflow_temperature_cel(new MinMaxScaler(minMaxValues[16][0], minMaxValues[16][1]).scale(Double.valueOf(values[17])));
                centroid.setS191_g_sense_error_rate(new MinMaxScaler(minMaxValues[17][0], minMaxValues[17][1]).scale(Double.valueOf(values[18])));
                centroid.setS192_power_off_retract_count(new MinMaxScaler(minMaxValues[18][0], minMaxValues[18][1]).scale(Double.valueOf(values[19])));
                centroid.setS193_load_unload_cycle_count(new MinMaxScaler(minMaxValues[19][0], minMaxValues[19][1]).scale(Double.valueOf(values[20])));
                centroid.setS194_temperature_celsius(new MinMaxScaler(minMaxValues[20][0], minMaxValues[20][1]).scale(Double.valueOf(values[21])));
                centroid.setS195_hardware_ecc_recovered(new MinMaxScaler(minMaxValues[21][0], minMaxValues[21][1]).scale(Double.valueOf(values[22])));
                centroid.setS196_reallocated_event_count(new MinMaxScaler(minMaxValues[22][0], minMaxValues[22][1]).scale(Double.valueOf(values[23])));
                centroid.setS197_current_pending_sector(new MinMaxScaler(minMaxValues[23][0], minMaxValues[23][1]).scale(Double.valueOf(values[24])));
                centroid.setS198_offline_uncorrectable(new MinMaxScaler(minMaxValues[24][0], minMaxValues[24][1]).scale(Double.valueOf(values[25])));
                centroid.setS199_udma_crc_error_count(new MinMaxScaler(minMaxValues[25][0], minMaxValues[25][1]).scale(Double.valueOf(values[26])));
                centroid.setS200_multi_zone_error_rate(new MinMaxScaler(minMaxValues[26][0], minMaxValues[26][1]).scale(Double.valueOf(values[27])));
                centroid.setS220_disk_shift(new MinMaxScaler(minMaxValues[27][0], minMaxValues[27][1]).scale(Double.valueOf(values[28])));
                centroid.setS222_loaded_hours(new MinMaxScaler(minMaxValues[28][0], minMaxValues[28][1]).scale(Double.valueOf(values[29])));
                centroid.setS223_load_retry_count(new MinMaxScaler(minMaxValues[29][0], minMaxValues[29][1]).scale(Double.valueOf(values[30])));
                centroid.setS226_load_in_time(new MinMaxScaler(minMaxValues[30][0], minMaxValues[30][1]).scale(Double.valueOf(values[31])));
                centroid.setS240_head_flying_hours(new MinMaxScaler(minMaxValues[31][0], minMaxValues[31][1]).scale(Double.valueOf(values[32])));
                centroid.setS241_total_lbas_written(new MinMaxScaler(minMaxValues[32][0], minMaxValues[32][1]).scale(Double.valueOf(values[33])));
                centroid.setS242_total_lbas_read(new MinMaxScaler(minMaxValues[33][0], minMaxValues[33][1]).scale(Double.valueOf(values[34])));
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
                    for (int i = 0; i < 50; i++) {
                        double distance = EuclideanDistance.calculateDistance(inputMessage, centroids[i]);
                        if (distance < minDistance) {
                            minDistance = distance;
                            cluster = i;
                        }
                    }
                    ClusteredHardware clusteredHardware = new ClusteredHardware();
                    clusteredHardware.setDate(inputMessage.getDate());
                    clusteredHardware.setSerial_number(inputMessage.getSerial_number());
                    clusteredHardware.setModel(inputMessage.getModel());
                    clusteredHardware.setFailure(inputMessage.getFailure());
                    clusteredHardware.setVaultId(inputMessage.getVaultId());
                    clusteredHardware.setS1_read_error_rate(inputMessage.getS1_read_error_rate());
                    clusteredHardware.setS2_throughput_performance(inputMessage.getS2_throughput_performance());
                    clusteredHardware.setS3_spin_up_time(inputMessage.getS3_spin_up_time());
                    clusteredHardware.setS4_start_stop_count(inputMessage.getS4_start_stop_count());
                    clusteredHardware.setS5_reallocated_sector_count(inputMessage.getS5_reallocated_sector_count());
                    clusteredHardware.setS7_seek_error_rate(inputMessage.getS7_seek_error_rate());
                    clusteredHardware.setS8_seek_time_performance(inputMessage.getS8_seek_time_performance());
                    clusteredHardware.setS9_power_on_hours(inputMessage.getS9_power_on_hours());
                    clusteredHardware.setS10_spin_retry_count(inputMessage.getS10_spin_retry_count());
                    clusteredHardware.setS12_power_cycle_count(inputMessage.getS12_power_cycle_count());
                    clusteredHardware.setS173_wear_leveling_count(inputMessage.getS173_wear_leveling_count());
                    clusteredHardware.setS174_unexpected_power_loss_count(inputMessage.getS174_unexpected_power_loss_count());
                    clusteredHardware.setS183_sata_downshift_count(inputMessage.getS183_sata_downshift_count());
                    clusteredHardware.setS187_reported_uncorrectable_errors(inputMessage.getS187_reported_uncorrectable_errors());
                    clusteredHardware.setS188_command_timeout(inputMessage.getS188_command_timeout());
                    clusteredHardware.setS189_high_fly_writes(inputMessage.getS189_high_fly_writes());
                    clusteredHardware.setS190_airflow_temperature_cel(inputMessage.getS190_airflow_temperature_cel());
                    clusteredHardware.setS191_g_sense_error_rate(inputMessage.getS191_g_sense_error_rate());
                    clusteredHardware.setS192_power_off_retract_count(inputMessage.getS192_power_off_retract_count());
                    clusteredHardware.setS193_load_unload_cycle_count(inputMessage.getS193_load_unload_cycle_count());
                    clusteredHardware.setS194_temperature_celsius(inputMessage.getS194_temperature_celsius());
                    clusteredHardware.setS195_hardware_ecc_recovered(inputMessage.getS195_hardware_ecc_recovered());
                    clusteredHardware.setS196_reallocated_event_count(inputMessage.getS196_reallocated_event_count());
                    clusteredHardware.setS197_current_pending_sector(inputMessage.getS197_current_pending_sector());
                    clusteredHardware.setS198_offline_uncorrectable(inputMessage.getS198_offline_uncorrectable());
                    clusteredHardware.setS199_udma_crc_error_count(inputMessage.getS199_udma_crc_error_count());
                    clusteredHardware.setS200_multi_zone_error_rate(inputMessage.getS200_multi_zone_error_rate());
                    clusteredHardware.setS220_disk_shift(inputMessage.getS220_disk_shift());
                    clusteredHardware.setS222_loaded_hours(inputMessage.getS222_loaded_hours());
                    clusteredHardware.setS223_load_retry_count(inputMessage.getS223_load_retry_count());
                    clusteredHardware.setS226_load_in_time(inputMessage.getS226_load_in_time());
                    clusteredHardware.setS240_head_flying_hours(inputMessage.getS240_head_flying_hours());
                    clusteredHardware.setS241_total_lbas_written(inputMessage.getS241_total_lbas_written());
                    clusteredHardware.setS242_total_lbas_read(inputMessage.getS242_total_lbas_read());
                    clusteredHardware.setClusterId(cluster);
                    return clusteredHardware;
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
