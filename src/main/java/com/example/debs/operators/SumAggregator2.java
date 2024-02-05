package com.example.debs.operators;

import com.example.debs.model.Centroid;
import com.example.debs.model.ClusteredHardware;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class SumAggregator2 implements AggregateFunction<ClusteredHardware, Centroid, Centroid> {
    @Override
    public Centroid createAccumulator() {
        Centroid centroid = new Centroid();
        centroid.setClusterId(-1L);
        centroid.setS1_read_error_rate(0.0);
        centroid.setS2_throughput_performance(0.0);
        centroid.setS3_spin_up_time(0.0);
        centroid.setS4_start_stop_count(0.0);
        centroid.setS5_reallocated_sector_count(0.0);
        centroid.setS7_seek_error_rate(0.0);
        centroid.setS8_seek_time_performance(0.0);
        centroid.setS9_power_on_hours(0.0);
        centroid.setS10_spin_retry_count(0.0);
        centroid.setS12_power_cycle_count(0.0);
        centroid.setS173_wear_leveling_count(0.0);
        centroid.setS174_unexpected_power_loss_count(0.0);
        centroid.setS183_sata_downshift_count(0.0);
        centroid.setS187_reported_uncorrectable_errors(0.0);
        centroid.setS188_command_timeout(0.0);
        centroid.setS189_high_fly_writes(0.0);
        centroid.setS190_airflow_temperature_cel(0.0);
        centroid.setS191_g_sense_error_rate(0.0);
        centroid.setS192_power_off_retract_count(0.0);
        centroid.setS193_load_unload_cycle_count(0.0);
        centroid.setS194_temperature_celsius(0.0);
        centroid.setS195_hardware_ecc_recovered(0.0);
        centroid.setS196_reallocated_event_count(0.0);
        centroid.setS197_current_pending_sector(0.0);
        centroid.setS198_offline_uncorrectable(0.0);
        centroid.setS199_udma_crc_error_count(0.0);
        centroid.setS200_multi_zone_error_rate(0.0);
        centroid.setS220_disk_shift(0.0);
        centroid.setS222_loaded_hours(0.0);
        centroid.setS223_load_retry_count(0.0);
        centroid.setS226_load_in_time(0.0);
        centroid.setS240_head_flying_hours(0.0);
        centroid.setS241_total_lbas_written(0.0);
        centroid.setS242_total_lbas_read(0.0);
        return centroid;
    }

    @Override
    public Centroid add(ClusteredHardware clusteredHardware, Centroid accumulator) {
        Centroid centroid = new Centroid();
        centroid.setClusterId(clusteredHardware.getClusterId());
        centroid.setS1_read_error_rate(clusteredHardware.getS1_read_error_rate() + accumulator.getS1_read_error_rate());
        centroid.setS2_throughput_performance(clusteredHardware.getS2_throughput_performance() + accumulator.getS2_throughput_performance());
        centroid.setS3_spin_up_time(clusteredHardware.getS3_spin_up_time() + accumulator.getS3_spin_up_time());
        centroid.setS4_start_stop_count(clusteredHardware.getS4_start_stop_count() + accumulator.getS4_start_stop_count());
        centroid.setS5_reallocated_sector_count(clusteredHardware.getS5_reallocated_sector_count() + accumulator.getS5_reallocated_sector_count());
        centroid.setS7_seek_error_rate(clusteredHardware.getS7_seek_error_rate() + accumulator.getS7_seek_error_rate());
        centroid.setS8_seek_time_performance(clusteredHardware.getS8_seek_time_performance() + accumulator.getS8_seek_time_performance());
        centroid.setS9_power_on_hours(clusteredHardware.getS9_power_on_hours() + accumulator.getS9_power_on_hours());
        centroid.setS10_spin_retry_count(clusteredHardware.getS10_spin_retry_count() + accumulator.getS10_spin_retry_count());
        centroid.setS12_power_cycle_count(clusteredHardware.getS12_power_cycle_count() + accumulator.getS12_power_cycle_count());
        centroid.setS173_wear_leveling_count(clusteredHardware.getS173_wear_leveling_count() + accumulator.getS173_wear_leveling_count());
        centroid.setS174_unexpected_power_loss_count(clusteredHardware.getS174_unexpected_power_loss_count() + accumulator.getS174_unexpected_power_loss_count());
        centroid.setS183_sata_downshift_count(clusteredHardware.getS183_sata_downshift_count() + accumulator.getS183_sata_downshift_count());
        centroid.setS187_reported_uncorrectable_errors(clusteredHardware.getS187_reported_uncorrectable_errors() + accumulator.getS187_reported_uncorrectable_errors());
        centroid.setS188_command_timeout(clusteredHardware.getS188_command_timeout() + accumulator.getS188_command_timeout());
        centroid.setS189_high_fly_writes(clusteredHardware.getS189_high_fly_writes() + accumulator.getS189_high_fly_writes());
        centroid.setS190_airflow_temperature_cel(clusteredHardware.getS190_airflow_temperature_cel() + accumulator.getS190_airflow_temperature_cel());
        centroid.setS191_g_sense_error_rate(clusteredHardware.getS191_g_sense_error_rate() + accumulator.getS191_g_sense_error_rate());
        centroid.setS192_power_off_retract_count(clusteredHardware.getS192_power_off_retract_count() + accumulator.getS192_power_off_retract_count());
        centroid.setS193_load_unload_cycle_count(clusteredHardware.getS193_load_unload_cycle_count() + accumulator.getS193_load_unload_cycle_count());
        centroid.setS194_temperature_celsius(clusteredHardware.getS194_temperature_celsius() + accumulator.getS194_temperature_celsius());
        centroid.setS195_hardware_ecc_recovered(clusteredHardware.getS195_hardware_ecc_recovered() + accumulator.getS195_hardware_ecc_recovered());
        centroid.setS196_reallocated_event_count(clusteredHardware.getS196_reallocated_event_count() + accumulator.getS196_reallocated_event_count());
        centroid.setS197_current_pending_sector(clusteredHardware.getS197_current_pending_sector() + accumulator.getS197_current_pending_sector());
        centroid.setS198_offline_uncorrectable(clusteredHardware.getS198_offline_uncorrectable() + accumulator.getS198_offline_uncorrectable());
        centroid.setS199_udma_crc_error_count(clusteredHardware.getS199_udma_crc_error_count() + accumulator.getS199_udma_crc_error_count());
        centroid.setS200_multi_zone_error_rate(clusteredHardware.getS200_multi_zone_error_rate() + accumulator.getS200_multi_zone_error_rate());
        centroid.setS220_disk_shift(clusteredHardware.getS220_disk_shift() + accumulator.getS220_disk_shift());
        centroid.setS222_loaded_hours(clusteredHardware.getS222_loaded_hours() + accumulator.getS222_loaded_hours());
        centroid.setS223_load_retry_count(clusteredHardware.getS223_load_retry_count() + accumulator.getS223_load_retry_count());
        centroid.setS226_load_in_time(clusteredHardware.getS226_load_in_time() + accumulator.getS226_load_in_time());
        centroid.setS240_head_flying_hours(clusteredHardware.getS240_head_flying_hours() + accumulator.getS240_head_flying_hours());
        centroid.setS241_total_lbas_written(clusteredHardware.getS241_total_lbas_written() + accumulator.getS241_total_lbas_written());
        centroid.setS242_total_lbas_read(clusteredHardware.getS242_total_lbas_read() + accumulator.getS242_total_lbas_read());
        return centroid;
    }

    @Override
    public Centroid getResult(Centroid accumulator) {
        return accumulator;
    }

    @Override
    public Centroid merge(Centroid accumulator1, Centroid accumulator2) {
        Centroid centroid = new Centroid();
        centroid.setClusterId(accumulator1.getClusterId());
        centroid.setS1_read_error_rate(accumulator1.getS1_read_error_rate() + accumulator2.getS1_read_error_rate());
        centroid.setS2_throughput_performance(accumulator1.getS2_throughput_performance() + accumulator2.getS2_throughput_performance());
        centroid.setS3_spin_up_time(accumulator1.getS3_spin_up_time() + accumulator2.getS3_spin_up_time());
        centroid.setS4_start_stop_count(accumulator1.getS4_start_stop_count() + accumulator2.getS4_start_stop_count());
        centroid.setS5_reallocated_sector_count(accumulator1.getS5_reallocated_sector_count() + accumulator2.getS5_reallocated_sector_count());
        centroid.setS7_seek_error_rate(accumulator1.getS7_seek_error_rate() + accumulator2.getS7_seek_error_rate());
        centroid.setS8_seek_time_performance(accumulator1.getS8_seek_time_performance() + accumulator2.getS8_seek_time_performance());
        centroid.setS9_power_on_hours(accumulator1.getS9_power_on_hours() + accumulator2.getS9_power_on_hours());
        centroid.setS10_spin_retry_count(accumulator1.getS10_spin_retry_count() + accumulator2.getS10_spin_retry_count());
        centroid.setS12_power_cycle_count(accumulator1.getS12_power_cycle_count() + accumulator2.getS12_power_cycle_count());
        centroid.setS173_wear_leveling_count(accumulator1.getS173_wear_leveling_count() + accumulator2.getS173_wear_leveling_count());
        centroid.setS174_unexpected_power_loss_count(accumulator1.getS174_unexpected_power_loss_count() + accumulator2.getS174_unexpected_power_loss_count());
        centroid.setS183_sata_downshift_count(accumulator1.getS183_sata_downshift_count() + accumulator2.getS183_sata_downshift_count());
        centroid.setS187_reported_uncorrectable_errors(accumulator1.getS187_reported_uncorrectable_errors() + accumulator2.getS187_reported_uncorrectable_errors());
        centroid.setS188_command_timeout(accumulator1.getS188_command_timeout() + accumulator2.getS188_command_timeout());
        centroid.setS189_high_fly_writes(accumulator1.getS189_high_fly_writes() + accumulator2.getS189_high_fly_writes());
        centroid.setS190_airflow_temperature_cel(accumulator1.getS190_airflow_temperature_cel() + accumulator2.getS190_airflow_temperature_cel());
        centroid.setS191_g_sense_error_rate(accumulator1.getS191_g_sense_error_rate() + accumulator2.getS191_g_sense_error_rate());
        centroid.setS192_power_off_retract_count(accumulator1.getS192_power_off_retract_count() + accumulator2.getS192_power_off_retract_count());
        centroid.setS193_load_unload_cycle_count(accumulator1.getS193_load_unload_cycle_count() + accumulator2.getS193_load_unload_cycle_count());
        centroid.setS194_temperature_celsius(accumulator1.getS194_temperature_celsius() + accumulator2.getS194_temperature_celsius());
        centroid.setS195_hardware_ecc_recovered(accumulator1.getS195_hardware_ecc_recovered() + accumulator2.getS195_hardware_ecc_recovered());
        centroid.setS196_reallocated_event_count(accumulator1.getS196_reallocated_event_count() + accumulator2.getS196_reallocated_event_count());
        centroid.setS197_current_pending_sector(accumulator1.getS197_current_pending_sector() + accumulator2.getS197_current_pending_sector());
        centroid.setS198_offline_uncorrectable(accumulator1.getS198_offline_uncorrectable() + accumulator2.getS198_offline_uncorrectable());
        centroid.setS199_udma_crc_error_count(accumulator1.getS199_udma_crc_error_count() + accumulator2.getS199_udma_crc_error_count());
        centroid.setS200_multi_zone_error_rate(accumulator1.getS200_multi_zone_error_rate() + accumulator2.getS200_multi_zone_error_rate());
        centroid.setS220_disk_shift(accumulator1.getS220_disk_shift() + accumulator2.getS220_disk_shift());
        centroid.setS222_loaded_hours(accumulator1.getS222_loaded_hours() + accumulator2.getS222_loaded_hours());
        centroid.setS223_load_retry_count(accumulator1.getS223_load_retry_count() + accumulator2.getS223_load_retry_count());
        centroid.setS226_load_in_time(accumulator1.getS226_load_in_time() + accumulator2.getS226_load_in_time());
        centroid.setS240_head_flying_hours(accumulator1.getS240_head_flying_hours() + accumulator2.getS240_head_flying_hours());
        centroid.setS241_total_lbas_written(accumulator1.getS241_total_lbas_written() + accumulator2.getS241_total_lbas_written());
        centroid.setS242_total_lbas_read(accumulator1.getS242_total_lbas_read() + accumulator2.getS242_total_lbas_read());
        return centroid;
    }
}
