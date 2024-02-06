package com.example.debs.scaler;

import com.example.debs.model.Smart;

public class MinMaxScaler {
    public static Smart scale(Smart value, Double[][] minMax) {
        Smart scaled = new Smart();
        scaled.setS1_read_error_rate((value.getS1_read_error_rate() - minMax[0][0]) / (minMax[0][1] - minMax[0][0]));
        scaled.setS2_throughput_performance((value.getS2_throughput_performance() - minMax[1][0]) / (minMax[1][1] - minMax[1][0]));
        scaled.setS3_spin_up_time((value.getS3_spin_up_time() - minMax[2][0]) / (minMax[2][1] - minMax[2][0]));
        scaled.setS4_start_stop_count((value.getS4_start_stop_count() - minMax[3][0]) / (minMax[3][1] - minMax[3][0]));
        scaled.setS5_reallocated_sector_count((value.getS5_reallocated_sector_count() - minMax[4][0]) / (minMax[4][1] - minMax[4][0]));
        scaled.setS7_seek_error_rate((value.getS7_seek_error_rate() - minMax[5][0]) / (minMax[5][1] - minMax[5][0]));
        scaled.setS8_seek_time_performance((value.getS8_seek_time_performance() - minMax[6][0]) / (minMax[6][1] - minMax[6][0]));
        scaled.setS9_power_on_hours((value.getS9_power_on_hours() - minMax[7][0]) / (minMax[7][1] - minMax[7][0]));
        scaled.setS10_spin_retry_count((value.getS10_spin_retry_count() - minMax[8][0]) / (minMax[8][1] - minMax[8][0]));
        scaled.setS12_power_cycle_count((value.getS12_power_cycle_count() - minMax[9][0]) / (minMax[9][1] - minMax[9][0]));
        scaled.setS173_wear_leveling_count((value.getS173_wear_leveling_count() - minMax[10][0]) / (minMax[10][1] - minMax[10][0]));
        scaled.setS174_unexpected_power_loss_count((value.getS174_unexpected_power_loss_count() - minMax[11][0]) / (minMax[11][1] - minMax[11][0]));
        scaled.setS183_sata_downshift_count((value.getS183_sata_downshift_count() - minMax[12][0]) / (minMax[12][1] - minMax[12][0]));
        scaled.setS187_reported_uncorrectable_errors((value.getS187_reported_uncorrectable_errors() - minMax[13][0]) / (minMax[13][1] - minMax[13][0]));
        scaled.setS188_command_timeout((value.getS188_command_timeout() - minMax[14][0]) / (minMax[14][1] - minMax[14][0]));
        scaled.setS189_high_fly_writes((value.getS189_high_fly_writes() - minMax[15][0]) / (minMax[15][1] - minMax[15][0]));
        scaled.setS190_airflow_temperature_cel((value.getS190_airflow_temperature_cel() - minMax[16][0]) / (minMax[16][1] - minMax[16][0]));
        scaled.setS191_g_sense_error_rate((value.getS191_g_sense_error_rate() - minMax[17][0]) / (minMax[17][1] - minMax[17][0]));
        scaled.setS192_power_off_retract_count((value.getS192_power_off_retract_count() - minMax[18][0]) / (minMax[18][1] - minMax[18][0]));
        scaled.setS193_load_unload_cycle_count((value.getS193_load_unload_cycle_count() - minMax[19][0]) / (minMax[19][1] - minMax[19][0]));
        scaled.setS194_temperature_celsius((value.getS194_temperature_celsius() - minMax[20][0]) / (minMax[20][1] - minMax[20][0]));
        scaled.setS195_hardware_ecc_recovered((value.getS195_hardware_ecc_recovered() - minMax[21][0]) / (minMax[21][1] - minMax[21][0]));
        scaled.setS196_reallocated_event_count((value.getS196_reallocated_event_count() - minMax[22][0]) / (minMax[22][1] - minMax[22][0]));
        scaled.setS197_current_pending_sector((value.getS197_current_pending_sector() - minMax[23][0]) / (minMax[23][1] - minMax[23][0]));
        scaled.setS198_offline_uncorrectable((value.getS198_offline_uncorrectable() - minMax[24][0]) / (minMax[24][1] - minMax[24][0]));
        scaled.setS199_udma_crc_error_count((value.getS199_udma_crc_error_count() - minMax[25][0]) / (minMax[25][1] - minMax[25][0]));
        scaled.setS200_multi_zone_error_rate((value.getS200_multi_zone_error_rate() - minMax[26][0]) / (minMax[26][1] - minMax[26][0]));
        scaled.setS220_disk_shift((value.getS220_disk_shift() - minMax[27][0]) / (minMax[27][1] - minMax[27][0]));
        scaled.setS222_loaded_hours((value.getS222_loaded_hours() - minMax[28][0]) / (minMax[28][1] - minMax[28][0]));
        scaled.setS223_load_retry_count((value.getS223_load_retry_count() - minMax[29][0]) / (minMax[29][1] - minMax[29][0]));
        scaled.setS226_load_in_time((value.getS226_load_in_time() - minMax[30][0]) / (minMax[30][1] - minMax[30][0]));
        scaled.setS240_head_flying_hours((value.getS240_head_flying_hours() - minMax[31][0]) / (minMax[31][1] - minMax[31][0]));
        scaled.setS241_total_lbas_written((value.getS241_total_lbas_written() - minMax[32][0]) / (minMax[32][1] - minMax[32][0]));
        scaled.setS242_total_lbas_read((value.getS242_total_lbas_read() - minMax[33][0]) / (minMax[33][1] - minMax[33][0]));
        return scaled;
    }
}
