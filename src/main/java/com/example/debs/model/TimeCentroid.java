package com.example.debs.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class TimeCentroid {
    private LocalDateTime dayEnd;
    private Long clusterId;
    private Double s1_read_error_rate;

    private Double s2_throughput_performance;

    private Double s3_spin_up_time;

    private Double s4_start_stop_count;

    private Double s5_reallocated_sector_count;

    private Double s7_seek_error_rate;

    private Double s8_seek_time_performance;

    private Double s9_power_on_hours;

    private Double s10_spin_retry_count;

    private Double s12_power_cycle_count;

    private Double s173_wear_leveling_count;

    private Double s174_unexpected_power_loss_count;

    private Double s183_sata_downshift_count;

    private Double s187_reported_uncorrectable_errors;

    private Double s188_command_timeout;

    private Double s189_high_fly_writes;

    private Double s190_airflow_temperature_cel;

    private Double s191_g_sense_error_rate;

    private Double s192_power_off_retract_count;

    private Double s193_load_unload_cycle_count;

    private Double s194_temperature_celsius;

    private Double s195_hardware_ecc_recovered;

    private Double s196_reallocated_event_count;

    private Double s197_current_pending_sector;

    private Double s198_offline_uncorrectable;

    private Double s199_udma_crc_error_count;

    private Double s200_multi_zone_error_rate;

    private Double s220_disk_shift;

    private Double s222_loaded_hours;

    private Double s223_load_retry_count;

    private Double s226_load_in_time;

    private Double s240_head_flying_hours;

    private Double s241_total_lbas_written;

    private Double s242_total_lbas_read;

    @Override
    public String toString() {
        return "TimeCentroid{" +
                "dayEnd=" + dayEnd +
                ", clusterId=" + clusterId +
                ", s1_read_error_rate=" + s1_read_error_rate +
                ", s2_throughput_performance=" + s2_throughput_performance +
                ", s3_spin_up_time=" + s3_spin_up_time +
                ", s4_start_stop_count=" + s4_start_stop_count +
                ", s5_reallocated_sector_count=" + s5_reallocated_sector_count +
                ", s7_seek_error_rate=" + s7_seek_error_rate +
                ", s8_seek_time_performance=" + s8_seek_time_performance +
                ", s9_power_on_hours=" + s9_power_on_hours +
                ", s10_spin_retry_count=" + s10_spin_retry_count +
                ", s12_power_cycle_count=" + s12_power_cycle_count +
                ", s173_wear_leveling_count=" + s173_wear_leveling_count +
                ", s174_unexpected_power_loss_count=" + s174_unexpected_power_loss_count +
                ", s183_sata_downshift_count=" + s183_sata_downshift_count +
                ", s187_reported_uncorrectable_errors=" + s187_reported_uncorrectable_errors +
                ", s188_command_timeout=" + s188_command_timeout +
                ", s189_high_fly_writes=" + s189_high_fly_writes +
                ", s190_airflow_temperature_cel=" + s190_airflow_temperature_cel +
                ", s191_g_sense_error_rate=" + s191_g_sense_error_rate +
                ", s192_power_off_retract_count=" + s192_power_off_retract_count +
                ", s193_load_unload_cycle_count=" + s193_load_unload_cycle_count +
                ", s194_temperature_celsius=" + s194_temperature_celsius +
                ", s195_hardware_ecc_recovered=" + s195_hardware_ecc_recovered +
                ", s196_reallocated_event_count=" + s196_reallocated_event_count +
                ", s197_current_pending_sector=" + s197_current_pending_sector +
                ", s198_offline_uncorrectable=" + s198_offline_uncorrectable +
                ", s199_udma_crc_error_count=" + s199_udma_crc_error_count +
                ", s200_multi_zone_error_rate=" + s200_multi_zone_error_rate +
                ", s220_disk_shift=" + s220_disk_shift +
                ", s222_loaded_hours=" + s222_loaded_hours +
                ", s223_load_retry_count=" + s223_load_retry_count +
                ", s226_load_in_time=" + s226_load_in_time +
                ", s240_head_flying_hours=" + s240_head_flying_hours +
                ", s241_total_lbas_written=" + s241_total_lbas_written +
                ", s242_total_lbas_read=" + s242_total_lbas_read +
                '}';
    }
}
