package com.example.debs.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class Centroid {
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
}
