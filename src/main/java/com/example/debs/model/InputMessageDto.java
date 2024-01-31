package com.example.debs.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.annotation.Nullable;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class InputMessageDto {
    private Long date;
    private String serial_number;
    private String model;
    private Long failure;
    private Long vault_id;
    @Nullable
    private Double s1_read_error_rate;
    @Nullable
    private Double s2_throughput_performance;
    @Nullable
    private Double s3_spin_up_time;
    @Nullable
    private Double s4_start_stop_count;
    @Nullable
    private Double s5_reallocated_sector_count;
    @Nullable
    private Double s7_seek_error_rate;
    @Nullable
    private Double s8_seek_time_performance;
    @Nullable
    private Double s9_power_on_hours;
    @Nullable
    private Double s10_spin_retry_count;
    @Nullable
    private Double s12_power_cycle_count;
    @Nullable
    private Double s173_wear_leveling_count;
    @Nullable
    private Double s174_unexpected_power_loss_count;
    @Nullable
    private Double s183_sata_downshift_count;
    @Nullable
    private Double s187_reported_uncorrectable_errors;
    @Nullable
    private Double s188_command_timeout;
    @Nullable
    private Double s189_high_fly_writes;
    @Nullable
    private Double s190_airflow_temperature_cel;
    @Nullable
    private Double s191_g_sense_error_rate;
    @Nullable
    private Double s192_power_off_retract_count;
    @Nullable
    private Double s193_load_unload_cycle_count;
    @Nullable
    private Double s194_temperature_celsius;
    @Nullable
    private Double s195_hardware_ecc_recovered;
    @Nullable
    private Double s196_reallocated_event_count;
    @Nullable
    private Double s197_current_pending_sector;
    @Nullable
    private Double s198_offline_uncorrectable;
    @Nullable
    private Double s199_udma_crc_error_count;
    @Nullable
    private Double s200_multi_zone_error_rate;
    @Nullable
    private Double s220_disk_shift;
    @Nullable
    private Double s222_loaded_hours;
    @Nullable
    private Double s223_load_retry_count;
    @Nullable
    private Double s226_load_in_time;
    @Nullable
    private Double s240_head_flying_hours;
    @Nullable
    private Double s241_total_lbas_written;
    @Nullable
    private Double s242_total_lbas_read;
}
