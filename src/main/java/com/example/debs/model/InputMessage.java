package com.example.debs.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneId;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class InputMessage implements Serializable {
    @Nullable
    private LocalDateTime date;
    @Nullable
    private String serial_number;
    @Nullable
    private String model;
    @Nullable
    private Long failure;
    @Nullable
    private Long vaultId;
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
    @Override
    public String toString() {
        return "InputMessage{" +
                "date=" + date +
                ", serial_number='" + serial_number + '\'' +
                ", model='" + model + '\'' +
                ", failure=" + failure +
                ", vaultId=" + vaultId +
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

    public long getEventTimeEpochMilli() {
        ZoneId zoneId = ZoneId.of("UTC");
        return date.atZone(zoneId).toEpochSecond() * 1000;
    }
}
