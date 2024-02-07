package com.example.debs.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Setter
public class Smart implements Serializable {
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

    public Smart(double[] val) {
        this.s1_read_error_rate = val[0];
        this.s2_throughput_performance = val[1];
        this.s3_spin_up_time = val[2];
        this.s4_start_stop_count = val[3];
        this.s5_reallocated_sector_count = val[4];
        this.s7_seek_error_rate = val[5];
        this.s8_seek_time_performance = val[6];
        this.s9_power_on_hours = val[7];
        this.s10_spin_retry_count = val[8];
        this.s12_power_cycle_count = val[9];
        this.s173_wear_leveling_count = val[10];
        this.s174_unexpected_power_loss_count = val[11];
        this.s183_sata_downshift_count = val[12];
        this.s187_reported_uncorrectable_errors = val[13];
        this.s188_command_timeout = val[14];
        this.s189_high_fly_writes = val[15];
        this.s190_airflow_temperature_cel = val[16];
        this.s191_g_sense_error_rate = val[17];
        this.s192_power_off_retract_count = val[18];
        this.s193_load_unload_cycle_count = val[19];
        this.s194_temperature_celsius = val[20];
        this.s195_hardware_ecc_recovered = val[21];
        this.s196_reallocated_event_count = val[22];
        this.s197_current_pending_sector = val[23];
        this.s198_offline_uncorrectable = val[24];
        this.s199_udma_crc_error_count = val[25];
        this.s200_multi_zone_error_rate = val[26];
        this.s220_disk_shift = val[27];
        this.s222_loaded_hours = val[28];
        this.s223_load_retry_count = val[29];
        this.s226_load_in_time = val[30];
        this.s240_head_flying_hours = val[31];
        this.s241_total_lbas_written = val[32];
        this.s242_total_lbas_read = val[33];
    }

    public void initSmart() {
        this.s1_read_error_rate = 0.0;
        this.s2_throughput_performance = 0.0;
        this.s3_spin_up_time = 0.0;
        this.s4_start_stop_count = 0.0;
        this.s5_reallocated_sector_count = 0.0;
        this.s7_seek_error_rate = 0.0;
        this.s8_seek_time_performance = 0.0;
        this.s9_power_on_hours = 0.0;
        this.s10_spin_retry_count = 0.0;
        this.s12_power_cycle_count = 0.0;
        this.s173_wear_leveling_count = 0.0;
        this.s174_unexpected_power_loss_count = 0.0;
        this.s183_sata_downshift_count = 0.0;
        this.s187_reported_uncorrectable_errors = 0.0;
        this.s188_command_timeout = 0.0;
        this.s189_high_fly_writes = 0.0;
        this.s190_airflow_temperature_cel = 0.0;
        this.s191_g_sense_error_rate = 0.0;
        this.s192_power_off_retract_count = 0.0;
        this.s193_load_unload_cycle_count = 0.0;
        this.s194_temperature_celsius = 0.0;
        this.s195_hardware_ecc_recovered = 0.0;
        this.s196_reallocated_event_count = 0.0;
        this.s197_current_pending_sector = 0.0;
        this.s198_offline_uncorrectable = 0.0;
        this.s199_udma_crc_error_count = 0.0;
        this.s200_multi_zone_error_rate = 0.0;
        this.s220_disk_shift = 0.0;
        this.s222_loaded_hours = 0.0;
        this.s223_load_retry_count = 0.0;
        this.s226_load_in_time = 0.0;
        this.s240_head_flying_hours = 0.0;
        this.s241_total_lbas_written = 0.0;
        this.s242_total_lbas_read = 0.0;
    }

    // Overloading plus operator
    public Smart plus(Smart smart2) {
        return new Smart(
                this.s1_read_error_rate + smart2.s1_read_error_rate,
                this.s2_throughput_performance + smart2.s2_throughput_performance,
                this.s3_spin_up_time + smart2.s3_spin_up_time,
                this.s4_start_stop_count + smart2.s4_start_stop_count,
                this.s5_reallocated_sector_count + smart2.s5_reallocated_sector_count,
                this.s7_seek_error_rate + smart2.s7_seek_error_rate,
                this.s8_seek_time_performance + smart2.s8_seek_time_performance,
                this.s9_power_on_hours + smart2.s9_power_on_hours,
                this.s10_spin_retry_count + smart2.s10_spin_retry_count,
                this.s12_power_cycle_count + smart2.s12_power_cycle_count,
                this.s173_wear_leveling_count + smart2.s173_wear_leveling_count,
                this.s174_unexpected_power_loss_count + smart2.s174_unexpected_power_loss_count,
                this.s183_sata_downshift_count + smart2.s183_sata_downshift_count,
                this.s187_reported_uncorrectable_errors + smart2.s187_reported_uncorrectable_errors,
                this.s188_command_timeout + smart2.s188_command_timeout,
                this.s189_high_fly_writes + smart2.s189_high_fly_writes,
                this.s190_airflow_temperature_cel + smart2.s190_airflow_temperature_cel,
                this.s191_g_sense_error_rate + smart2.s191_g_sense_error_rate,
                this.s192_power_off_retract_count + smart2.s192_power_off_retract_count,
                this.s193_load_unload_cycle_count + smart2.s193_load_unload_cycle_count,
                this.s194_temperature_celsius + smart2.s194_temperature_celsius,
                this.s195_hardware_ecc_recovered + smart2.s195_hardware_ecc_recovered,
                this.s196_reallocated_event_count + smart2.s196_reallocated_event_count,
                this.s197_current_pending_sector + smart2.s197_current_pending_sector,
                this.s198_offline_uncorrectable + smart2.s198_offline_uncorrectable,
                this.s199_udma_crc_error_count + smart2.s199_udma_crc_error_count,
                this.s200_multi_zone_error_rate + smart2.s200_multi_zone_error_rate,
                this.s220_disk_shift + smart2.s220_disk_shift,
                this.s222_loaded_hours + smart2.s222_loaded_hours,
                this.s223_load_retry_count + smart2.s223_load_retry_count,
                this.s226_load_in_time + smart2.s226_load_in_time,
                this.s240_head_flying_hours + smart2.s240_head_flying_hours,
                this.s241_total_lbas_written + smart2.s241_total_lbas_written,
                this.s242_total_lbas_read + smart2.s242_total_lbas_read
        );
    }

    // Overloading divide operator
    public Smart divide(long n) {
        if (n == 0) {
            throw new IllegalArgumentException("Cannot divide by 0");
        }
        return new Smart(
                this.s1_read_error_rate / n,
                this.s2_throughput_performance / n,
                this.s3_spin_up_time / n,
                this.s4_start_stop_count / n,
                this.s5_reallocated_sector_count / n,
                this.s7_seek_error_rate / n,
                this.s8_seek_time_performance / n,
                this.s9_power_on_hours / n,
                this.s10_spin_retry_count / n,
                this.s12_power_cycle_count / n,
                this.s173_wear_leveling_count / n,
                this.s174_unexpected_power_loss_count / n,
                this.s183_sata_downshift_count / n,
                this.s187_reported_uncorrectable_errors / n,
                this.s188_command_timeout / n,
                this.s189_high_fly_writes / n,
                this.s190_airflow_temperature_cel / n,
                this.s191_g_sense_error_rate / n,
                this.s192_power_off_retract_count / n,
                this.s193_load_unload_cycle_count / n,
                this.s194_temperature_celsius / n,
                this.s195_hardware_ecc_recovered / n,
                this.s196_reallocated_event_count / n,
                this.s197_current_pending_sector / n,
                this.s198_offline_uncorrectable / n,
                this.s199_udma_crc_error_count / n,
                this.s200_multi_zone_error_rate / n,
                this.s220_disk_shift / n,
                this.s222_loaded_hours / n,
                this.s223_load_retry_count / n,
                this.s226_load_in_time / n,
                this.s240_head_flying_hours / n,
                this.s241_total_lbas_written / n,
                this.s242_total_lbas_read / n
        );
    }

    @Override
    public String toString() {
        return "Smart{" +
                "s1_read_error_rate=" + s1_read_error_rate +
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

    public double euclideanDistance(Smart smart2) {
        return Math.sqrt(
                Math.pow(this.s1_read_error_rate - smart2.s1_read_error_rate, 2) +
                Math.pow(this.s2_throughput_performance - smart2.s2_throughput_performance, 2) +
                Math.pow(this.s3_spin_up_time - smart2.s3_spin_up_time, 2) +
                Math.pow(this.s4_start_stop_count - smart2.s4_start_stop_count, 2) +
                Math.pow(this.s5_reallocated_sector_count - smart2.s5_reallocated_sector_count, 2) +
                Math.pow(this.s7_seek_error_rate - smart2.s7_seek_error_rate, 2) +
                Math.pow(this.s8_seek_time_performance - smart2.s8_seek_time_performance, 2) +
                Math.pow(this.s9_power_on_hours - smart2.s9_power_on_hours, 2) +
                Math.pow(this.s10_spin_retry_count - smart2.s10_spin_retry_count, 2) +
                Math.pow(this.s12_power_cycle_count - smart2.s12_power_cycle_count, 2) +
                Math.pow(this.s173_wear_leveling_count - smart2.s173_wear_leveling_count, 2) +
                Math.pow(this.s174_unexpected_power_loss_count - smart2.s174_unexpected_power_loss_count, 2) +
                Math.pow(this.s183_sata_downshift_count - smart2.s183_sata_downshift_count, 2) +
                Math.pow(this.s187_reported_uncorrectable_errors - smart2.s187_reported_uncorrectable_errors, 2) +
                Math.pow(this.s188_command_timeout - smart2.s188_command_timeout, 2) +
                Math.pow(this.s189_high_fly_writes - smart2.s189_high_fly_writes, 2) +
                Math.pow(this.s190_airflow_temperature_cel - smart2.s190_airflow_temperature_cel, 2) +
                Math.pow(this.s191_g_sense_error_rate - smart2.s191_g_sense_error_rate, 2) +
                Math.pow(this.s192_power_off_retract_count - smart2.s192_power_off_retract_count, 2) +
                Math.pow(this.s193_load_unload_cycle_count - smart2.s193_load_unload_cycle_count, 2) +
                Math.pow(this.s194_temperature_celsius - smart2.s194_temperature_celsius, 2) +
                Math.pow(this.s195_hardware_ecc_recovered - smart2.s195_hardware_ecc_recovered, 2) +
                Math.pow(this.s196_reallocated_event_count - smart2.s196_reallocated_event_count, 2) +
                Math.pow(this.s197_current_pending_sector - smart2.s197_current_pending_sector, 2) +
                Math.pow(this.s198_offline_uncorrectable - smart2.s198_offline_uncorrectable, 2) +
                Math.pow(this.s199_udma_crc_error_count - smart2.s199_udma_crc_error_count, 2) +
                Math.pow(this.s200_multi_zone_error_rate - smart2.s200_multi_zone_error_rate, 2) +
                Math.pow(this.s220_disk_shift - smart2.s220_disk_shift, 2) +
                Math.pow(this.s222_loaded_hours - smart2.s222_loaded_hours, 2) +
                Math.pow(this.s223_load_retry_count - smart2.s223_load_retry_count, 2) +
                Math.pow(this.s226_load_in_time - smart2.s226_load_in_time, 2) +
                Math.pow(this.s240_head_flying_hours - smart2.s240_head_flying_hours, 2) +
                Math.pow(this.s241_total_lbas_written - smart2.s241_total_lbas_written, 2) +
                Math.pow(this.s242_total_lbas_read - smart2.s242_total_lbas_read, 2)
        );
    }

    public double infinityNorm(Smart smart2) {
        // get rid of negative values in this Smart object and smart2
        List<Double> diff = new ArrayList<>();
        if (this.s1_read_error_rate >= 0 && smart2.s1_read_error_rate >= 0) {
            diff.add(Math.abs(this.s1_read_error_rate - smart2.s1_read_error_rate));
        }
        if (this.s2_throughput_performance >= 0 && smart2.s2_throughput_performance >= 0) {
            diff.add(Math.abs(this.s2_throughput_performance - smart2.s2_throughput_performance));
        }
        if (this.s3_spin_up_time >= 0 && smart2.s3_spin_up_time >= 0) {
            diff.add(Math.abs(this.s3_spin_up_time - smart2.s3_spin_up_time));
        }
        if (this.s4_start_stop_count >= 0 && smart2.s4_start_stop_count >= 0) {
            diff.add(Math.abs(this.s4_start_stop_count - smart2.s4_start_stop_count));
        }
        if (this.s5_reallocated_sector_count >= 0 && smart2.s5_reallocated_sector_count >= 0) {
            diff.add(Math.abs(this.s5_reallocated_sector_count - smart2.s5_reallocated_sector_count));
        }
        if (this.s7_seek_error_rate >= 0 && smart2.s7_seek_error_rate >= 0) {
            diff.add(Math.abs(this.s7_seek_error_rate - smart2.s7_seek_error_rate));
        }
        if (this.s8_seek_time_performance >= 0 && smart2.s8_seek_time_performance >= 0) {
            diff.add(Math.abs(this.s8_seek_time_performance - smart2.s8_seek_time_performance));
        }
        if (this.s9_power_on_hours >= 0 && smart2.s9_power_on_hours >= 0) {
            diff.add(Math.abs(this.s9_power_on_hours - smart2.s9_power_on_hours));
        }
        if (this.s10_spin_retry_count >= 0 && smart2.s10_spin_retry_count >= 0) {
            diff.add(Math.abs(this.s10_spin_retry_count - smart2.s10_spin_retry_count));
        }
        if (this.s12_power_cycle_count >= 0 && smart2.s12_power_cycle_count >= 0) {
            diff.add(Math.abs(this.s12_power_cycle_count - smart2.s12_power_cycle_count));
        }
        if (this.s173_wear_leveling_count >= 0 && smart2.s173_wear_leveling_count >= 0) {
            diff.add(Math.abs(this.s173_wear_leveling_count - smart2.s173_wear_leveling_count));
        }
        if (this.s174_unexpected_power_loss_count >= 0 && smart2.s174_unexpected_power_loss_count >= 0) {
            diff.add(Math.abs(this.s174_unexpected_power_loss_count - smart2.s174_unexpected_power_loss_count));
        }
        if (this.s183_sata_downshift_count >= 0 && smart2.s183_sata_downshift_count >= 0) {
            diff.add(Math.abs(this.s183_sata_downshift_count - smart2.s183_sata_downshift_count));
        }
        if (this.s187_reported_uncorrectable_errors >= 0 && smart2.s187_reported_uncorrectable_errors >= 0) {
            diff.add(Math.abs(this.s187_reported_uncorrectable_errors - smart2.s187_reported_uncorrectable_errors));
        }
        if (this.s188_command_timeout >= 0 && smart2.s188_command_timeout >= 0) {
            diff.add(Math.abs(this.s188_command_timeout - smart2.s188_command_timeout));
        }
        if (this.s189_high_fly_writes >= 0 && smart2.s189_high_fly_writes >= 0) {
            diff.add(Math.abs(this.s189_high_fly_writes - smart2.s189_high_fly_writes));
        }
        if (this.s190_airflow_temperature_cel >= 0 && smart2.s190_airflow_temperature_cel >= 0) {
            diff.add(Math.abs(this.s190_airflow_temperature_cel - smart2.s190_airflow_temperature_cel));
        }
        if (this.s191_g_sense_error_rate >= 0 && smart2.s191_g_sense_error_rate >= 0) {
            diff.add(Math.abs(this.s191_g_sense_error_rate - smart2.s191_g_sense_error_rate));
        }
        if (this.s192_power_off_retract_count >= 0 && smart2.s192_power_off_retract_count >= 0) {
            diff.add(Math.abs(this.s192_power_off_retract_count - smart2.s192_power_off_retract_count));
        }
        if (this.s193_load_unload_cycle_count >= 0 && smart2.s193_load_unload_cycle_count >= 0) {
            diff.add(Math.abs(this.s193_load_unload_cycle_count - smart2.s193_load_unload_cycle_count));
        }
        if (this.s194_temperature_celsius >= 0 && smart2.s194_temperature_celsius >= 0) {
            diff.add(Math.abs(this.s194_temperature_celsius - smart2.s194_temperature_celsius));
        }
        if (this.s195_hardware_ecc_recovered >= 0 && smart2.s195_hardware_ecc_recovered >= 0) {
            diff.add(Math.abs(this.s195_hardware_ecc_recovered - smart2.s195_hardware_ecc_recovered));
        }
        if (this.s196_reallocated_event_count >= 0 && smart2.s196_reallocated_event_count >= 0) {
            diff.add(Math.abs(this.s196_reallocated_event_count - smart2.s196_reallocated_event_count));
        }
        if (this.s197_current_pending_sector >= 0 && smart2.s197_current_pending_sector >= 0) {
            diff.add(Math.abs(this.s197_current_pending_sector - smart2.s197_current_pending_sector));
        }
        if (this.s198_offline_uncorrectable >= 0 && smart2.s198_offline_uncorrectable >= 0) {
            diff.add(Math.abs(this.s198_offline_uncorrectable - smart2.s198_offline_uncorrectable));
        }
        if (this.s199_udma_crc_error_count >= 0 && smart2.s199_udma_crc_error_count >= 0) {
            diff.add(Math.abs(this.s199_udma_crc_error_count - smart2.s199_udma_crc_error_count));
        }
        if (this.s200_multi_zone_error_rate >= 0 && smart2.s200_multi_zone_error_rate >= 0) {
            diff.add(Math.abs(this.s200_multi_zone_error_rate - smart2.s200_multi_zone_error_rate));
        }
        if (this.s220_disk_shift >= 0 && smart2.s220_disk_shift >= 0) {
            diff.add(Math.abs(this.s220_disk_shift - smart2.s220_disk_shift));
        }
        if (this.s222_loaded_hours >= 0 && smart2.s222_loaded_hours >= 0) {
            diff.add(Math.abs(this.s222_loaded_hours - smart2.s222_loaded_hours));
        }
        if (this.s223_load_retry_count >= 0 && smart2.s223_load_retry_count >= 0) {
            diff.add(Math.abs(this.s223_load_retry_count - smart2.s223_load_retry_count));
        }
        if (this.s226_load_in_time >= 0 && smart2.s226_load_in_time >= 0) {
            diff.add(Math.abs(this.s226_load_in_time - smart2.s226_load_in_time));
        }
        if (this.s240_head_flying_hours >= 0 && smart2.s240_head_flying_hours >= 0) {
            diff.add(Math.abs(this.s240_head_flying_hours - smart2.s240_head_flying_hours));
        }
        if (this.s241_total_lbas_written >= 0 && smart2.s241_total_lbas_written >= 0) {
            diff.add(Math.abs(this.s241_total_lbas_written - smart2.s241_total_lbas_written));
        }
        if (this.s242_total_lbas_read >= 0 && smart2.s242_total_lbas_read >= 0) {
            diff.add(Math.abs(this.s242_total_lbas_read - smart2.s242_total_lbas_read));
        }
        return diff.stream().mapToDouble(Double::doubleValue).max().orElse(0.0);
    }
}
