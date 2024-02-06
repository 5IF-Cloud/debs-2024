package com.example.debs.schema;

import com.example.debs.model.InputMessage;
import com.example.debs.model.InputMessageDto;
import com.example.debs.model.Smart;
import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class InputMessageDeserializerSchema implements DeserializationSchema<InputMessage> {

    @Override
    public InputMessage deserialize(byte[] bytes) {
        Gson gson = new Gson();
        String jsonString = new String(bytes, StandardCharsets.UTF_8);

        InputMessageDto inputMessageDto = gson.fromJson(jsonString, InputMessageDto.class);

        InputMessage inputMessage = new InputMessage();
        LocalDateTime date = LocalDateTime.ofEpochSecond(inputMessageDto.getDate(), 0, ZoneOffset.UTC);
        inputMessage.setDate(date);
        inputMessage.setSerial_number(inputMessageDto.getSerial_number());
        inputMessage.setModel(inputMessageDto.getModel());
        inputMessage.setFailure(inputMessageDto.getFailure());
        inputMessage.setVaultId(inputMessageDto.getVault_id());
        Smart smart = inputMessageDto.getSmart();

        // if one of these below is NaN, then set them to -1.0
        assert smart.getS1_read_error_rate() != null;
        smart.setS1_read_error_rate(smart.getS1_read_error_rate().isNaN() ? -1.0 : smart.getS1_read_error_rate());
        assert smart.getS2_throughput_performance() != null;
        smart.setS2_throughput_performance(smart.getS2_throughput_performance().isNaN() ? -1.0 : smart.getS2_throughput_performance());
        assert smart.getS3_spin_up_time() != null;
        smart.setS3_spin_up_time(smart.getS3_spin_up_time().isNaN() ? -1.0 : smart.getS3_spin_up_time());
        assert smart.getS4_start_stop_count() != null;
        smart.setS4_start_stop_count(smart.getS4_start_stop_count().isNaN() ? -1.0 : smart.getS4_start_stop_count());
        assert smart.getS5_reallocated_sector_count() != null;
        smart.setS5_reallocated_sector_count(smart.getS5_reallocated_sector_count().isNaN() ? -1.0 : smart.getS5_reallocated_sector_count());
        assert smart.getS7_seek_error_rate() != null;
        smart.setS7_seek_error_rate(smart.getS7_seek_error_rate().isNaN() ? -1.0 : smart.getS7_seek_error_rate());
        assert smart.getS8_seek_time_performance() != null;
        smart.setS8_seek_time_performance(smart.getS8_seek_time_performance().isNaN() ? -1.0 : smart.getS8_seek_time_performance());
        assert smart.getS9_power_on_hours() != null;
        smart.setS9_power_on_hours(smart.getS9_power_on_hours().isNaN() ? -1.0 : smart.getS9_power_on_hours());
        assert smart.getS10_spin_retry_count() != null;
        smart.setS10_spin_retry_count(smart.getS10_spin_retry_count().isNaN() ? -1.0 : smart.getS10_spin_retry_count());
        assert smart.getS12_power_cycle_count() != null;
        smart.setS12_power_cycle_count(smart.getS12_power_cycle_count().isNaN() ? -1.0 : smart.getS12_power_cycle_count());
        assert smart.getS173_wear_leveling_count() != null;
        smart.setS173_wear_leveling_count(smart.getS173_wear_leveling_count().isNaN() ? -1.0 : smart.getS173_wear_leveling_count());
        assert smart.getS174_unexpected_power_loss_count() != null;
        smart.setS174_unexpected_power_loss_count(smart.getS174_unexpected_power_loss_count().isNaN() ? -1.0 : smart.getS174_unexpected_power_loss_count());
        assert smart.getS183_sata_downshift_count() != null;
        smart.setS183_sata_downshift_count(smart.getS183_sata_downshift_count().isNaN() ? -1.0 : smart.getS183_sata_downshift_count());
        assert smart.getS187_reported_uncorrectable_errors() != null;
        smart.setS187_reported_uncorrectable_errors(smart.getS187_reported_uncorrectable_errors().isNaN() ? -1.0 : smart.getS187_reported_uncorrectable_errors());
        assert smart.getS188_command_timeout() != null;
        smart.setS188_command_timeout(smart.getS188_command_timeout().isNaN() ? -1.0 : smart.getS188_command_timeout());
        assert smart.getS189_high_fly_writes() != null;
        smart.setS189_high_fly_writes(smart.getS189_high_fly_writes().isNaN() ? -1.0 : smart.getS189_high_fly_writes());
        assert smart.getS190_airflow_temperature_cel() != null;
        smart.setS190_airflow_temperature_cel(smart.getS190_airflow_temperature_cel().isNaN() ? -1.0 : smart.getS190_airflow_temperature_cel());
        assert smart.getS191_g_sense_error_rate() != null;
        smart.setS191_g_sense_error_rate(smart.getS191_g_sense_error_rate().isNaN() ? -1.0 : smart.getS191_g_sense_error_rate());
        assert smart.getS192_power_off_retract_count() != null;
        smart.setS192_power_off_retract_count(smart.getS192_power_off_retract_count().isNaN() ? -1.0 : smart.getS192_power_off_retract_count());
        assert smart.getS193_load_unload_cycle_count() != null;
        smart.setS193_load_unload_cycle_count(smart.getS193_load_unload_cycle_count().isNaN() ? -1.0 : smart.getS193_load_unload_cycle_count());
        assert smart.getS194_temperature_celsius() != null;
        smart.setS194_temperature_celsius(smart.getS194_temperature_celsius().isNaN() ? -1.0 : smart.getS194_temperature_celsius());
        assert smart.getS195_hardware_ecc_recovered() != null;
        smart.setS195_hardware_ecc_recovered(smart.getS195_hardware_ecc_recovered().isNaN() ? -1.0 : smart.getS195_hardware_ecc_recovered());
        assert smart.getS196_reallocated_event_count() != null;
        smart.setS196_reallocated_event_count(smart.getS196_reallocated_event_count().isNaN() ? -1.0 : smart.getS196_reallocated_event_count());
        assert smart.getS197_current_pending_sector() != null;
        smart.setS197_current_pending_sector(smart.getS197_current_pending_sector().isNaN() ? -1.0 : smart.getS197_current_pending_sector());
        assert smart.getS198_offline_uncorrectable() != null;
        smart.setS198_offline_uncorrectable(smart.getS198_offline_uncorrectable().isNaN() ? -1.0 : smart.getS198_offline_uncorrectable());
        assert smart.getS199_udma_crc_error_count() != null;
        smart.setS199_udma_crc_error_count(smart.getS199_udma_crc_error_count().isNaN() ? -1.0 : smart.getS199_udma_crc_error_count());
        assert smart.getS200_multi_zone_error_rate() != null;
        smart.setS200_multi_zone_error_rate(smart.getS200_multi_zone_error_rate().isNaN() ? -1.0 : smart.getS200_multi_zone_error_rate());
        assert smart.getS220_disk_shift() != null;
        smart.setS220_disk_shift(smart.getS220_disk_shift().isNaN() ? -1.0 : smart.getS220_disk_shift());
        assert smart.getS222_loaded_hours() != null;
        smart.setS222_loaded_hours(smart.getS222_loaded_hours().isNaN() ? -1.0 : smart.getS222_loaded_hours());
        assert smart.getS223_load_retry_count() != null;
        smart.setS223_load_retry_count(smart.getS223_load_retry_count().isNaN() ? -1.0 : smart.getS223_load_retry_count());
        assert smart.getS226_load_in_time() != null;
        smart.setS226_load_in_time(smart.getS226_load_in_time().isNaN() ? -1.0 : smart.getS226_load_in_time());
        assert smart.getS240_head_flying_hours() != null;
        smart.setS240_head_flying_hours(smart.getS240_head_flying_hours().isNaN() ? -1.0 : smart.getS240_head_flying_hours());
        assert smart.getS241_total_lbas_written() != null;
        smart.setS241_total_lbas_written(smart.getS241_total_lbas_written().isNaN() ? -1.0 : smart.getS241_total_lbas_written());
        assert smart.getS242_total_lbas_read() != null;
        smart.setS242_total_lbas_read(smart.getS242_total_lbas_read().isNaN() ? -1.0 : smart.getS242_total_lbas_read());
        inputMessage.setSmart(smart);
        return inputMessage;
    }

    @Override
    public boolean isEndOfStream(InputMessage inputMessage) {
        return false;
    }

    @Override
    public TypeInformation<InputMessage> getProducedType() {
        return TypeInformation.of(InputMessage.class);
    }
}
