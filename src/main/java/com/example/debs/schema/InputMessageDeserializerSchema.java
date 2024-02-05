package com.example.debs.schema;

import com.example.debs.model.InputMessage;
import com.example.debs.model.InputMessageDto;
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

        // if one of these below is NaN, then set them to -1.0
        assert inputMessageDto.getS1_read_error_rate() != null;
        inputMessage.setS1_read_error_rate(inputMessageDto.getS1_read_error_rate().isNaN() ? -1.0 : inputMessageDto.getS1_read_error_rate());
        assert inputMessageDto.getS2_throughput_performance() != null;
        inputMessage.setS2_throughput_performance(inputMessageDto.getS2_throughput_performance().isNaN() ? -1.0 : inputMessageDto.getS2_throughput_performance());
        assert inputMessageDto.getS3_spin_up_time() != null;
        inputMessage.setS3_spin_up_time(inputMessageDto.getS3_spin_up_time().isNaN() ? -1.0 : inputMessageDto.getS3_spin_up_time());
        assert inputMessageDto.getS4_start_stop_count() != null;
        inputMessage.setS4_start_stop_count(inputMessageDto.getS4_start_stop_count().isNaN() ? -1.0 : inputMessageDto.getS4_start_stop_count());
        assert inputMessageDto.getS5_reallocated_sector_count() != null;
        inputMessage.setS5_reallocated_sector_count(inputMessageDto.getS5_reallocated_sector_count().isNaN() ? -1.0 : inputMessageDto.getS5_reallocated_sector_count());
        assert inputMessageDto.getS7_seek_error_rate() != null;
        inputMessage.setS7_seek_error_rate(inputMessageDto.getS7_seek_error_rate().isNaN() ? -1.0 : inputMessageDto.getS7_seek_error_rate());
        assert inputMessageDto.getS8_seek_time_performance() != null;
        inputMessage.setS8_seek_time_performance(inputMessageDto.getS8_seek_time_performance().isNaN() ? -1.0 : inputMessageDto.getS8_seek_time_performance());
        assert inputMessageDto.getS9_power_on_hours() != null;
        inputMessage.setS9_power_on_hours(inputMessageDto.getS9_power_on_hours().isNaN() ? -1.0 : inputMessageDto.getS9_power_on_hours());
        assert inputMessageDto.getS10_spin_retry_count() != null;
        inputMessage.setS10_spin_retry_count(inputMessageDto.getS10_spin_retry_count().isNaN() ? -1.0 : inputMessageDto.getS10_spin_retry_count());
        assert inputMessageDto.getS12_power_cycle_count() != null;
        inputMessage.setS12_power_cycle_count(inputMessageDto.getS12_power_cycle_count().isNaN() ? -1.0 : inputMessageDto.getS12_power_cycle_count());
        assert inputMessageDto.getS173_wear_leveling_count() != null;
        inputMessage.setS173_wear_leveling_count(inputMessageDto.getS173_wear_leveling_count().isNaN() ? -1.0 : inputMessageDto.getS173_wear_leveling_count());
        assert inputMessageDto.getS174_unexpected_power_loss_count() != null;
        inputMessage.setS174_unexpected_power_loss_count(inputMessageDto.getS174_unexpected_power_loss_count().isNaN() ? -1.0 : inputMessageDto.getS174_unexpected_power_loss_count());
        assert inputMessageDto.getS183_sata_downshift_count() != null;
        inputMessage.setS183_sata_downshift_count(inputMessageDto.getS183_sata_downshift_count().isNaN() ? -1.0 : inputMessageDto.getS183_sata_downshift_count());
        assert inputMessageDto.getS187_reported_uncorrectable_errors() != null;
        inputMessage.setS187_reported_uncorrectable_errors(inputMessageDto.getS187_reported_uncorrectable_errors().isNaN() ? -1.0 : inputMessageDto.getS187_reported_uncorrectable_errors());
        assert inputMessageDto.getS188_command_timeout() != null;
        inputMessage.setS188_command_timeout(inputMessageDto.getS188_command_timeout().isNaN() ? -1.0 : inputMessageDto.getS188_command_timeout());
        assert inputMessageDto.getS189_high_fly_writes() != null;
        inputMessage.setS189_high_fly_writes(inputMessageDto.getS189_high_fly_writes().isNaN() ? -1.0 : inputMessageDto.getS189_high_fly_writes());
        assert inputMessageDto.getS190_airflow_temperature_cel() != null;
        inputMessage.setS190_airflow_temperature_cel(inputMessageDto.getS190_airflow_temperature_cel().isNaN() ? -1.0 : inputMessageDto.getS190_airflow_temperature_cel());
        assert inputMessageDto.getS191_g_sense_error_rate() != null;
        inputMessage.setS191_g_sense_error_rate(inputMessageDto.getS191_g_sense_error_rate().isNaN() ? -1.0 : inputMessageDto.getS191_g_sense_error_rate());
        assert inputMessageDto.getS192_power_off_retract_count() != null;
        inputMessage.setS192_power_off_retract_count(inputMessageDto.getS192_power_off_retract_count().isNaN() ? -1.0 : inputMessageDto.getS192_power_off_retract_count());
        assert inputMessageDto.getS193_load_unload_cycle_count() != null;
        inputMessage.setS193_load_unload_cycle_count(inputMessageDto.getS193_load_unload_cycle_count().isNaN() ? -1.0 : inputMessageDto.getS193_load_unload_cycle_count());
        assert inputMessageDto.getS194_temperature_celsius() != null;
        inputMessage.setS194_temperature_celsius(inputMessageDto.getS194_temperature_celsius().isNaN() ? -1.0 : inputMessageDto.getS194_temperature_celsius());
        assert inputMessageDto.getS195_hardware_ecc_recovered() != null;
        inputMessage.setS195_hardware_ecc_recovered(inputMessageDto.getS195_hardware_ecc_recovered().isNaN() ? -1.0 : inputMessageDto.getS195_hardware_ecc_recovered());
        assert inputMessageDto.getS196_reallocated_event_count() != null;
        inputMessage.setS196_reallocated_event_count(inputMessageDto.getS196_reallocated_event_count().isNaN() ? -1.0 : inputMessageDto.getS196_reallocated_event_count());
        assert inputMessageDto.getS197_current_pending_sector() != null;
        inputMessage.setS197_current_pending_sector(inputMessageDto.getS197_current_pending_sector().isNaN() ? -1.0 : inputMessageDto.getS197_current_pending_sector());
        assert inputMessageDto.getS198_offline_uncorrectable() != null;
        inputMessage.setS198_offline_uncorrectable(inputMessageDto.getS198_offline_uncorrectable().isNaN() ? -1.0 : inputMessageDto.getS198_offline_uncorrectable());
        assert inputMessageDto.getS199_udma_crc_error_count() != null;
        inputMessage.setS199_udma_crc_error_count(inputMessageDto.getS199_udma_crc_error_count().isNaN() ? -1.0 : inputMessageDto.getS199_udma_crc_error_count());
        assert inputMessageDto.getS200_multi_zone_error_rate() != null;
        inputMessage.setS200_multi_zone_error_rate(inputMessageDto.getS200_multi_zone_error_rate().isNaN() ? -1.0 : inputMessageDto.getS200_multi_zone_error_rate());
        assert inputMessageDto.getS220_disk_shift() != null;
        inputMessage.setS220_disk_shift(inputMessageDto.getS220_disk_shift().isNaN() ? -1.0 : inputMessageDto.getS220_disk_shift());
        assert inputMessageDto.getS222_loaded_hours() != null;
        inputMessage.setS222_loaded_hours(inputMessageDto.getS222_loaded_hours().isNaN() ? -1.0 : inputMessageDto.getS222_loaded_hours());
        assert inputMessageDto.getS223_load_retry_count() != null;
        inputMessage.setS223_load_retry_count(inputMessageDto.getS223_load_retry_count().isNaN() ? -1.0 : inputMessageDto.getS223_load_retry_count());
        assert inputMessageDto.getS226_load_in_time() != null;
        inputMessage.setS226_load_in_time(inputMessageDto.getS226_load_in_time().isNaN() ? -1.0 : inputMessageDto.getS226_load_in_time());
        assert inputMessageDto.getS240_head_flying_hours() != null;
        inputMessage.setS240_head_flying_hours(inputMessageDto.getS240_head_flying_hours().isNaN() ? -1.0 : inputMessageDto.getS240_head_flying_hours());
        assert inputMessageDto.getS241_total_lbas_written() != null;
        inputMessage.setS241_total_lbas_written(inputMessageDto.getS241_total_lbas_written().isNaN() ? -1.0 : inputMessageDto.getS241_total_lbas_written());
        assert inputMessageDto.getS242_total_lbas_read() != null;
        inputMessage.setS242_total_lbas_read(inputMessageDto.getS242_total_lbas_read().isNaN() ? -1.0 : inputMessageDto.getS242_total_lbas_read());
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
