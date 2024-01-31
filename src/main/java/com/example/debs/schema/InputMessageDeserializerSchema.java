package com.example.debs.schema;

import com.example.debs.model.InputMessage;
import com.example.debs.model.InputMessageDto;
import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class InputMessageDeserializerSchema implements DeserializationSchema<InputMessage> {

    @Override
    public InputMessage deserialize(byte[] bytes) {
        Gson gson = new Gson();

        String jsonString = new String(bytes, StandardCharsets.UTF_8);

        // Handle NaN representation in JSON (e.g., replace "NaN" with Double.NaN)
        jsonString = jsonString.replaceAll("\"NaN\"", "null");

        InputMessageDto inputMessageDto = gson.fromJson(jsonString, InputMessageDto.class);

        InputMessage inputMessage = new InputMessage();
        LocalDateTime date = LocalDateTime.ofEpochSecond(inputMessageDto.getDate(), 0, ZoneOffset.UTC);
        inputMessage.setDate(date);
        inputMessage.setSerial_number(inputMessageDto.getSerial_number());
        inputMessage.setModel(inputMessageDto.getModel());
        inputMessage.setFailure(inputMessageDto.getFailure());
        inputMessage.setVaultId(inputMessageDto.getVault_id());
        // if one of these below is null, then set them to -1.0
        inputMessage.setS1_read_error_rate(inputMessageDto.getS1_read_error_rate() == null ? -1.0 : inputMessageDto.getS1_read_error_rate());
        inputMessage.setS2_throughput_performance(inputMessageDto.getS2_throughput_performance() == null ? -1.0 : inputMessageDto.getS2_throughput_performance());
        inputMessage.setS3_spin_up_time(inputMessageDto.getS3_spin_up_time() == null ? -1.0 : inputMessageDto.getS3_spin_up_time());
        inputMessage.setS4_start_stop_count(inputMessageDto.getS4_start_stop_count() == null ? -1.0 : inputMessageDto.getS4_start_stop_count());
        inputMessage.setS5_reallocated_sector_count(inputMessageDto.getS5_reallocated_sector_count() == null ? -1.0 : inputMessageDto.getS5_reallocated_sector_count());
        inputMessage.setS7_seek_error_rate(inputMessageDto.getS7_seek_error_rate() == null ? -1.0 : inputMessageDto.getS7_seek_error_rate());
        inputMessage.setS8_seek_time_performance(inputMessageDto.getS8_seek_time_performance() == null ? -1.0 : inputMessageDto.getS8_seek_time_performance());
        inputMessage.setS9_power_on_hours(inputMessageDto.getS9_power_on_hours() == null ? -1.0 : inputMessageDto.getS9_power_on_hours());
        inputMessage.setS10_spin_retry_count(inputMessageDto.getS10_spin_retry_count() == null ? -1.0 : inputMessageDto.getS10_spin_retry_count());
        inputMessage.setS12_power_cycle_count(inputMessageDto.getS12_power_cycle_count() == null ? -1.0 : inputMessageDto.getS12_power_cycle_count());
        inputMessage.setS173_wear_leveling_count(inputMessageDto.getS173_wear_leveling_count() == null ? -1.0 : inputMessageDto.getS173_wear_leveling_count());
        inputMessage.setS174_unexpected_power_loss_count(inputMessageDto.getS174_unexpected_power_loss_count() == null ? -1.0 : inputMessageDto.getS174_unexpected_power_loss_count());
        inputMessage.setS183_sata_downshift_count(inputMessageDto.getS183_sata_downshift_count() == null ? -1.0 : inputMessageDto.getS183_sata_downshift_count());
        inputMessage.setS187_reported_uncorrectable_errors(inputMessageDto.getS187_reported_uncorrectable_errors() == null ? -1.0 : inputMessageDto.getS187_reported_uncorrectable_errors());
        inputMessage.setS188_command_timeout(inputMessageDto.getS188_command_timeout() == null ? -1.0 : inputMessageDto.getS188_command_timeout());
        inputMessage.setS189_high_fly_writes(inputMessageDto.getS189_high_fly_writes() == null ? -1.0 : inputMessageDto.getS189_high_fly_writes());
        inputMessage.setS190_airflow_temperature_cel(inputMessageDto.getS190_airflow_temperature_cel() == null ? -1.0 : inputMessageDto.getS190_airflow_temperature_cel());
        inputMessage.setS191_g_sense_error_rate(inputMessageDto.getS191_g_sense_error_rate() == null ? -1.0 : inputMessageDto.getS191_g_sense_error_rate());
        inputMessage.setS192_power_off_retract_count(inputMessageDto.getS192_power_off_retract_count() == null ? -1.0 : inputMessageDto.getS192_power_off_retract_count());
        inputMessage.setS193_load_unload_cycle_count(inputMessageDto.getS193_load_unload_cycle_count() == null ? -1.0 : inputMessageDto.getS193_load_unload_cycle_count());
        inputMessage.setS194_temperature_celsius(inputMessageDto.getS194_temperature_celsius() == null ? -1.0 : inputMessageDto.getS194_temperature_celsius());
        inputMessage.setS195_hardware_ecc_recovered(inputMessageDto.getS195_hardware_ecc_recovered() == null ? -1.0 : inputMessageDto.getS195_hardware_ecc_recovered());
        inputMessage.setS196_reallocated_event_count(inputMessageDto.getS196_reallocated_event_count() == null ? -1.0 : inputMessageDto.getS196_reallocated_event_count());
        inputMessage.setS197_current_pending_sector(inputMessageDto.getS197_current_pending_sector() == null ? -1.0 : inputMessageDto.getS197_current_pending_sector());
        inputMessage.setS198_offline_uncorrectable(inputMessageDto.getS198_offline_uncorrectable() == null ? -1.0 : inputMessageDto.getS198_offline_uncorrectable());
        inputMessage.setS199_udma_crc_error_count(inputMessageDto.getS199_udma_crc_error_count() == null ? -1.0 : inputMessageDto.getS199_udma_crc_error_count());
        inputMessage.setS200_multi_zone_error_rate(inputMessageDto.getS200_multi_zone_error_rate() == null ? -1.0 : inputMessageDto.getS200_multi_zone_error_rate());
        inputMessage.setS220_disk_shift(inputMessageDto.getS220_disk_shift() == null ? -1.0 : inputMessageDto.getS220_disk_shift());
        inputMessage.setS222_loaded_hours(inputMessageDto.getS222_loaded_hours() == null ? -1.0 : inputMessageDto.getS222_loaded_hours());
        inputMessage.setS223_load_retry_count(inputMessageDto.getS223_load_retry_count() == null ? -1.0 : inputMessageDto.getS223_load_retry_count());
        inputMessage.setS226_load_in_time(inputMessageDto.getS226_load_in_time() == null ? -1.0 : inputMessageDto.getS226_load_in_time());
        inputMessage.setS240_head_flying_hours(inputMessageDto.getS240_head_flying_hours() == null ? -1.0 : inputMessageDto.getS240_head_flying_hours());
        inputMessage.setS241_total_lbas_written(inputMessageDto.getS241_total_lbas_written() == null ? -1.0 : inputMessageDto.getS241_total_lbas_written());
        inputMessage.setS242_total_lbas_read(inputMessageDto.getS242_total_lbas_read() == null ? -1.0 : inputMessageDto.getS242_total_lbas_read());
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
