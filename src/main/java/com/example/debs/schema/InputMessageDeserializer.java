package com.example.debs.schema;

import com.example.debs.model.InputMessage;
import com.example.debs.model.InputMessageDto;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class InputMessageDeserializer implements KafkaRecordDeserializationSchema<InputMessage> {

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<InputMessage> collector) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        InputMessageDto inputMessageDto = objectMapper.readValue(consumerRecord.value(), InputMessageDto.class);

        InputMessage inputMessage = new InputMessage();
        LocalDateTime date = LocalDateTime.ofEpochSecond(inputMessageDto.getDate(), 0, ZoneOffset.UTC);
        inputMessage.setDate(date);
        inputMessage.setSerial_number(inputMessageDto.getSerial_number());
        inputMessage.setModel(inputMessageDto.getModel());
        inputMessage.setFailure(inputMessageDto.getFailure());
        inputMessage.setVaultId(inputMessageDto.getVault_id());
        inputMessage.setS1_read_error_rate(inputMessageDto.getS1_read_error_rate());
        inputMessage.setS2_throughput_performance(inputMessageDto.getS2_throughput_performance());
        inputMessage.setS3_spin_up_time(inputMessageDto.getS3_spin_up_time());
        inputMessage.setS4_start_stop_count(inputMessageDto.getS4_start_stop_count());
        inputMessage.setS5_reallocated_sector_count(inputMessageDto.getS5_reallocated_sector_count());
        inputMessage.setS7_seek_error_rate(inputMessageDto.getS7_seek_error_rate());
        inputMessage.setS8_seek_time_performance(inputMessageDto.getS8_seek_time_performance());
        inputMessage.setS9_power_on_hours(inputMessageDto.getS9_power_on_hours());
        inputMessage.setS10_spin_retry_count(inputMessageDto.getS10_spin_retry_count());
        inputMessage.setS12_power_cycle_count(inputMessageDto.getS12_power_cycle_count());
        inputMessage.setS173_wear_leveling_count(inputMessageDto.getS173_wear_leveling_count());
        inputMessage.setS174_unexpected_power_loss_count(inputMessageDto.getS174_unexpected_power_loss_count());
        inputMessage.setS183_sata_downshift_count(inputMessageDto.getS183_sata_downshift_count());
        inputMessage.setS187_reported_uncorrectable_errors(inputMessageDto.getS187_reported_uncorrectable_errors());
        inputMessage.setS188_command_timeout(inputMessageDto.getS188_command_timeout());
        inputMessage.setS189_high_fly_writes(inputMessageDto.getS189_high_fly_writes());
        inputMessage.setS190_airflow_temperature_cel(inputMessageDto.getS190_airflow_temperature_cel());
        inputMessage.setS191_g_sense_error_rate(inputMessageDto.getS191_g_sense_error_rate());
        inputMessage.setS192_power_off_retract_count(inputMessageDto.getS192_power_off_retract_count());
        inputMessage.setS193_load_unload_cycle_count(inputMessageDto.getS193_load_unload_cycle_count());
        inputMessage.setS194_temperature_celsius(inputMessageDto.getS194_temperature_celsius());
        inputMessage.setS195_hardware_ecc_recovered(inputMessageDto.getS195_hardware_ecc_recovered());
        inputMessage.setS196_reallocated_event_count(inputMessageDto.getS196_reallocated_event_count());
        inputMessage.setS197_current_pending_sector(inputMessageDto.getS197_current_pending_sector());
        inputMessage.setS198_offline_uncorrectable(inputMessageDto.getS198_offline_uncorrectable());
        inputMessage.setS199_udma_crc_error_count(inputMessageDto.getS199_udma_crc_error_count());
        inputMessage.setS200_multi_zone_error_rate(inputMessageDto.getS200_multi_zone_error_rate());
        inputMessage.setS220_disk_shift(inputMessageDto.getS220_disk_shift());
        inputMessage.setS222_loaded_hours(inputMessageDto.getS222_loaded_hours());
        inputMessage.setS223_load_retry_count(inputMessageDto.getS223_load_retry_count());
        inputMessage.setS226_load_in_time(inputMessageDto.getS226_load_in_time());
        inputMessage.setS240_head_flying_hours(inputMessageDto.getS240_head_flying_hours());
        inputMessage.setS241_total_lbas_written(inputMessageDto.getS241_total_lbas_written());
        inputMessage.setS242_total_lbas_read(inputMessageDto.getS242_total_lbas_read());
        collector.collect(inputMessage);
    }

    @Override
    public TypeInformation<InputMessage> getProducedType() {
        return TypeInformation.of(InputMessage.class);
    }
}
