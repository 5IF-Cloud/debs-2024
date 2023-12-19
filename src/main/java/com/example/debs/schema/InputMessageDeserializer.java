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

public class InputMessageDeserializer implements KafkaRecordDeserializationSchema<InputMessage> {

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> consumerRecord, Collector<InputMessage> collector) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        InputMessageDto inputMessageDto = objectMapper.readValue(consumerRecord.value(), InputMessageDto.class);

        InputMessage inputMessage = new InputMessage();
        Instant instant = Instant.ofEpochMilli(inputMessageDto.getDate() * 1000);
        inputMessage.setDate(instant);
        inputMessage.setIsFailure(inputMessageDto.getFailure().equals(1L));
        inputMessage.setVaultId(inputMessageDto.getVault_id());
        collector.collect(inputMessage);
    }

    @Override
    public TypeInformation<InputMessage> getProducedType() {
        return TypeInformation.of(InputMessage.class);
    }
}
