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
        inputMessage.setFailure(inputMessageDto.getFailure());
        inputMessage.setVaultId(inputMessageDto.getVault_id());
        collector.collect(inputMessage);
    }

    @Override
    public TypeInformation<InputMessage> getProducedType() {
        return TypeInformation.of(InputMessage.class);
    }
}
