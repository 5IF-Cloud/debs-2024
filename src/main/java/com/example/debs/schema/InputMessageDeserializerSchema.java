package com.example.debs.schema;

import com.example.debs.model.InputMessage;
import com.example.debs.model.InputMessageDto;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class InputMessageDeserializerSchema implements DeserializationSchema<InputMessage> {

    @Override
    public InputMessage deserialize(byte[] bytes) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        InputMessageDto inputMessageDto = objectMapper.readValue(bytes, InputMessageDto.class);
        InputMessage inputMessage = new InputMessage();
        LocalDateTime date = LocalDateTime.ofEpochSecond(inputMessageDto.getDate(), 0, ZoneOffset.UTC);
        inputMessage.setDate(date);
        inputMessage.setFailure(inputMessageDto.getFailure());
        inputMessage.setVaultId(inputMessageDto.getVault_id());
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
