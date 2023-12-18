package com.example.debs.schema;

import com.example.debs.model.InputMessage;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

public class InputMessageDeserializerSchema implements DeserializationSchema<InputMessage> {

    @Override
    public InputMessage deserialize(byte[] bytes) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        InputMessage inputMessage = objectMapper.readValue(bytes, InputMessage.class);
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
