package com.example.debs.operators;

import com.example.debs.model.InputMessage;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class InputMessageTimestampAssigner implements AssignerWithPunctuatedWatermarks<InputMessage> {
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(InputMessage lastElement, long extractedTimestamp) {
        return new Watermark(extractedTimestamp - 120000); // 2 minutes
    }

    @Override
    public long extractTimestamp(InputMessage message, long previousElementTimestamp) {
        return message.getEventTimeEpochMilli();
    }
}
