package com.example.debs.operators;

import com.example.debs.model.InputMessage;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.time.ZoneId;

public class InputMessageTimestampAssigner implements AssignerWithPunctuatedWatermarks<InputMessage> {
    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(InputMessage lastElement, long extractedTimestamp) {
        return new Watermark(extractedTimestamp - 120000); // 2 minutes
    }

    @Override
    public long extractTimestamp(InputMessage message, long previousElementTimestamp) {
        ZoneId zoneId = ZoneId.of("UTC");
        return message.getDate().atZone(zoneId).toEpochSecond() * 1000;
    }
}
