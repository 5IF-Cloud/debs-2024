package com.example.debs.operators;

import com.example.debs.model.Centroid;
import com.example.debs.model.Smart;
import com.example.debs.model.TimeCentroid;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class MyProcessWindowFunction2 extends ProcessWindowFunction<Centroid, TimeCentroid, Long, TimeWindow> {
    @Override
    public void process(Long key, Context context, Iterable<Centroid> input, Collector<TimeCentroid> out) {
        for (Centroid centroid : input) {
            // Convert start time window to LocalDateTime
            long startTimeWindow = context.window().getStart();
            // Convert Long milliseconds to LocalDateTime
            LocalDateTime dayEnd = LocalDateTime.ofEpochSecond(startTimeWindow / 1000 + 86400, 0, ZoneOffset.UTC);

            TimeCentroid timeCentroid = new TimeCentroid(dayEnd, key, centroid.getSmart());
            out.collect(timeCentroid);
        }
    }
}
